#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <chrono>
#include <utility>
#include <vector>
#include <stdexcept>
#ifndef NDEBUG
#include <cassert>
#endif
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/server_callback.h>
#include "uFilterPickerPickBroker/subscribeService.hpp"
#include "uFilterPickerPickBroker/subscribeServiceOptions.hpp"
#include "uFilterPickerPickBroker/grpcServerOptions.hpp"
#include "uFilterPickerPickBroker/metricsSingleton.hpp"
#include "uFilterPickerPickBroker/pickStore.hpp"
#include "uFilterPickerPickBrokerAPI/v1/subscribe_service.grpc.pb.h"
#include "uFilterPickerPickBrokerAPI/v1/pick.pb.h"
#include "uFilterPickerPickBrokerAPI/v1/stream_since_request.pb.h"

using namespace UFilterPickerPickBroker;

namespace
{

[[nodiscard]]
bool validateSubscriber(const grpc::CallbackServerContext *context,
                        const std::string &accessToken)
{
    if (accessToken.empty()) { return true; }
    for (const auto &item : context->client_metadata())
    {
        if (item.first == "x-custom-auth-token")
        {
            if (item.second == accessToken) { return true; }
        }
    }
    return false;
}

class AsynchronousWriter :
    public grpc::ServerWriteReactor<UFilterPickerPickBrokerAPI::V1::Pick>
{
public:
    using Pick = UFilterPickerPickBrokerAPI::V1::Pick;

    AsynchronousWriter(
        const SubscribeServiceOptions &options,
        grpc::CallbackServerContext *context,
        const UFilterPickerPickBrokerAPI::V1::StreamSinceRequest *request,
        PickStore *store,
        std::function<void(AsynchronousWriter *)> unregisterCallback,
        const std::atomic<bool> *keepRunning,
        std::atomic<int> *subscriberCount,
        const bool isSecured,
        std::shared_ptr<spdlog::logger> logger) :
        mContext(context),
        mStore(store),
        mUnregisterCallback(std::move(unregisterCallback)),
        mKeepRunning(keepRunning),
        mSubscriberCount(subscriberCount),
        mLogger(std::move(logger))
    {
#ifndef NDEBUG
        assert(mContext != nullptr);
        assert(mStore != nullptr);
        assert(mKeepRunning != nullptr);
        assert(mSubscriberCount != nullptr);
#endif
        mPeer = context->peer();
        mContextAddress = reinterpret_cast<uintptr_t>(context);
        mMaximumNumberOfSubscribers = options.getMaximumNumberOfSubscribers();

        if (mSubscriberCount->load() >= mMaximumNumberOfSubscribers)
        {
            SPDLOG_LOGGER_WARN(mLogger,
                               "Maximum subscriber limit of {} reached",
                               mMaximumNumberOfSubscribers);
            Finish({grpc::StatusCode::RESOURCE_EXHAUSTED,
                    "Maximum subscriber limit reached"});
            return;
        }

        const auto grpcOptions = options.getGRPCOptions();
        const auto accessToken = grpcOptions.getAccessToken();

        if (isSecured && accessToken != std::nullopt)
        {
            if (!::validateSubscriber(context, *accessToken))
            {
                SPDLOG_LOGGER_WARN(mLogger,
                                   "Unauthorized subscriber {} rejected", mPeer);
                Finish({grpc::StatusCode::UNAUTHENTICATED, "Invalid access token"});
                return;
            }
        }

        const auto now
            = std::chrono::high_resolution_clock::now().time_since_epoch();
        if (request->has_backfill_duration())
        {
            const auto &dur = request->backfill_duration();
            const auto backfillNs = std::chrono::seconds(dur.seconds())
                                  + std::chrono::nanoseconds(dur.nanos());
            mStore->subscribe(mContextAddress, now - backfillNs);
        }
        else
        {
            mStore->subscribe(mContextAddress);
        }

        const auto nSubscribers = mSubscriberCount->fetch_add(1) + 1;
        const auto utilization
            = static_cast<double>(nSubscribers)
             / std::max(1, mMaximumNumberOfSubscribers);
        mMetrics.updateSubscribeServiceUtilization(utilization);
        mRegistered = true;
        SPDLOG_LOGGER_INFO(mLogger,
            "Pick subscriber connected {}; SubscribeService managing {} subscribers ({} pct utilized)",
            mPeer, nSubscribers, utilization);
        tryWrite();
    }

    void tryWrite()
    {
        if (mWriteInProgress.exchange(true)) { return; }
        auto picks = mStore->getPicks(mContextAddress);
        if (picks.empty())
        {
            // Release lock with seq_cst barrier so any concurrent enqueue that
            // missed us sees mWriteInProgress=false before its tryWrite call
            mWriteInProgress.store(false, std::memory_order_seq_cst);
            // Double-check for picks inserted between getPicks and store
            picks = mStore->getPicks(mContextAddress);
            if (picks.empty() || mWriteInProgress.exchange(true)) { return; }
        }
        mPendingPicks = std::move(picks);
        mPendingIndex = 0;
        StartWrite(&mPendingPicks[mPendingIndex]);
    }

    void OnWriteDone(bool ok) override
    {
        if (!ok)
        {
            Finish(grpc::Status::OK);
            return;
        }
        if (!mKeepRunning->load(std::memory_order_relaxed))
        {
            Finish({grpc::StatusCode::UNAVAILABLE, "Server shutdown - try again later"});
            return;
        }
        ++mPendingIndex;
        if (mPendingIndex < mPendingPicks.size())
        {
            StartWrite(&mPendingPicks[mPendingIndex]);
            return;
        }
        mPendingPicks.clear();
        mPendingIndex = 0;
        auto more = mStore->getPicks(mContextAddress);
        if (!more.empty())
        {
            mPendingPicks = std::move(more);
            StartWrite(&mPendingPicks[0]);
            return;
        }
        mWriteInProgress.store(false, std::memory_order_seq_cst);
        more = mStore->getPicks(mContextAddress);
        if (!more.empty() && !mWriteInProgress.exchange(true))
        {
            mPendingPicks = std::move(more);
            StartWrite(&mPendingPicks[0]);
        }
    }

    void OnDone() override
    {
        if (mRegistered)
        {
            mStore->unsubscribe(mContextAddress);
            const int nSubscribers = mSubscriberCount->fetch_sub(1) - 1;
            const auto utilization
                = static_cast<double>(nSubscribers)
                 / std::max(1, mMaximumNumberOfSubscribers);
            mMetrics.updateSubscribeServiceUtilization(utilization);
            SPDLOG_LOGGER_DEBUG(mLogger,
                "{} disconnected; Now managing {} subscribers ({} pct utilized)",
                mPeer, nSubscribers, utilization);
        }
        mUnregisterCallback(this);
        delete this;
    }

    void OnCancel() override
    {
        SPDLOG_LOGGER_INFO(mLogger,
                           "Async subscribe service RPC canceled by {}",
                           mPeer);
        Finish(grpc::Status::CANCELLED);
    }

    [[nodiscard]] bool isRegistered() const noexcept { return mRegistered; }

private:
    grpc::CallbackServerContext *mContext{nullptr};
    PickStore *mStore{nullptr};
    std::function<void(AsynchronousWriter *)> mUnregisterCallback;
    const std::atomic<bool> *mKeepRunning{nullptr};
    std::atomic<int> *mSubscriberCount{nullptr};
    std::shared_ptr<spdlog::logger> mLogger;
    UFilterPickerPickBroker::MetricsSingleton &mMetrics
    {
        UFilterPickerPickBroker::MetricsSingleton::getInstance()
    };
    std::vector<UFilterPickerPickBrokerAPI::V1::Pick> mPendingPicks;
    std::string mPeer;
    uintptr_t mContextAddress{0};
    std::atomic<bool> mWriteInProgress{false};
    size_t mPendingIndex{0};
    int mMaximumNumberOfSubscribers{64};
    bool mRegistered{false};
};

}

class SubscribeService::SubscribeServiceImpl :
    public UFilterPickerPickBrokerAPI::V1::SubscribeService::CallbackService
{
public:
    SubscribeServiceImpl(
        const SubscribeServiceOptions &options,
        std::unique_ptr<PickStore> &&store,
        std::shared_ptr<spdlog::logger> logger) :
        mOptions(options),
        mPickStore(std::move(store)),
        mLogger(std::move(logger))
    {
        if (!mOptions.hasGRPCOptions())
        {
            throw std::invalid_argument("GRPC server options not set");
        }
        if (mPickStore == nullptr)
        {
            throw std::invalid_argument("Pick store is null");
        }
        if (mLogger == nullptr)
        {
            // NOLINTBEGIN(misc-include-cleaner)
            const auto classId
                = std::to_string(reinterpret_cast<std::uintptr_t>(this));
            mLogger = spdlog::stdout_color_mt("SubscribeServiceConsole-"
                                            + classId);
            // NOLINTEND(misc-include-cleaner)
        }
    }

    void start()
    {
        mKeepRunning.store(true);
        mSubscriberCount.store(0);
        MetricsSingleton::getInstance().updateSubscribeServiceUtilization(0);
        const auto grpcOptions = mOptions.getGRPCOptions();
        const auto address = grpcOptions.getHost() + ":"
                           + std::to_string(grpcOptions.getPort());
        grpc::ServerBuilder builder;
        if (grpcOptions.getServerKey() == std::nullopt ||
            grpcOptions.getServerCertificate() == std::nullopt)
        {
            SPDLOG_LOGGER_INFO(mLogger,
                               "Initiating non-secured subscribe service");
            builder.AddListeningPort(address,
                                     grpc::InsecureServerCredentials());
            mSecured = false;
        }
        else
        {
            auto serverKey = grpcOptions.getServerKey();
            auto serverCertificate = grpcOptions.getServerCertificate();
#ifndef NDEBUG
            assert(serverKey != std::nullopt);
            assert(serverCertificate != std::nullopt);
#endif
            SPDLOG_LOGGER_INFO(mLogger, "Initiating secured subscribe service");
            const grpc::SslServerCredentialsOptions::PemKeyCertPair keyCertPair
            {
                *serverKey,
                *serverCertificate
            };
            grpc::SslServerCredentialsOptions sslOptions;
            sslOptions.pem_key_cert_pairs.emplace_back(keyCertPair);
            builder.AddListeningPort(address,
                                     grpc::SslServerCredentials(sslOptions));
            mSecured = true;
        }
        builder.RegisterService(this);
        SPDLOG_LOGGER_INFO(mLogger,
                           "SubscribeService listening at {}", address);
        mServer = builder.BuildAndStart();
        mServer->Wait();
        mStarted = true;
    }

    void stop()
    {
        mKeepRunning.store(false);
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        if (mServer)
        {
            if (mStarted)
            {
                SPDLOG_LOGGER_INFO(mLogger, "Shutting down subscribe service");
            }
            mServer->Shutdown();
            if (mStarted)
            {
                SPDLOG_LOGGER_INFO(mLogger, "Subscribe service shut down");
            }
        }
        mSubscriberCount.store(0);
        MetricsSingleton::getInstance().updateSubscribeServiceUtilization(0);
        mStarted = false;
    }

    void enqueue(UFilterPickerPickBrokerAPI::V1::Pick &&pick)
    {
        mPickStore->enqueue(std::move(pick));
        const std::lock_guard lock(mWritersMutex);
        for (auto *writer : mActiveWriters)
        {
            writer->tryWrite();
        }
    }

    void unregisterWriter(::AsynchronousWriter *writer)
    {
        const std::lock_guard lock(mWritersMutex);
        mActiveWriters.erase(writer);
    }

    grpc::ServerWriteReactor<UFilterPickerPickBrokerAPI::V1::Pick>*
    StreamSince(grpc::CallbackServerContext *context,
                const UFilterPickerPickBrokerAPI::V1::StreamSinceRequest *request) override
    {
        auto *writer = new ::AsynchronousWriter(
            mOptions,
            context,
            request,
            mPickStore.get(),
            [this](::AsynchronousWriter *w) { unregisterWriter(w); },
            &mKeepRunning,
            &mSubscriberCount,
            mSecured,
            mLogger);
        if (writer->isRegistered())
        {
            const std::lock_guard lock(mWritersMutex);
            mActiveWriters.insert(writer);
        }
        return writer;
    }

    ~SubscribeServiceImpl() override
    {
        stop();
        std::this_thread::sleep_for(std::chrono::milliseconds{25});
    }

private:
    SubscribeServiceOptions mOptions;
    std::unique_ptr<PickStore> mPickStore;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::unique_ptr<grpc::Server> mServer{nullptr};
    std::set<::AsynchronousWriter *> mActiveWriters;
    std::mutex mWritersMutex;
    UFilterPickerPickBroker::MetricsSingleton &mMetrics
    {
        UFilterPickerPickBroker::MetricsSingleton::getInstance()
    };
    std::atomic<int> mSubscriberCount{0};
    std::atomic<bool> mKeepRunning{true};
    bool mSecured{false};
    bool mStarted{false};
};

/// Constructor
SubscribeService::SubscribeService(
    const SubscribeServiceOptions &options,
    std::unique_ptr<PickStore> &&store,
    std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<SubscribeServiceImpl>(options,
                                                  std::move(store),
                                                  std::move(logger)))
{
}

/// Start the subscribe service
std::future<void> SubscribeService::start()
{
    return std::async(&SubscribeServiceImpl::start, &*pImpl);
}

/// Enqueue a pick for delivery to all subscribers
void SubscribeService::enqueue(UFilterPickerPickBrokerAPI::V1::Pick &&pick)
{
    pImpl->enqueue(std::move(pick));
}

/// Stop the subscribe service
void SubscribeService::stop()
{
    pImpl->stop();
}

/// Destructor
SubscribeService::~SubscribeService() = default;
