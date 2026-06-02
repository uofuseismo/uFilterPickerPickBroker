#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <chrono>
#include <queue>
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
#include <grpcpp/support/time.h> //NOLINT
#include "uFilterPickerPickBroker/subscribeService.hpp"
#include "uFilterPickerPickBroker/subscribeServiceOptions.hpp"
#include "uFilterPickerPickBroker/grpcServerOptions.hpp"
#include "uFilterPickerPickBroker/metricsSingleton.hpp"
#include "uFilterPickerPickBroker/pickStore.hpp"
#include "uFilterPickerPickBrokerAPI/v1/subscribe_service.grpc.pb.h"
#include "uFilterPickerPickBrokerAPI/v1/pick.pb.h"
#include "uFilterPickerPickBrokerAPI/v1/stream_since_request.pb.h"

// Chunk the output by this many picks at a time
#define MAXIMUM_QUEUE_SIZE 32

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
        const std::atomic<bool> *keepRunning,
        std::atomic<int> *subscriberCount,
        const bool isSecured,
        std::shared_ptr<spdlog::logger> logger) :
        mContext(context),
        mStore(store),
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
        if (request)
        {
            if (!request->identifier().empty())
            {
                mPeer = mPeer + " (" + request->identifier() + ")";
            }
        }
        mContextAddress = reinterpret_cast<uintptr_t>(context);
        mMaximumNumberOfSubscribers = options.getMaximumNumberOfSubscribers();

        // Can we afford another subscriber?
        if (mSubscriberCount->load() >= mMaximumNumberOfSubscribers)
        {
            SPDLOG_LOGGER_WARN(mLogger,
                               "Maximum subscriber limit of {} reached",
                               mMaximumNumberOfSubscribers);
            Finish({grpc::StatusCode::RESOURCE_EXHAUSTED,
                    "Maximum subscriber limit reached"});
        }

        // Authenticate (if we have certs and there is an access token)
        if (isSecured)
        {
            const auto accessToken = options.getGRPCOptions().getAccessToken();
            if (accessToken != std::nullopt)
            {
                if (!::validateSubscriber(context, *accessToken))
                {
                     SPDLOG_LOGGER_WARN(mLogger,
                                        "Unauthorized subscriber {} rejected",
                                        mPeer);
                     Finish({grpc::StatusCode::UNAUTHENTICATED,
                             "Invalid access token"});
                }
            }
        }

        // Try subscribing 
        const auto now
            = std::chrono::high_resolution_clock::now().time_since_epoch();
        if (request->has_backfill_duration())
        {
            // Figure out the receive times this subscriber wants
            const auto &duration = request->backfill_duration();
            const auto backfillNanoSeconds
                 = std::chrono::seconds(duration.seconds())
                 + std::chrono::nanoseconds(duration.nanos());
            try
            {
                mStore->subscribe(mContextAddress, now - backfillNanoSeconds);
            }
            catch (const std::exception &e)
            {
                SPDLOG_LOGGER_ERROR(mLogger,
                                    "Failed to subscribe {} because {}",
                                    mPeer, std::string {e.what()});
                Finish({grpc::StatusCode::INTERNAL,
                        "Server error - failed to subscribe with backfill"});
            }
        }
        else
        {
            // Otherwise, they'll happily takes what shows up next
            try
            {
                mStore->subscribe(mContextAddress);
            }
            catch (const std::exception &e)
            {
                SPDLOG_LOGGER_ERROR(mLogger,
                                    "Failed to subscribe {} because {}",
                                    mPeer, std::string {e.what()});
                Finish({grpc::StatusCode::INTERNAL,
                        "Server error - failed to subscribe"});
            }
        }

        // Update
        const auto nSubscribers = mSubscriberCount->fetch_add(1) + 1;
        const auto utilization
            = static_cast<double>(nSubscribers)
             / std::max(1, mMaximumNumberOfSubscribers);
        mMetrics.updateSubscribeServiceUtilization(utilization);
        mRegistered = true;
        SPDLOG_LOGGER_INFO(mLogger,
            "Pick subscriber connected {}; SubscribeService managing {} subscribers ({} pct utilized)",
            mPeer, nSubscribers, utilization);

        // Start it up
        mWriteInProgress = false;
        nextWrite();
    }

    void nextWrite()
    {
        while (mKeepRunning->load())
        {
            // Handle cancellation
            if (mContext->IsCancelled()){break;}

            // Send another one
            if (!mPicksQueue.empty() && !mWriteInProgress)
            {
                const auto &pick = mPicksQueue.front();
                mWriteInProgress = true;
                mMetrics.incrementPicksSentCounter();
                StartWrite(&pick);
                return;
            }
            // Try to get more
            if (mPicksQueue.empty())
            {
                try
                {
                    auto newPicks
                        = mStore->getPicks(mContextAddress, MAXIMUM_QUEUE_SIZE);
                    for (auto &pick : newPicks)
                    {
                        mPicksQueue.push(std::move(pick));
                    }
                }
                catch (const std::exception &e)
                {
                    SPDLOG_LOGGER_ERROR(mLogger,
                                    "Failed to get new picks for {} because {}",
                                    mPeer,
                                    std::string {e.what()});
                }
            }
                
            if (mPicksQueue.empty() && !mWriteInProgress)
            {
                std::this_thread::sleep_for(mTimeOut);
            }
        }
        // We're done (for some reason)
        if (mContext)
        {
            if (mContext->IsCancelled())
            {
                SPDLOG_LOGGER_INFO(mLogger, "{} issued cancel", mPeer);
                Finish(grpc::Status::CANCELLED);
            }
            else
            {
                SPDLOG_LOGGER_INFO(mLogger, "Server cancelling {}", mPeer);
                Finish(grpc::Status::OK);
            }
        }
    }

    void OnWriteDone(bool ok) override
    {
        // Likely a cancel but deal with it
        if (!ok)
        {
            if (mContext)
            {
                if (mContext->IsCancelled())
                {
                    SPDLOG_LOGGER_DEBUG(mLogger,
                                        "{} issued dlient cancel",
                                        mPeer);
                    return Finish(grpc::Status::CANCELLED);
                }
            }
            return Finish(grpc::Status {grpc::StatusCode::UNKNOWN,
                                        "Unknown server error"});
        }
        if (!mKeepRunning->load(std::memory_order_relaxed))
        {
            return Finish({grpc::StatusCode::UNAVAILABLE,
                           "Server shutdown - try again later"});
        }
        mWriteInProgress = false;
        mPicksQueue.pop();
        nextWrite();
    }

    void OnDone() override
    {
        if (mRegistered)
        {
            mStore->unsubscribe(mContextAddress);
            mRegistered = false;
        }
        const int nSubscribers = mStore->getNumberOfSubscribers(); //mSubscriberCount->fetch_sub(1) - 1;
        const auto utilization
            = static_cast<double>(nSubscribers)
             / std::max(1, mMaximumNumberOfSubscribers);
        mMetrics.updateSubscribeServiceUtilization(utilization);
        SPDLOG_LOGGER_DEBUG(mLogger,
            "{} disconnected; Now managing {} subscribers ({} pct utilized)",
            mPeer, nSubscribers, utilization);
        delete this;
    }

    void OnCancel() override
    {
        SPDLOG_LOGGER_INFO(mLogger,
                           "Async subscribe service RPC canceled by {}",
                           mPeer);
        if (mRegistered)
        {
            mStore->unsubscribe(mContextAddress);
            mRegistered = false;
        }
    }

private:
    grpc::CallbackServerContext *mContext{nullptr};
    PickStore *mStore{nullptr};
    const std::atomic<bool> *mKeepRunning{nullptr};
    std::atomic<int> *mSubscriberCount{nullptr};
    std::shared_ptr<spdlog::logger> mLogger;
    UFilterPickerPickBroker::MetricsSingleton &mMetrics
    {
        UFilterPickerPickBroker::MetricsSingleton::getInstance()
    };
    std::queue<UFilterPickerPickBrokerAPI::V1::Pick> mPicksQueue;
    UFilterPickerPickBrokerAPI::V1::Pick mPick;
    std::string mPeer;
    uintptr_t mContextAddress{0};
    std::chrono::milliseconds mTimeOut{15};
    bool mWriteInProgress{false};
    //size_t mMaximumQueueSize{128};
    //size_t mPendingIndex{0};
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
            constexpr int64_t timeOutSeconds{2};
            constexpr int64_t timeOutNanoSeconds{0};
            const gpr_timespec deadline // NOLINT
            {
                timeOutSeconds,
                timeOutNanoSeconds,
                GPR_TIMESPAN // NOLINT
            };
            mServer->Shutdown(deadline);
            if (mStarted)
            {
                SPDLOG_LOGGER_INFO(mLogger, "Subscribe service shut down");
            }
            mServer = nullptr;
        }
        mSubscriberCount.store(0);
        MetricsSingleton::getInstance().updateSubscribeServiceUtilization(0);
        mStarted = false;
    }

    void enqueue(const std::chrono::nanoseconds &receivedTime,
                 UFilterPickerPickBrokerAPI::V1::Pick &&pick)
    {
        // Write pick to store
        mPickStore->enqueue(receivedTime, std::move(pick));
    }

    grpc::ServerWriteReactor<UFilterPickerPickBrokerAPI::V1::Pick>*
    StreamSince(grpc::CallbackServerContext *context,
                const UFilterPickerPickBrokerAPI::V1::StreamSinceRequest *request) override
    {
        return new ::AsynchronousWriter(
            mOptions,
            context,
            request,
            mPickStore.get(),
            &mKeepRunning,
            &mSubscriberCount,
            mSecured,
            mLogger);
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
void SubscribeService::enqueue(
    const std::chrono::nanoseconds &receivedTime,
    UFilterPickerPickBrokerAPI::V1::Pick &&pick)
{
    pImpl->enqueue(receivedTime, std::move(pick));
}

/// Stop the subscribe service
void SubscribeService::stop()
{
    pImpl->stop();
}

/// Destructor
SubscribeService::~SubscribeService() = default;
