#include <atomic>
#include <cstdint>
#include <utility>
#include <memory>
#include <functional>
#include <string>
#include <algorithm>
#include <optional>
#include <exception>
#include <thread>
#include <chrono>
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
#include "uFilterPickerPickBroker/exception.hpp"
#include "uFilterPickerPickBroker/publishService.hpp"
#include "uFilterPickerPickBroker/publishServiceOptions.hpp"
#include "uFilterPickerPickBroker/grpcServerOptions.hpp"
#include "uFilterPickerPickBroker/metricsSingleton.hpp"
#include "uFilterPickerPickBrokerAPI/v1/publish_service.grpc.pb.h"
#include "uFilterPickerPickBrokerAPI/v1/pick.pb.h"
#include "uFilterPickerPickBrokerAPI/v1/publish_response.pb.h"

using namespace UFilterPickerPickBroker;

namespace
{

[[nodiscard]]
bool validatePublisher(const grpc::CallbackServerContext *context,
                       const std::string &accessToken)
{
    if (accessToken.empty()){return true;}
    for (const auto &item : context->client_metadata())
    {
        if (item.first == "x-custom-auth-token")
        {
            if (item.second == accessToken)
            {
                return true;
            }
        }
    }
    return false;
}

/// Handles one publisher's client-streaming Publish RPC session.
class AsynchronousReader :
    public grpc::ServerReadReactor<UFilterPickerPickBrokerAPI::V1::Pick>
{
public:
    using Pick = UFilterPickerPickBrokerAPI::V1::Pick;
    using PublishResponse = UFilterPickerPickBrokerAPI::V1::PublishResponse;

    AsynchronousReader(
        const PublishServiceOptions &options,
        grpc::CallbackServerContext *context,
        const std::function<void (UFilterPickerPickBrokerAPI::V1::Pick &&)> &callback,
        PublishResponse *response,
        const std::atomic<bool> *keepRunning,
        std::atomic<int> *publisherCount,
        const bool isSecured,
        std::shared_ptr<spdlog::logger> logger)
      :
        mContext(context),
        mCallback(callback),
        mResponse(response),
        mKeepRunning(keepRunning),
        mPublisherCount(publisherCount),
        mLogger(std::move(logger))
    {
#ifndef NDEBUG
        assert(mContext != nullptr);
        assert(mResponse != nullptr);
        assert(mKeepRunning != nullptr);
        assert(mPublisherCount != nullptr);
#endif
        mPeer = context->peer();
        mMaximumNumberOfPublishers = options.getMaximumNumberOfPublishers();
        mMaximumConsecutiveInvalidMessages
            = options.getMaximumConsecutiveInvalidMessages();

        if (mPublisherCount->load() >= mMaximumNumberOfPublishers)
        {
            SPDLOG_LOGGER_WARN(mLogger,
                               "Maximum publisher limit of {} reached",
                               mMaximumNumberOfPublishers);
            Finish({grpc::StatusCode::RESOURCE_EXHAUSTED,
                    "Maximum publisher limit reached"});
            return;
        }

        const auto grpcOptions = options.getGRPCOptions();
        const auto accessToken = grpcOptions.getAccessToken();

        if (isSecured && accessToken != std::nullopt)
        {
            if (!::validatePublisher(context, *accessToken))
            {
                SPDLOG_LOGGER_WARN(mLogger, "Unauthorized publisher {} rejected", mPeer);
                Finish({grpc::StatusCode::UNAUTHENTICATED, "Invalid access token"});
                return;
            }
        }

        const auto nPublishers = mPublisherCount->fetch_add(1) + 1;
        const auto utilization
            = static_cast<double> (nPublishers)
             /std::max(1, mMaximumNumberOfPublishers);
        mMetrics.updatePublishServiceUtilization(utilization);
        mRegistered = true;
        SPDLOG_LOGGER_INFO(mLogger,
            "Pick publisher connected {}; PublishService managing {} publishers ({} pct utilized)",
            mPeer,
            nPublishers,
            utilization);
        StartRead(&mCurrentPick);
    }

    void OnReadDone(bool ok) override
    {
        if (!ok)
        {
            mResponse->set_total_picks(mTotalPicks);
            mResponse->set_picks_rejected(mRejectedPicks);
            mResponse->set_duplicate_picks(mDuplicatePicks);
            Finish(grpc::Status::OK);
            return;
        }
        ++mTotalPicks;
        try
        {
            mCallback(std::move(mCurrentPick));
            mInvalidMessageCounter = 0;
        }
        catch (const std::invalid_argument &)
        {
            ++mInvalidMessageCounter;
            ++mInvalidPicks;
            if (mInvalidMessageCounter > mMaximumConsecutiveInvalidMessages)
            {
                SPDLOG_LOGGER_WARN(mLogger,
                    "PublishService disconnecting {} because it sent too many consecutive invalid messages",
                    mPeer);
                const grpc::Status status{
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "Too many consecutive messages were invalid - check API"};
                Finish(status);
                return;
            }
        }
        catch (const DuplicatePickException &)
        {
            ++mRejectedPicks;
            ++mDuplicatePicks;
        }
        catch (const std::exception &e)
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Rejected pick: {}", e.what());
            ++mRejectedPicks;
        }
        if (mKeepRunning->load(std::memory_order_relaxed))
        {
            StartRead(&mCurrentPick);
        }
        else
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Terminating publish service");
            const grpc::Status status{grpc::StatusCode::UNAVAILABLE,
                                      "Server shutdown - try again later"};
            Finish(status);
        }
    }

    void OnDone() override
    {
        if (mRegistered)
        {
            const int nPublishers = mPublisherCount->fetch_sub(1) - 1;
            auto utilization
                = static_cast<double> (nPublishers)
                 /std::max(1, mMaximumNumberOfPublishers);
            mMetrics.updatePublishServiceUtilization(utilization);
            SPDLOG_LOGGER_DEBUG(mLogger,
                "{} disconnected; Now managing {} publishers ({} pct utilized)",
                mPeer,
                nPublishers,
                utilization);
        }
        delete this;
    }

    void OnCancel() override
    {
        SPDLOG_LOGGER_INFO(mLogger,
                           "Async pick broker publish service RPC canceled by {}",
                           mPeer);
        Finish(grpc::Status::CANCELLED);
    }

//private:
    grpc::CallbackServerContext *mContext{nullptr};
    UFilterPickerPickBrokerAPI::V1::PublishResponse *mResponse{nullptr};
    std::function<void (UFilterPickerPickBrokerAPI::V1::Pick &&)> mCallback;
    const std::atomic<bool> *mKeepRunning{nullptr};
    std::atomic<int> *mPublisherCount{nullptr};
    std::shared_ptr<spdlog::logger> mLogger;
    UFilterPickerPickBrokerAPI::V1::Pick mCurrentPick;
    UFilterPickerPickBroker::MetricsSingleton &mMetrics
    {
        UFilterPickerPickBroker::MetricsSingleton::getInstance()
    };
    std::string mPeer;
    uint64_t mTotalPicks{0};
    uint64_t mInvalidPicks{0};
    uint64_t mRejectedPicks{0};
    uint64_t mDuplicatePicks{0};
    uint32_t mInvalidMessageCounter{0};
    uint32_t mMaximumConsecutiveInvalidMessages{8};
    int mMaximumNumberOfPublishers{2048};
    bool mRegistered{false};
};

}

class PublishService::PublishServiceImpl :
    public UFilterPickerPickBrokerAPI::V1::PublishService::CallbackService
{
public:
    PublishServiceImpl
    (
        const PublishServiceOptions &options,
        const std::function<void (UFilterPickerPickBrokerAPI::V1::Pick &&)> &callback,
        std::shared_ptr<spdlog::logger> logger
    ) :
        mOptions(options),
        mCallback(callback),
        mLogger(std::move(logger))
    {
        if (!mOptions.hasGRPCOptions())
        {
            throw std::invalid_argument("GRPC server options not set");
        }
        if (mLogger == nullptr)
        {
            // NOLINTBEGIN(misc-include-cleaner)
            auto classId
                = std::to_string (reinterpret_cast<std::uintptr_t> (this));
            mLogger = spdlog::stdout_color_mt("PublishServiceConsole-"
                                            + classId);
            // NOLINTEND(misc-include-cleaner)
        }
    }

    void start()
    {
        mKeepRunning.store(true);
        mPublisherCount.store(0);
        MetricsSingleton::getInstance().updatePublishServiceUtilization(0);
        const auto grpcOptions = mOptions.getGRPCOptions();
        const auto address = grpcOptions.getHost() + ":"
                           + std::to_string(grpcOptions.getPort());
        grpc::ServerBuilder builder;
        auto maximumMessageSizeInBytes
            = mOptions.getMaximumMessageSizeInBytes();
        if (maximumMessageSizeInBytes != std::nullopt)
        {
            builder.SetMaxReceiveMessageSize(
                *maximumMessageSizeInBytes);
        }
        if (grpcOptions.getServerKey() == std::nullopt ||
            grpcOptions.getServerCertificate() == std::nullopt)
        {
            SPDLOG_LOGGER_INFO(mLogger,
                               "Initiating non-secured publish service");
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
            SPDLOG_LOGGER_INFO(mLogger, "Initiating secured publish service");
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
                           "PublishService listening at {}", address);
        mServer = builder.BuildAndStart();
    }

    void stop()
    {
        mKeepRunning.store(false);
        std::this_thread::sleep_for(std::chrono::milliseconds {10});
        if (mServer)
        {
            SPDLOG_LOGGER_INFO(mLogger, "Shutting down server");
            mServer->Shutdown();
            SPDLOG_LOGGER_INFO(mLogger, "Server shut down");
        }
        mPublisherCount.store(0);
        MetricsSingleton::getInstance().updatePublishServiceUtilization(0);
    }

    grpc::ServerReadReactor<UFilterPickerPickBrokerAPI::V1::Pick>*
        Publish(grpc::CallbackServerContext* context,
                UFilterPickerPickBrokerAPI::V1::PublishResponse *publishResponse) override
    {
        return new ::AsynchronousReader(
            mOptions,
            context,
            mCallback,
            publishResponse,
            &mKeepRunning,
            &mPublisherCount,
            mSecured,
            mLogger);
    }

    ~PublishServiceImpl() override
    {
        stop();
        std::this_thread::sleep_for(std::chrono::milliseconds {25});
    }
//private:
    PublishServiceOptions mOptions;
    std::function<void (UFilterPickerPickBrokerAPI::V1::Pick &&)> mCallback;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::unique_ptr<grpc::Server> mServer{nullptr};
    std::atomic<int> mPublisherCount{0};
    std::atomic<bool> mKeepRunning{true};
    bool mSecured{false};
};

/// Constructor
PublishService::PublishService(
    const PublishServiceOptions &options,
    const std::function<void (UFilterPickerPickBrokerAPI::V1::Pick &&)> &callback,
    std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<PublishServiceImpl> (options,
                                                callback,
                                                std::move(logger)))
{
}

/// Start the publish service
void PublishService::start()
{
    pImpl->start();
}

/// Stop the publish service
void PublishService::stop()
{
    pImpl->stop();
}

/// Destructor
PublishService::~PublishService() = default;
