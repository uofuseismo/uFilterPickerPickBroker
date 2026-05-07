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
#include "uFilterPickerProxy/exception.hpp"
#include "uFilterPickerProxy/frontend.hpp"
#include "uFilterPickerProxy/frontendOptions.hpp"
#include "uFilterPickerProxy/grpcServerOptions.hpp"
#include "uFilterPickerProxyAPI/v1/frontend.grpc.pb.h"
#include "uFilterPickerProxyAPI/v1/pick.pb.h"
#include "uFilterPickerProxyAPI/v1/publish_response.pb.h"

using namespace UFilterPickerProxy;

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
    public grpc::ServerReadReactor<UFilterPickerProxyAPI::V1::Pick>
{
public:
    using Pick = UFilterPickerProxyAPI::V1::Pick;
    using PublishResponse = UFilterPickerProxyAPI::V1::PublishResponse;

    AsynchronousReader(
        const FrontendOptions &options,
        grpc::CallbackServerContext *context,
        const std::function<void (UFilterPickerProxyAPI::V1::Pick &&)> &callback,
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
        mMaximumConsecutiveInvalidMessages 
            = options.getMaximumConsecutiveInvalidMessages();

        const auto maxPublishers = options.getMaximumNumberOfPublishers();
        if (mPublisherCount->load() >= maxPublishers)
        {
            SPDLOG_LOGGER_WARN(mLogger, "Maximum publisher limit reached");
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

        // Looks good to go
        const auto nPublishers = mPublisherCount->fetch_add(1) + 1;
        const auto utilization
            = static_cast<double> (nPublishers)
             /std::max(1, mMaximumNumberOfPublishers);

        mRegistered = true;
        SPDLOG_LOGGER_INFO(mLogger,
            "Pick publisher connected {}; Frontend managing {} publishers ({} pct utilized)",
            mPeer,
            nPublishers,
            utilization);
        StartRead(&mCurrentPick);
    }

    void OnReadDone(bool ok) override
    {
        // Handle errors
        if (!ok)
        {
            mResponse->set_total_picks(mTotalPicks);
            mResponse->set_picks_rejected(mRejectedPicks);
            mResponse->set_duplicate_picks(mDuplicatePicks);
            Finish(grpc::Status::OK);
            return;
        }
        // Handle normal case
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
                    "Frontend disconnecting {} because it sent too many consecutive invalid messages",
                    mPeer);
                const grpc::Status status{
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "Too many conseuctive messages were invalid - check API"};
                Finish(status);
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
        // If server is still running then read another a packet
        if (mKeepRunning->load(std::memory_order_relaxed))
        {
            StartRead(&mCurrentPick);
        }
        else
        {
             
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
            SPDLOG_LOGGER_DEBUG(mLogger,
                "{} disconnected; Now managing {} publishers ({} pct utilized)",
                mPeer,
                nPublishers,
                utilization);
        }
        delete this;
    }

//private:
    grpc::CallbackServerContext *mContext{nullptr};
    UFilterPickerProxyAPI::V1::PublishResponse *mResponse{nullptr};
    std::function<void (UFilterPickerProxyAPI::V1::Pick &&)> mCallback;
    const std::atomic<bool> *mKeepRunning{nullptr};
    std::atomic<int> *mPublisherCount{nullptr};
    std::shared_ptr<spdlog::logger> mLogger;
    UFilterPickerProxyAPI::V1::Pick mCurrentPick;
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

class Frontend::FrontendImpl :
    public UFilterPickerProxyAPI::V1::Frontend::CallbackService 
{
public:
    FrontendImpl
    (
        const FrontendOptions &options,
        const std::function<void (UFilterPickerProxyAPI::V1::Pick &&)> &callback,
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
            mLogger = spdlog::stdout_color_mt("ProxyFrontendConsole-"
                                            + classId);
            // NOLINTEND(misc-include-cleaner)
        }
    }

    void start()
    {
        mKeepRunning.store(true);
        mNumberOfPublishers.store(0);
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
                               "Initiating non-secured proxy frontend");
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
            SPDLOG_LOGGER_INFO(mLogger, "Initiating secured proxy frontend");
            const grpc::SslServerCredentialsOptions::PemKeyCertPair keyCertPair
            {
                *serverKey,        // Private key
                *serverCertificate // Public key (cert chain)
            };
            grpc::SslServerCredentialsOptions sslOptions; 
            sslOptions.pem_key_cert_pairs.emplace_back(keyCertPair);
            builder.AddListeningPort(address,
                                     grpc::SslServerCredentials(sslOptions));
            mSecured = true;
        }
        builder.RegisterService(this);
        SPDLOG_LOGGER_INFO(mLogger,
                           "Frontend listening at {}", address);
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
        mNumberOfPublishers.store(0);
    }

    /// The RPC
    grpc::ServerReadReactor<UFilterPickerProxyAPI::V1::Pick>*
        Publish(grpc::CallbackServerContext* context,
                UFilterPickerProxyAPI::V1::PublishResponse *publishResponse) override
    {
        /*
        return new ::AsynchronousReader(
            mOptions,
            context,
            mCallback,
            publishResponse,
            mSecured,
            &mKeepRunning,
            &mNumberOfPublishers,
            mLogger);
            */
    }

    ~FrontendImpl() override
    {
        stop();
        std::this_thread::sleep_for(std::chrono::milliseconds {25});
    }
//private:
    FrontendOptions mOptions;
    std::function<void (UFilterPickerProxyAPI::V1::Pick &&)> mCallback;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::unique_ptr<grpc::Server> mServer{nullptr};
    std::atomic<int> mNumberOfPublishers{0};
    std::atomic<bool> mKeepRunning{true};
    bool mSecured{false};
};

/// Constructor
Frontend::Frontend(
    const FrontendOptions &options,
    const std::function<void (UFilterPickerProxyAPI::V1::Pick &&)> &callback,
    std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<FrontendImpl> (options,
                                          callback, 
                                          std::move(logger)))
{
}

/// Start the frontend acquisition
void Frontend::start()
{
    pImpl->start();
}

/// Stop the frontend
void Frontend::stop()
{
    pImpl->stop();
}

/// Destructor
Frontend::~Frontend() = default;
