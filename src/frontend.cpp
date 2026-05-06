#include <atomic>
#include <utility>
#include <memory>
#include <functional>
#include <string>
#include <optional>
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
#include <grpcpp/support/server_callback.h>
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

/*
class AsyncBidirectionalReader :
    public grpc::BidiServerReactor
           <
              UFilterPickerProxyAPI::V1::Pick,
              UFilterPickerProxyAPI::V1::WriteResponse
           >
{
}
*/

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
