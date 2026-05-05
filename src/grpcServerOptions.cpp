#include <cstdint>
#include <utility>
#include <optional>
#include <string>
#include <stdexcept>
#include <memory>
#include "uFilterPickerProxy/grpcServerOptions.hpp"

using namespace UFilterPickerProxy;

class GRPCServerOptions::GRPCServerOptionsImpl
{
public:
    std::string mHost{"localhost"};
    uint16_t mPort{50000};
    std::optional<std::string> mAccessToken;
    std::optional<std::string> mServerCertificate;
    std::optional<std::string> mServerKey;
    bool mReflectionEnabled{false};
};

/// Constructor
GRPCServerOptions::GRPCServerOptions() :
    pImpl(std::make_unique<GRPCServerOptionsImpl>())
{
}

/// Copy constructor
GRPCServerOptions::GRPCServerOptions(const GRPCServerOptions &options)
{
    *this = options;
}

/// Move constructor
GRPCServerOptions::GRPCServerOptions(GRPCServerOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment operator
GRPCServerOptions& GRPCServerOptions::operator=(const GRPCServerOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<GRPCServerOptionsImpl>(*options.pImpl);
    return *this;
}

/// Move constructor
GRPCServerOptions& GRPCServerOptions::operator=(GRPCServerOptions &&options) noexcept
{
    if (this == &options){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Sets the host.
void GRPCServerOptions::setHost(const std::string &host)
{
    if (host.empty()){throw std::invalid_argument("Host cannot be empty.");}
    pImpl->mHost = host;
}

/// Gets the host.
std::string GRPCServerOptions::getHost() const noexcept
{
    return pImpl->mHost;   
}

/// Sets the port.
void GRPCServerOptions::setPort(uint16_t port)
{
    if (port == 0){throw std::invalid_argument("Port cannot be 0.");}
    pImpl->mPort = port;
}

/// Gets the port.
uint16_t GRPCServerOptions::getPort() const noexcept
{
    return pImpl->mPort;
}

/// Sets the access token.
void GRPCServerOptions::setAccessToken(const std::string &accessToken)
{
    if (accessToken.empty()){throw std::invalid_argument("Access token cannot be empty.");}
    pImpl->mAccessToken = std::optional<std::string> (accessToken);
}

/// Gets the access token.
std::optional<std::string> GRPCServerOptions::getAccessToken() const noexcept
{
    return pImpl->mAccessToken;
}

/// Sets the server certificate.
void GRPCServerOptions::setServerCertificate(const std::string &serverCertificate)
{
    if (serverCertificate.empty())
    {
        throw std::invalid_argument("Server certificate cannot be empty.");
    }
    pImpl->mServerCertificate = std::optional<std::string> (serverCertificate);
}

/// Gets the server certificate.
std::optional<std::string> GRPCServerOptions::getServerCertificate() const noexcept
{
    return pImpl->mServerCertificate;
}

/// Sets the server's private key.
void GRPCServerOptions::setServerKey(const std::string &serverKey)
{
    if (serverKey.empty())
    {
        throw std::invalid_argument("Server key cannot be empty.");
    }
    pImpl->mServerKey = std::optional<std::string> (serverKey);
}

/// Gets the server's private key.
std::optional<std::string> GRPCServerOptions::getServerKey() const noexcept
{
    return pImpl->mServerKey;
}

/// Enables reflection.
void GRPCServerOptions::enableReflection() noexcept
{
    pImpl->mReflectionEnabled = true;
}   

/// Disables reflection.
void GRPCServerOptions::disableReflection() noexcept
{
    pImpl->mReflectionEnabled = false;
}   

/// Gets whether reflection is enabled.
bool GRPCServerOptions::isReflectionEnabled() const noexcept
{
    return pImpl->mReflectionEnabled;
}

/// Destructor
GRPCServerOptions::~GRPCServerOptions() = default;
