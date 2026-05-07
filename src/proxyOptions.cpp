#include <memory>
#include <utility>
#include <stdexcept>
#include "uFilterPickerProxy/proxyOptions.hpp"
#include "uFilterPickerProxy/frontendOptions.hpp"
#include "uFilterPickerProxy/backendOptions.hpp"
#include "uFilterPickerProxy/grpcServerOptions.hpp"

using namespace UFilterPickerProxy;

class ProxyOptions::ProxyOptionsImpl
{
public:
    FrontendOptions mFrontendOptions; 
    BackendOptions mBackendOptions;
    bool mHasFrontendOptions{false};
    bool mHasBackendOptions{false};
};

/// Constructor
ProxyOptions::ProxyOptions() :
    pImpl(std::make_unique<ProxyOptionsImpl> ())
{
}

/// Copy constructor
ProxyOptions::ProxyOptions(const ProxyOptions &options)
{
    *this = options;
}

/// Move constructor
ProxyOptions::ProxyOptions(ProxyOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
ProxyOptions &ProxyOptions::operator=(const ProxyOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<ProxyOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
ProxyOptions &ProxyOptions::operator=(ProxyOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Destructor
ProxyOptions::~ProxyOptions() = default;

/// Frontend options
void ProxyOptions::setFrontendOptions(const FrontendOptions &options)
{
    if (hasBackendOptions())
    {
        const auto thisPort = options.getGRPCOptions().getPort(); 
        const auto grpcBEPort = getBackendOptions().getGRPCOptions().getPort();
        if (thisPort == grpcBEPort)
        {
            throw std::invalid_argument("Cannot bind frontend to backend port");
        }
    }
    pImpl->mFrontendOptions = options;
    pImpl->mHasFrontendOptions = true;
}

FrontendOptions ProxyOptions::getFrontendOptions() const
{
    if (!hasFrontendOptions())
    {
        throw std::runtime_error("Frontend options not set");
    }
    return pImpl->mFrontendOptions;
}

bool ProxyOptions::hasFrontendOptions() const noexcept
{
    return pImpl->mHasFrontendOptions;
}

/// Backend options
void ProxyOptions::setBackendOptions(const BackendOptions &options)
{
    if (hasFrontendOptions())
    {
        const auto thisPort = options.getGRPCOptions().getPort(); 
        const auto grpcFEPort = getFrontendOptions().getGRPCOptions().getPort();
        if (thisPort == grpcFEPort)
        {
            throw std::invalid_argument("Cannot bind backend to frontend port");
        }
    }    
    pImpl->mBackendOptions = options;
    pImpl->mHasBackendOptions = true;
}

BackendOptions ProxyOptions::getBackendOptions() const
{
    if (!hasBackendOptions())
    {
        throw std::runtime_error("Frontend options not set");
    }
    return pImpl->mBackendOptions;
}

bool ProxyOptions::hasBackendOptions() const noexcept
{
    return pImpl->mHasBackendOptions;
}