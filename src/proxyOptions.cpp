#include <memory>
#include <utility>
#include "uFilterPickerProxy/proxyOptions.hpp"
#include "uFilterPickerProxy/frontendOptions.hpp"
#include "uFilterPickerProxy/backendOptions.hpp"

using namespace UFilterPickerProxy;

class ProxyOptions::ProxyOptionsImpl
{
public:
    FrontendOptions mFrontendOptions; 
    BackendOptions mBackendOptions;
    bool mHaveFrontendOptions{false};
    bool mhaveBackendOptions{false};
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
