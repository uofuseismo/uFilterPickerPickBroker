#include <memory>
#include <utility>
#include <stdexcept>
#include "uFilterPickerProxy/backendOptions.hpp"
#include "uFilterPickerProxy/grpcServerOptions.hpp"

using namespace UFilterPickerProxy;

class BackendOptions::BackendOptionsImpl
{
public:
    GRPCServerOptions mGRPCOptions;
    int mMaximumNumberOfSubscribers{64};
    bool mHasGRPCOptions{false};
};

/// Constructor
BackendOptions::BackendOptions() :
    pImpl(std::make_unique<BackendOptionsImpl> ())
{
}

/// Copy constructor
BackendOptions::BackendOptions(const BackendOptions &options)
{
    *this = options;
}

/// Move constructor
BackendOptions::BackendOptions(BackendOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
BackendOptions &BackendOptions::operator=(const BackendOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<BackendOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
BackendOptions &BackendOptions::operator=(BackendOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Destructor
BackendOptions::~BackendOptions() = default;

/// GRPC options
void BackendOptions::setGRPCOptions(const GRPCServerOptions &options)
{
    pImpl->mGRPCOptions = options;
    pImpl->mHasGRPCOptions = true;
}

GRPCServerOptions BackendOptions::getGRPCOptions() const
{
    if (!hasGRPCOptions()){throw std::runtime_error("gRPC options not set");}
    return pImpl->mGRPCOptions;
}

bool BackendOptions::hasGRPCOptions() const noexcept
{
    return pImpl->mHasGRPCOptions;
}

/// Max number of subscribers
void BackendOptions::setMaximumNumberOfSubscribers(const int maxSubscribers)
{
    if (maxSubscribers < 1)
    {
        throw std::invalid_argument(
            "Maximum number of subscribers must be postiive");
    }
    pImpl->mMaximumNumberOfSubscribers = maxSubscribers;
}

int BackendOptions::getMaximumNumberOfSubscribers() const noexcept
{
    return pImpl->mMaximumNumberOfSubscribers; 
}