#include <chrono>
#include <memory>
#include <utility>
#include <stdexcept>
#include "uFilterPickerPickBroker/subscribeServiceOptions.hpp"
#include "uFilterPickerPickBroker/pickStoreOptions.hpp"
#include "uFilterPickerPickBroker/grpcServerOptions.hpp"

using namespace UFilterPickerPickBroker;

class SubscribeServiceOptions::SubscribeServiceOptionsImpl
{
public:
    GRPCServerOptions mGRPCOptions;
    PickStoreOptions mPickStoreOptions;
    int mMaximumNumberOfSubscribers{64};
    bool mHasGRPCOptions{false};
};

/// Constructor
SubscribeServiceOptions::SubscribeServiceOptions() :
    pImpl(std::make_unique<SubscribeServiceOptionsImpl> ())
{
}

/// Copy constructor
SubscribeServiceOptions::SubscribeServiceOptions(const SubscribeServiceOptions &options)
{
    *this = options;
}

/// Move constructor
SubscribeServiceOptions::SubscribeServiceOptions(SubscribeServiceOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
SubscribeServiceOptions &SubscribeServiceOptions::operator=(const SubscribeServiceOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<SubscribeServiceOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
SubscribeServiceOptions &SubscribeServiceOptions::operator=(SubscribeServiceOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Destructor
SubscribeServiceOptions::~SubscribeServiceOptions() = default;

/// GRPC options
void SubscribeServiceOptions::setGRPCOptions(const GRPCServerOptions &options)
{
    pImpl->mGRPCOptions = options;
    pImpl->mHasGRPCOptions = true;
}

GRPCServerOptions SubscribeServiceOptions::getGRPCOptions() const
{
    if (!hasGRPCOptions()){throw std::runtime_error("gRPC options not set");}
    return pImpl->mGRPCOptions;
}

bool SubscribeServiceOptions::hasGRPCOptions() const noexcept
{
    return pImpl->mHasGRPCOptions;
}

/// Max number of subscribers
void SubscribeServiceOptions::setMaximumNumberOfSubscribers(const int maxSubscribers)
{
    if (maxSubscribers < 1)
    {
        throw std::invalid_argument(
            "Maximum number of subscribers must be positive");
    }
    pImpl->mMaximumNumberOfSubscribers = maxSubscribers;
}

int SubscribeServiceOptions::getMaximumNumberOfSubscribers() const noexcept
{
    return pImpl->mMaximumNumberOfSubscribers;
}

void SubscribeServiceOptions::setPickStoreOptions(
    const PickStoreOptions &options)
{
    pImpl->mPickStoreOptions = options;
}

PickStoreOptions SubscribeServiceOptions::getPickStoreOptions() const noexcept
{
    return pImpl->mPickStoreOptions;
}
