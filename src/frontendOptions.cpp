#include <memory>
#include <utility>
#include <stdexcept>
#include <optional>
#include "uFilterPickerProxy/frontendOptions.hpp"
#include "uFilterPickerProxy/grpcServerOptions.hpp"

using namespace UFilterPickerProxy;

class FrontendOptions::FrontendOptionsImpl
{
public:
    GRPCServerOptions mGRPCOptions;
    int mMaximumPublishers{2048};
    int mMaximumConsecutiveInvalidMessages{8};
    std::optional<int> mMaximumMessageSizeInBytes{std::nullopt};
    bool mHasGRPCOptions{false};
};

/// Constructor
FrontendOptions::FrontendOptions() :
    pImpl(std::make_unique<FrontendOptionsImpl> ())
{
}

/// Copy constructor
FrontendOptions::FrontendOptions(const FrontendOptions &options)
{
    *this = options;
}

/// Move constructor
FrontendOptions::FrontendOptions(FrontendOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
FrontendOptions &FrontendOptions::operator=(const FrontendOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<FrontendOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
FrontendOptions &FrontendOptions::operator=(FrontendOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Destructor
FrontendOptions::~FrontendOptions() = default;

/// GRPC options
void FrontendOptions::setGRPCOptions(const GRPCServerOptions &options)
{
    pImpl->mGRPCOptions = options;
    pImpl->mHasGRPCOptions = true;
}

GRPCServerOptions FrontendOptions::getGRPCOptions() const
{
    if (!hasGRPCOptions()){throw std::runtime_error("gRPC options not set");}
    return pImpl->mGRPCOptions;
}

bool FrontendOptions::hasGRPCOptions() const noexcept
{
    return pImpl->mHasGRPCOptions;
}

/// The maximum number of publishers
void FrontendOptions::setMaximumNumberOfPublishers(
    const int maxPublishers)
{
    if (maxPublishers < 1)
    {
        throw std::invalid_argument(
           "Maximum number of publishers must be positive");
    }
    pImpl->mMaximumPublishers = maxPublishers;
}

int FrontendOptions::getMaximumNumberOfPublishers() const noexcept
{
    return pImpl->mMaximumPublishers;
}

/// Max message size
void FrontendOptions::setMaximumMessageSizeInBytes(
    const int maxMessageSize)
{
    if (maxMessageSize < 1)
    {
        throw std::invalid_argument(
           "Maximum message size must be positive");
    }
    pImpl->mMaximumMessageSizeInBytes = std::optional<int> (maxMessageSize);
}

std::optional<int> 
    FrontendOptions::getMaximumMessageSizeInBytes() const noexcept
{
    return pImpl->mMaximumMessageSizeInBytes;
}
