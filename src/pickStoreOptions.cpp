#include <chrono>
#include <utility>
#include <memory>
#include <stdexcept>
#include "uFilterPickerPickBroker/pickStoreOptions.hpp"

using namespace UFilterPickerPickBroker;

class PickStoreOptions::PickStoreOptionsImpl
{
public:
    std::chrono::seconds mMaximumRetentionDuration{std::chrono::minutes {15}};
    int mMaximumQueueCapacity{8192};
};

/// Constructor
PickStoreOptions::PickStoreOptions() :
    pImpl(std::make_unique<PickStoreOptionsImpl> ())
{
}

/// Copy constructor
PickStoreOptions::PickStoreOptions(
    const PickStoreOptions &options)
{
    *this = options;
}

/// Move constructor
PickStoreOptions::PickStoreOptions(
    PickStoreOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
PickStoreOptions &PickStoreOptions::operator=(
    const PickStoreOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<PickStoreOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
PickStoreOptions &PickStoreOptions::operator=(
    PickStoreOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move (options.pImpl);
    return *this;
}

/// Destructor
PickStoreOptions::~PickStoreOptions() = default;

void PickStoreOptions::setMaximumQueueCapacity(const int maxCapacity)
{
    if (maxCapacity < 1)
    {
        throw std::invalid_argument("Maximum queue size must be positive");
    }
    pImpl->mMaximumQueueCapacity = maxCapacity;
}

int PickStoreOptions::getMaximumQueueCapacity() const noexcept
{
    return pImpl->mMaximumQueueCapacity;
}

void PickStoreOptions::setMaximumRetentionDuration(
    const std::chrono::seconds &duration)
{
    if (duration.count() < 1)
    {
        throw std::invalid_argument("Pick retention duration must be positive");
    }
    pImpl->mMaximumRetentionDuration = duration;
}

std::chrono::seconds
    PickStoreOptions::getMaximumRetentionDuration() const noexcept
{
    return pImpl->mMaximumRetentionDuration;
}
