#include <cstdint>
#include <atomic>
#include <mutex>
#include <algorithm>
#include "uFilterPickerPickBroker/metricsSingleton.hpp"

using namespace UFilterPickerPickBroker;


MetricsSingleton &MetricsSingleton::getInstance()
{
    std::mutex mutex;
    const std::scoped_lock lock{mutex};
    static MetricsSingleton instance;
    return instance;
}

void MetricsSingleton::resetCounters()
{
    mOverflowInputPicksCounter.store(0, std::memory_order_relaxed);
    mPicksReceivedCounter.store(0, std::memory_order_relaxed);
    mInvalidPicksReceivedCounter.store(0, std::memory_order_relaxed);
    mDuplicatePicksCounter.store(0, std::memory_order_relaxed);
    mPicksSentCounter.store(0, std::memory_order_relaxed);
    mPublishServiceUtilization.store(0, std::memory_order_relaxed);
    mSubscribeServiceUtilization.store(0, std::memory_order_relaxed);
}

void MetricsSingleton::incrementOverflowInputPicksCounter()
{
    mOverflowInputPicksCounter.fetch_add(1, std::memory_order_relaxed);
}

int64_t MetricsSingleton::getOverflowInputPicksCount() const noexcept
{
    return mOverflowInputPicksCounter.load(std::memory_order_relaxed);
}

void MetricsSingleton::incrementPicksReceivedCounter()
{
    constexpr int64_t one{1};
    mPicksReceivedCounter.fetch_add(one, std::memory_order_relaxed);
}

int64_t MetricsSingleton::getPicksReceivedCount() const noexcept
{
    return mPicksReceivedCounter.load(std::memory_order_relaxed);
}

void MetricsSingleton::incrementInvalidPicksReceivedCounter()
{
    constexpr int64_t one{1};
    mInvalidPicksReceivedCounter.fetch_add(one, std::memory_order_relaxed);
}

int64_t MetricsSingleton::getInvalidPicksReceivedCount() const noexcept
{
    return mInvalidPicksReceivedCounter.load(std::memory_order_relaxed);
}

void MetricsSingleton::incrementDuplicatePicksReceivedCounter()
{
    constexpr int64_t one{1};
    mDuplicatePicksCounter.fetch_add(one, std::memory_order_relaxed);
}

int64_t MetricsSingleton::getDuplicatePicksReceivedCount() const noexcept
{
    return mDuplicatePicksCounter.load(std::memory_order_relaxed);
}

void MetricsSingleton::incrementPicksSentCounter()
{
    constexpr int64_t one{1};
    mPicksSentCounter.fetch_add(one, std::memory_order_relaxed);
}

int64_t MetricsSingleton::getPicksSentCount() const noexcept
{
    return mPicksSentCounter.load(std::memory_order_relaxed);
}

void MetricsSingleton::updatePublishServiceUtilization(const double utilization)
{
    mPublishServiceUtilization.store(std::min(std::max(0.0, utilization), 1.0),
                                     std::memory_order_relaxed);
}

double MetricsSingleton::getPublishServiceUtilization() const noexcept
{
    return mPublishServiceUtilization.load(std::memory_order_relaxed);
}

void MetricsSingleton::updateSubscribeServiceUtilization(const double utilization)
{
    mSubscribeServiceUtilization.store(std::min(std::max(0.0, utilization), 1.0),
                                       std::memory_order_relaxed);
}

double MetricsSingleton::getSubscribeServiceUtilization() const noexcept
{
    return mSubscribeServiceUtilization.load(std::memory_order_relaxed);
}

void UFilterPickerPickBroker::initializeMetricsSingleton()
{
    MetricsSingleton::getInstance();
}
