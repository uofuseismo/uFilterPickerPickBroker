#include <cstdint>
#include <atomic>
#include <mutex>
#include <algorithm>
#include "uFilterPickerProxy/metricsSingleton.hpp"

using namespace UFilterPickerProxy;


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
    mFrontendUtilization.store(0, std::memory_order_relaxed);
    mBackendUtilization.store(0, std::memory_order_relaxed);
}

void MetricsSingleton::incrementOverflowInputPicksCounter()
{
    constexpr int64_t one{1};
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

void MetricsSingleton::updateFrontendUtilization(const double utilization)
{
    mFrontendUtilization.store(std::min(std::max(0.0, utilization), 1.0),
                               std::memory_order_relaxed);
}

void MetricsSingleton::updateBackendUtilization(const double utilization)
{
    mBackendUtilization.store(std::min(std::max(0.0, utilization), 1.0),
                              std::memory_order_relaxed);
}

void UFilterPickerProxy::initializeMetricsSingleton()
{
    MetricsSingleton::getInstance();
}

