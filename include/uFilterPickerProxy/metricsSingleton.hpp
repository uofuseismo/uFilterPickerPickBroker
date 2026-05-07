#ifndef UFILTER_PICKER_PROXY_METRICS_SINGLETON_HPP
#define UFILTER_PICKER_PROXY_METRICS_SINGLETON_HPP
#include <memory>
namespace UFilterPickerProxy
{
/// @class MetricsSingleton metricsSingleton.hpp
/// @brief This class allows metrics to be sent to the OTel collector.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class MetricsSingleton
{
public:
    /// @result An instance of the singleton.  This is thread-safe.
    [[maybe_unused]] static MetricsSingleton &getInstance();

    /// @brief Increments overflow input pick counter.  This indicates the
    ///        proxy middleware is getting bogged down and picks are not
    ///        being propagated.
    void incrementOverflowInputPicksCounter();
    /// @result The overflow input picks count.
    [[nodiscard]] int64_t getOverflowInputPicksCount() const noexcept;

    /// @brief Increments number of picks received.
    void incrementPicksReceivedCounter();
    /// @result The number of picks received.
    [[nodiscard]] int64_t getPicksReceivedCount() const noexcept;

    /// @brief Increments the number of invalid picks received.
    void incrementInvalidPicksReceivedCounter();
    /// @result The number of invalid picks received.
    [[nodiscard]] int64_t getInvalidPicksReceivedCount() const noexcept;

    /// @brief Updates the frontend utilization.
    /// @param[in] utilization  This utilization in the range of [0, 1].
    void updateFrontendUtilization(double utilization);
    /// @result The frontend utilization.  To convert this to a percent multiply by 100.
    [[nodiscard]] double getFrontendUtilization() const noexcept;

    /// @brief Updates the backend utilization.
    /// @param[in] utilization  This utilization in the range of [0, 1].
    void updateBackendUtilization(double utilization);
    /// @result The frontend utilization.  To convert this to a percent multiply by 100.
    [[nodiscard]] double getBackendUtilization() const noexcept;

    /// @brief Resets the counters and clears maps.  This is useful for unit tests.
    void resetCounters();
private:
    MetricsSingleton() = default;
    ~MetricsSingleton() = default;
    std::atomic<int64_t> mOverflowInputPicksCounter{0};
    std::atomic<int64_t> mPicksReceivedCounter{0};
    std::atomic<int64_t> mInvalidPicksReceivedCounter{0};
    std::atomic<double> mFrontendUtilization{0};
    std::atomic<double> mBackendUtilization{0};
};

/// @brief Convenience function to instantiate the metrics singleton once at the
///        beginning of the main program.
void initializeMetricsSingleton();

}
#endif
