#ifndef UFILTER_PICKER_PICK_BROKER_METRICS_SINGLETON_HPP
#define UFILTER_PICKER_PICK_BROKER_METRICS_SINGLETON_HPP
#include <memory>
namespace UFilterPickerPickBroker
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
    ///        broker middleware is getting bogged down and picks are not
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

    /// @brief Increments the duplicate picks received counter.
    void incrementDuplicatePicksReceivedCounter();
    /// @result The number of duplicate picks received.
    [[nodiscard]] int64_t getDuplicatePicksReceivedCount() const noexcept;

    /// @brief Increments the number of picks sent counter.
    void incrementPicksSentCounter();
    /// @result The number of sent picks.
    [[nodiscard]] int64_t getPicksSentCount() const noexcept;

    /// @brief Updates the publish service utilization.
    /// @param[in] utilization  This utilization in the range of [0, 1].
    void updatePublishServiceUtilization(double utilization);
    /// @result The publish service utilization.  To convert to percent multiply by 100.
    [[nodiscard]] double getPublishServiceUtilization() const noexcept;

    /// @brief Updates the subscribe service utilization.
    /// @param[in] utilization  This utilization in the range of [0, 1].
    void updateSubscribeServiceUtilization(double utilization);
    /// @result The subscribe service utilization.  To convert to percent multiply by 100.
    [[nodiscard]] double getSubscribeServiceUtilization() const noexcept;

    /// @brief Resets the counters and clears maps.  This is useful for unit tests.
    void resetCounters();
private:
    MetricsSingleton() = default;
    ~MetricsSingleton() = default;
    std::atomic<int64_t> mOverflowInputPicksCounter{0};
    std::atomic<int64_t> mPicksReceivedCounter{0};
    std::atomic<int64_t> mInvalidPicksReceivedCounter{0};
    std::atomic<int64_t> mDuplicatePicksCounter{0};
    std::atomic<int64_t> mPicksSentCounter{0};
    std::atomic<double> mPublishServiceUtilization{0};
    std::atomic<double> mSubscribeServiceUtilization{0};
};

/// @brief Convenience function to instantiate the metrics singleton once at the
///        beginning of the main program.
void initializeMetricsSingleton();

}
#endif
