#ifndef UFILTER_PICKER_PICK_BROKER_PICK_STORE_OPTIONS_HPP
#define UFILTER_PICKER_PICK_BROKER_PICK_STORE_OPTIONS_HPP
#include <chrono>
#include <memory>
namespace UFilterPickerPickBroker
{
/// @class PickStoreOptions pickStoreOptions.hpp
/// @brief Defines the behavior of the pick store.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class PickStoreOptions
{
public:
    /// @brief Constructor.
    PickStoreOptions();
    /// @brief Copy constructor.
    PickStoreOptions(const PickStoreOptions &options);
    /// @brief Move constructor.
    PickStoreOptions(PickStoreOptions &&options) noexcept;

    /// @brief Sets the maximum message queue capacity.
    /// @param[in] maximumQueueCapacity  The maximum number of pick messages that
    ///                                  can be stored in the queue.
    /// @note This should be a large number.  If there are a lot of picks then
    ///       picks younger than the maximum retention duration may be purged.
    void setMaximumQueueCapacity(int maximumQueueCapacity);
    /// @return The maximum queue size.
    [[nodiscard]] int getMaximumQueueCapacity() const noexcept;

    /// @brief Sets the maximum amount of time to retain a pick.
    /// @param[in] duration   Picks received before now - duration are purged.
    void setMaximumRetentionDuration(const std::chrono::seconds &duration);
    /// @result Defines the maximum amount of time a pick can be held.
    [[nodiscard]] std::chrono::seconds getMaximumRetentionDuration() const noexcept;

    /// @brief Destructor.
    ~PickStoreOptions();
    /// @brief Copy assignment.
    PickStoreOptions& operator=(const PickStoreOptions &options);
    /// @brief Move assignment.
    PickStoreOptions& operator=(PickStoreOptions &&options) noexcept;
private:
    class PickStoreOptionsImpl;
    std::unique_ptr<PickStoreOptionsImpl> pImpl;
};
}
#endif
