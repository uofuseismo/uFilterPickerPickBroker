#ifndef UFILTER_PICKER_PICK_BROKER_PICK_STORE_HPP
#define UFILTER_PICKER_PICK_BROKER_PICK_STORE_HPP
#include <memory>
#include <vector>
#include <optional>
#include <chrono>
#include <spdlog/logger.h>
namespace UFilterPickerPickBroker
{
 class PickStoreOptions;
}
namespace UFilterPickerPickBrokerAPI::V1
{
 class Pick;
}
namespace UFilterPickerPickBroker
{
/// @class PickStore pickStore.hpp
/// @brief Manages and publishes picks to real-time pick subscribers.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class PickStore
{
public:
    /// @brief Constructor.
    /// @param[in] options  The options that define the pick store's behavior.
    /// @param[in] logger   The logger.
    PickStore(const PickStoreOptions &options,
              std::shared_ptr<spdlog::logger> logger);
    PickStore(const PickStoreOptions &options,
              std::vector<std::pair<std::chrono::nanoseconds, UFilterPickerPickBrokerAPI::V1::Pick>> &backfillPicks,
              std::shared_ptr<spdlog::logger> logger);

    /// @name Publisher Utilities
    /// @{

    /// @brief Enqueues the next pick for publication to all subscribers.
    /// @param[in,out] pick   The pick to send to subscribers.
    void enqueue(UFilterPickerPickBrokerAPI::V1::Pick &&pick);
    /// @}

    /// @name Subscriber Utilities
    /// @{

    /// @brief Subscribes the context to the pick stream.
    /// @param[in] contextAddress  The memory address of the context that is
    ///                            subscribing.
    /// @param[in] startTime       Picks received after this time will be streamed.
    /// @throws std::invalid_argument if startTime exceeds the current time.
    void subscribe(uintptr_t contextAddress,
                   const std::chrono::nanoseconds &startTime);
    /// @brief Subscribes the context to the pick stream and begins receiving all
    ///        newly acquired pick messages.
    /// @param[in] contextAddress  The memory address of the context that is
    ///                            subscribing.
    void subscribe(uintptr_t contextAddress);

    /// @brief Unsubscribes the context from the pick stream.
    /// @param[in] contextAddress  The memory address of the context that is
    ///                            unsubscribing.
    void unsubscribe(uintptr_t contextAddress);

    /// @brief Fetches the latest pick to publish.
    /// @param contextAddress  The subscribing context address.
    /// @return The latest picks to send to the subscriber.
    [[nodiscard]] std::vector<UFilterPickerPickBrokerAPI::V1::Pick> getPicks(uintptr_t contextAddress) const;
    /// @}

    /// @brief  Destructor.
    ~PickStore();
    PickStore(const PickStore &) = delete;
    PickStore(PickStore &&) noexcept = delete;
    PickStore& operator=(const PickStore &) = delete;
    PickStore& operator=(PickStore &&) noexcept = delete;
private:
    class PickStoreImpl;
    std::unique_ptr<PickStoreImpl> pImpl;
};
}
#endif
