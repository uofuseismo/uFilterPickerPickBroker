#ifndef UFILTER_PICKER_PICK_BROKER_SUBSCRIBE_SERVICE_HPP
#define UFILTER_PICKER_PICK_BROKER_SUBSCRIBE_SERVICE_HPP
#include <spdlog/logger.h>
#include <memory>
#include <functional>
namespace UFilterPickerPickBrokerAPI::V1
{
 class Pick;
}
namespace UFilterPickerPickBroker
{
 class Database;
 class SubscribeServiceOptions;
 class PickStore;
}

namespace UFilterPickerPickBroker
{
/// @class SubscribeService subscribeService.hpp
/// @brief This is the subscribe service from which picks are consumed.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class SubscribeService
{
public:
    /// @brief Constructor.
    /// @param[in] options   The subscribe service's options.
    /// @param[in] store     The pick store for the subscribe service.
    /// @param[in] logger    The logging utility.
    SubscribeService(const SubscribeServiceOptions &options,
                     std::unique_ptr<PickStore> &&store,
                     std::shared_ptr<spdlog::logger> logger);

    /// @brief Starts the subscribe service's pick publishing.
    void start();

    /// @brief Enqueues a pick to be written to consumers.
    void enqueue(UFilterPickerPickBrokerAPI::V1::Pick &&pick);

    /// @brief Stops the subscribe service and prevents the propagation of picks.
    void stop();

    /// @brief Destructor.
    ~SubscribeService();
    SubscribeService() = delete;
    SubscribeService(const SubscribeService &) = delete;
    SubscribeService(SubscribeService &&) noexcept = delete;
    SubscribeService &operator=(const SubscribeService &) = delete;
    SubscribeService &operator=(SubscribeService &&) noexcept = delete;
private:
    class SubscribeServiceImpl;
    std::unique_ptr<SubscribeServiceImpl> pImpl;
};
}
#endif
