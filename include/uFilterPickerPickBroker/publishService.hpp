#ifndef UFILTER_PICKER_PICK_BROKER_PUBLISH_SERVICE_HPP
#define UFILTER_PICKER_PICK_BROKER_PUBLISH_SERVICE_HPP
#include <spdlog/logger.h>
#include <memory>
#include <functional>
namespace UFilterPickerPickBrokerAPI::V1
{
 class Pick;
}
namespace UFilterPickerPickBroker
{
 class PublishServiceOptions;
}

namespace UFilterPickerPickBroker
{
/// @class PublishService publishService.hpp
/// @brief This is the publish service to which picks are submitted.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class PublishService
{
public:
    /// @brief Constructor.
    /// @param[in] options   The publish service's options.
    /// @param[in] callback  The callback that allows picks to be propagated.
    /// @param[in] logger    The logging utility.
    PublishService(const PublishServiceOptions &options,
                   const std::function<void (UFilterPickerPickBrokerAPI::V1::Pick &&)> &callback,
                   std::shared_ptr<spdlog::logger> logger);

    /// @brief Starts the publish service's pick acquisition.
    void start();
    /// @brief Stops the publish service and prevents the reading of picks.
    void stop();

    /// @brief Destructor.
    ~PublishService();
    PublishService() = delete;
    PublishService(const PublishService &) = delete;
    PublishService(PublishService &&) noexcept = delete;
    PublishService &operator=(const PublishService &) = delete;
    PublishService &operator=(PublishService &&) noexcept = delete;
private:
    class PublishServiceImpl;
    std::unique_ptr<PublishServiceImpl> pImpl;
};
}
#endif
