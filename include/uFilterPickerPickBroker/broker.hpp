#ifndef UFILTER_PICKER_PICK_BROKER_BROKER_HPP
#define UFILTER_PICKER_PICK_BROKER_BROKER_HPP
#include <future>
#include <memory>
#include <spdlog/logger.h>
namespace UFilterPickerPickBroker
{
 class BrokerOptions;
 class Database;
}
namespace UFilterPickerPickBroker
{
/// @class Broker broker.hpp
/// @brief The broker reads picks from publishers on the publish service and
///        propagates those picks to the subscribers on the subscribe service.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class Broker
{
public:
    /// @brief Constructor.
    /// @param[in] options   The options defining the broker.
    /// @param[in] database  The database that allows the broker to restore picks
    ///                      after shutdown - i.e., be stateful.
    /// @param[in] logger    The logger.
    Broker(const BrokerOptions &options,
           std::unique_ptr<Database> &&database,
           std::shared_ptr<spdlog::logger> logger);

    /// @result True indicates the broker is initialized.
    [[nodiscard]] bool isInitialized() const noexcept;

    /// @brief Starts the broker.
    std::future<void> start();

    /// @result True indicates the broker is running.
    [[nodiscard]] bool isRunning() const noexcept;

    /// @brief Stops the broker.
    void stop();

    /// @brief Destructor.
    ~Broker();

    Broker() = delete;
    Broker(const Broker &) = delete;
    Broker(Broker &&) noexcept = delete;
    Broker& operator=(const Broker &) = delete;
    Broker& operator=(Broker &&) noexcept = delete;
private:
    class BrokerImpl;
    std::unique_ptr<BrokerImpl> pImpl;
};
}
#endif
