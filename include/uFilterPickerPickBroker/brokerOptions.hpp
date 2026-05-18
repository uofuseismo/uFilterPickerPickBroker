#ifndef UFILTER_PICKER_PICK_BROKER_BROKER_OPTIONS_HPP
#define UFILTER_PICKER_PICK_BROKER_BROKER_OPTIONS_HPP
#include <chrono>
#include <memory>
namespace UFilterPickerPickBroker
{
 class PublishServiceOptions;
 class SubscribeServiceOptions;
}
namespace UFilterPickerPickBroker
{
/// @class BrokerOptions brokerOptions.hpp
/// @brief The options defining the broker.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class BrokerOptions
{
public:
    /// @brief Constructor.
    BrokerOptions();
    /// @brief Copy constructor.
    BrokerOptions(const BrokerOptions &options);
    /// @brief Move constructor.
    BrokerOptions(BrokerOptions &&options) noexcept;

    /// @brief Sets the publish service options.  This is where picks are
    ///        submitted by publishers.
    /// @param[in] options  The publish service options.
    void setPublishServiceOptions(const PublishServiceOptions &options);
    /// @result The publish service options.
    /// @throws std::runtime_error if \c hasPublishServiceOptions() is false.
    [[nodiscard]] PublishServiceOptions getPublishServiceOptions() const;
    /// @result True indicates the publish service options were set.
    [[nodiscard]] bool hasPublishServiceOptions() const noexcept;

    /// @brief Sets the subscribe service options.  This is where picks are
    ///        consumed by subscribers.
    /// @param[in] options  The subscribe service options.
    void setSubscribeServiceOptions(const SubscribeServiceOptions &options);
    /// @result The subscribe service options.
    /// @throws std::runtime_error if \c hasSubscribeServiceOptions() is false.
    [[nodiscard]] SubscribeServiceOptions getSubscribeServiceOptions() const;
    /// @result True indicates the subscribe service options were set.
    [[nodiscard]] bool hasSubscribeServiceOptions() const noexcept;

    /// @brief Sets the internal queue size.
    void setQueueCapacity(int queueCapacity);
    /// @result The maximum internal queue size.
    [[nodiscard]] int getQueueCapacity() const noexcept;

    /// @brief Determines how long the picks can remain in the database before
    ///        being purged.
    void setPickRetentionInterval(const std::chrono::milliseconds &retention);
    /// @result The pick retention time in the database.
    [[nodiscard]] std::chrono::milliseconds getPickRetentionInterval() const noexcept;

    /// @brief Destructor.
    ~BrokerOptions();
    /// @brief Copy assignment.
    BrokerOptions& operator=(const BrokerOptions &options);
    /// @brief Move assignment.
    BrokerOptions& operator=(BrokerOptions &&options) noexcept;
private:
    class BrokerOptionsImpl;
    std::unique_ptr<BrokerOptionsImpl> pImpl;
};
}
#endif
