#ifndef UFILTER_PICKER_PICK_BROKER_SUBSCRIBE_SERVICE_OPTIONS_HPP
#define UFILTER_PICKER_PICK_BROKER_SUBSCRIBE_SERVICE_OPTIONS_HPP
#include <memory>
namespace UFilterPickerPickBroker
{
 class GRPCServerOptions;
 class PickStoreOptions;
}
namespace UFilterPickerPickBroker
{
class SubscribeServiceOptions
{
public:
    /// @brief Move constructor.
    SubscribeServiceOptions();
    /// @brief Copy constructor.
    SubscribeServiceOptions(const SubscribeServiceOptions &options);
    /// @brief Move constructor.
    SubscribeServiceOptions(SubscribeServiceOptions &&options) noexcept;

    /// @brief Sets the gRPC server options.
    /// @param[in] options  The gRPC server options.
    void setGRPCOptions(const GRPCServerOptions &options);
    /// @result The gRPC server options.
    /// @throws std::runtime_error if \c hasGRPCOptions() is false.
    [[nodiscard]] GRPCServerOptions getGRPCOptions() const;
    /// @result True indicates the gRPC options were set.
    [[nodiscard]] bool hasGRPCOptions() const noexcept;

    /// @param[in] maxSubscribers  The maximum number of subscribers.
    void setMaximumNumberOfSubscribers(const int maxSubscribers);
    /// @result The maximum number of subscribers.
    /// @note By default this is 64.
    [[nodiscard]] int getMaximumNumberOfSubscribers() const noexcept;

    /// @brief Sets the output queue size.
    void setQueueCapacity(int queueCapacity);
    /// @result The maximum output queue size.
    [[nodiscard]] int getQueueCapacity() const noexcept;

    /// @brief Sets the maximum pick retention period.
    void setMaximumRetentionDuration(const std::chrono::seconds &duration);
    /// @result The maximum pick message retention duration.
    [[nodiscard]] std::chrono::seconds getMaximumRetentionDuration() const noexcept;

    /// @brief Sets the pick store options.
    void setPickStoreOptions(const PickStoreOptions &options);
    /// @result The pick store options.
    [[nodiscard]] PickStoreOptions getPickStoreOptions() const noexcept;

    /// @brief Destructor.
    ~SubscribeServiceOptions();
    /// @brief Copy assignment.
    SubscribeServiceOptions& operator=(const SubscribeServiceOptions &options);
    /// @brief Move assignment.
    SubscribeServiceOptions& operator=(SubscribeServiceOptions &&options) noexcept;
private:
    class SubscribeServiceOptionsImpl;
    std::unique_ptr<SubscribeServiceOptionsImpl> pImpl;
};
}
#endif
