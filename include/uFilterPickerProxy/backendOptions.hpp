#ifndef UFILTER_PICKER_PROXY_BACKEND_OPTIONS_HPP
#define UFILTER_PICKER_PROXY_BACKEND_OPTIONS_HPP
#include <memory>
namespace UFilterPickerProxy
{
 class GRPCServerOptions;
}
namespace UFilterPickerProxy
{
class BackendOptions
{
public:
    /// @brief Move constructor.
    BackendOptions();
    /// @brief Copy constructor.
    BackendOptions(const BackendOptions &options);
    /// @brief Move constructor.
    BackendOptions(BackendOptions &&options) noexcept;

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

    /// @brief Destructor.
    ~BackendOptions();
    /// @brief Copy assignment.
    BackendOptions& operator=(const BackendOptions &options);
    /// @brief Move assignment.
    BackendOptions& operator=(BackendOptions &&options) noexcept;
private:
    class BackendOptionsImpl;
    std::unique_ptr<BackendOptionsImpl> pImpl;
};
}
#endif
