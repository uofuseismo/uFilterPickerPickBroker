#ifndef UFILTER_PICKER_PROXY_PROXY_OPTIONS_HPP
#define UFILTER_PICKER_PROXY_PROXY_OPTIONS_HPP
#include <memory>
namespace UFilterPickerProxy
{
 class FrontendOptions;
 class BackendOptions;
}
namespace UFilterPickerProxy
{
/// @class ProxyOptions proxyOptions.hpp
/// @brief The options defining the proxy.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class ProxyOptions
{
public:
    /// @brief Constructor.
    ProxyOptions();
    /// @brief Copy constructor.
    ProxyOptions(const ProxyOptions &options);
    /// @brief Move constructor.
    ProxyOptions(ProxyOptions &&options) noexcept;

    /// @brief Sets the frontend options.  This is where picks are submitted
    ///        by publishers.
    /// @param[in] options  The frontend options.
    void setFrontendOptions(const FrontendOptions &options);
    /// @result The frontend options.  
    /// @throws std::runtime_error if \c haveFrontendOptions() is false. 
    [[nodiscard]] FrontendOptions getFrontendOptions() const;
    /// @result True indicates the frontend options were set.
    [[nodiscard]] bool haveFrontendOptions() const noexcept;

    /// @brief Sets the backend options.  This is where picks are consumed
    ///        by subscribers.
    /// @param[in] options  The backend options.
    void setBackendOptions(const BackendOptions &options);
    /// @result The backend options.  
    /// @throws std::runtime_error if \c haveBackendOptions() is false. 
    [[nodiscard]] BackendOptions getBackendOptions() const;
    /// @result True indicates the backend options were set.
    [[nodiscard]] bool haveBackendOptions() const noexcept;

    /// @brief Destructor.
    ~ProxyOptions();
    /// @brief Copy assignment.
    ProxyOptions& operator=(const ProxyOptions &options);
    /// @brief Move assignment.
    ProxyOptions& operator=(ProxyOptions &&options) noexcept;
private:
    class ProxyOptionsImpl;
    std::unique_ptr<ProxyOptionsImpl> pImpl;
};
}
#endif
