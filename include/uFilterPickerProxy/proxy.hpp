#ifndef UFILTER_PICKER_PROXY_PROXY_HPP
#define UFILTER_PICKER_PROXY_PROXY_HPP
#include <future>
#include <memory>
#include <spdlog/logger.h>
namespace UFilterPickerProxy
{
 class ProxyOptions;
}
namespace UFilterPickerProxy
{
/// @class Proxy proxy.hpp
/// @brief The proxy reads picks from publishers on the frontend and propagates
///        those picks to the subscribers on the backend.
/// @copyright Ben Baker (University of Utah) distributed under the MIT
///            NO AI license.
class Proxy
{
public:
    /// @brief Constructor.
    /// @param[in] options  The options defining the proxy.
    /// @param[in] logger   The logger.
    Proxy(const ProxyOptions &options,
          std::shared_ptr<spdlog::logger> logger);

    /// @brief Starts the proxy.
    std::future<void> start();

    /// @result True indicates the proxy is running.
    [[nodiscard]] bool isRunning() const noexcept;

    /// @brief Stops the proxy.
    void stop();

    /// @brief Destructor.
    ~Proxy();

    Proxy() = delete;
    Proxy(const Proxy &) = delete;
    Proxy(Proxy &&) noexcept = delete;
    Proxy& operator=(const Proxy &) = delete;
    Proxy& operator=(Proxy &&) noexcept = delete;
private:
    class ProxyImpl;
    std::unique_ptr<ProxyImpl> pImpl;
};
}
#endif
