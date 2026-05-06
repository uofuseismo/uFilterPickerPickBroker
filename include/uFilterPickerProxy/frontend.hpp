#ifndef UFILTER_PICKER_PROXY_FRONTEND_HPP
#define UFILTER_PICKER_PROXY_FRONTEND_HPP
#include <spdlog/logger.h>
#include <memory>
#include <functional>
namespace UFilterPickerProxyAPI::V1
{
 class Pick;
}
namespace UFilterPickerProxy
{
 class FrontendOptions;
}

namespace UFilterPickerProxy
{
/// @class Frontend frontend.hpp
/// @brief This is the frontend to which picks are submitted.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class Frontend
{
public:
    /// @brief Constructor.
    /// @param[in] options   The frontend's options.
    /// @param[in] callback  The callback that allows picks to propagated.
    /// @param[in] logger    The logging utility.
    Frontend(const FrontendOptions &options,
             const std::function<void (UFilterPickerProxyAPI::V1::Pick &&)> &callback,
             std::shared_ptr<spdlog::logger> logger);

    /// @brief Starts the frontend's pick acquisition.
    void start();
    /// @brief Stops the frontend and prevents the reading of picks.
    void stop();

    /// @brief Destructor.
    ~Frontend();
    Frontend() = delete;
    Frontend(const Frontend &) = delete;
    Frontend(Frontend &&) noexcept = delete;
    Frontend &operator=(const Frontend &) = delete;
    Frontend &operator=(Frontend &&) noexcept = delete;
private:
    class FrontendImpl;
    std::unique_ptr<FrontendImpl> pImpl;
};
}
#endif
