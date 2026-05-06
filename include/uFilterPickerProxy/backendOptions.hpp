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
