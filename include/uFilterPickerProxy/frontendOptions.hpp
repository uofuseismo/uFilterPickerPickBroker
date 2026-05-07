#ifndef UFILTER_PICKER_PROXY_FRONTEND_OPTIONS_HPP
#define UFILTER_PICKER_PROXY_FRONTEND_OPTIONS_HPP
#include <memory>
#include <optional>
namespace UFilterPickerProxy
{
 class GRPCServerOptions;
}
namespace UFilterPickerProxy
{
/// @class FrontendOptions frontendOptions.hpp
/// @brief Defines the frontend's options.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class FrontendOptions
{
public:
    /// @brief Constructor.
    FrontendOptions();
    /// @brief Copy constructor.
    FrontendOptions(const FrontendOptions &options);
    /// @brief Move constructor.
    FrontendOptions(FrontendOptions &&options) noexcept;

    /// @brief Sets the gRPC server options.
    /// @param[in] options  The gRPC server options.
    void setGRPCOptions(const GRPCServerOptions &options);
    /// @result The gRPC server options.
    /// @throws std::runtime_error if \c hasGRPCOptions() is false.
    [[nodiscard]] GRPCServerOptions getGRPCOptions() const;
    /// @result True indicates the gRPC options were set.
    [[nodiscard]] bool hasGRPCOptions() const noexcept;

    /// @brief Sets the maximum number of publishers.
    /// @param[in] maxPublishers  The maximum number of publishers.
    /// @throws std::invalid_argument if this is not positive. 
    void setMaximumNumberOfPublishers(int maxPublishers);
    /// @result The maximum number of publishers.
    [[nodiscard]] int getMaximumNumberOfPublishers() const noexcept;

    /// @brief Sets the maximum message size in bytes.
    /// @param[in] maxMessageSize  The maximum message size.
    /// @throws std::invalid_argument if this is not positive. 
    void setMaximumMessageSizeInBytes(int maxMessageSize);
    /// @result The maximum number of publishers.
    [[nodiscard]] std::optional<int> getMaximumMessageSizeInBytes() const noexcept;

    /// @brief Sets the maximum number of consecutive invalid messages before
    ///        the client is bounced with a 400 error.
    void setMaximumConsecutiveInvalidMessages(uint32_t maxConsecutiveInvalidMessages) noexcept;
    /// @result After submitting this many invalid messages consecutively
    ///         the client is purged.
    /// @note By default this is 8.
    [[nodiscard]] uint32_t getMaximumConsecutiveInvalidMessages() const noexcept;

    /// @brief Destructor.
    ~FrontendOptions();
    /// @brief Copy assignment.
    FrontendOptions& operator=(const FrontendOptions &options);
    /// @brief Move assignment.
    FrontendOptions& operator=(FrontendOptions &&options) noexcept;
private:
    class FrontendOptionsImpl;
    std::unique_ptr<FrontendOptionsImpl> pImpl;
};
}
#endif
