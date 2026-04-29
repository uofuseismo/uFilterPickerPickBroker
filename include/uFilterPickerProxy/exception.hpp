#ifndef UFILTER_PICKER_PROXY_EXCEPTION_HPP
#define UFILTER_PICKER_PROXY_EXCEPTION_HPP
#include <string>
#include <exception>
namespace UFilterPickerProxy
{
/// @class AlreadyExists exception.hpp
/// @brief Use this error when a pick already exists in the database.
/// @copryright Ben Baker (University of Utah) distributed under the
///             MIT NO AI license.
class AlreadyExists final : public std::exception
{
public:
    explicit AlreadyExists(const std::string &message);
    [[nodiscard]] const char *what() const noexcept;
private:
    std::string mMessage; 
};
}
#endif
