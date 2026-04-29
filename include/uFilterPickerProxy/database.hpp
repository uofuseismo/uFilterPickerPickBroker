#ifndef UFILTER_PICKER_PROXY_DATABASE_HPP
#define UFILTER_PICKER_PROXY_DATABASE_HPP
#include <memory>
#include <filesystem>
namespace UFilterPickerProxyAPI::V1
{
 class Pick;
}
namespace UFilterPickerProxy
{
/// @name Database database.hpp
/// @brief A simple SQlite3 database for managing submitted picks.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class Database
{
public:
    enum class Mode
    {
        Create,
        ReadWrite,
        ReadOnly
    }; 
public:
    /// @brief Opens/creates the database.
    Database(const std::filesystem::path &sqliteFile, Mode mode);
    /// @result True indicates the database is open.
    [[nodiscard]] bool isOpen() const noexcept;
    /// @result True indicates the database is open in read-only mode
    ///         - i.e., no writing.
    [[nodiscard]] bool isReadOnly() const noexcept;
    /// @brief Add a pick.
    void add(const UFilterPickerProxyAPI::V1::Pick &pick);
    /// @brief Destructor.
    ~Database();

    Database& operator=(const Database &) = delete;
    Database& operator=(Database &&) noexcept = delete;
    Database(const Database &) = delete;
    Database(Database &&) noexcept = delete;
private:
    class DatabaseImpl;
    std::unique_ptr<DatabaseImpl> pImpl;
};
}
#endif
