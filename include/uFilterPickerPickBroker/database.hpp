#ifndef UFILTER_PICKER_PICK_BROKER_DATABASE_HPP
#define UFILTER_PICKER_PICK_BROKER_DATABASE_HPP
#include <memory>
#include <filesystem>
#include <chrono>
#include <string>
#include <vector>
#include <spdlog/logger.h>
namespace UFilterPickerPickBrokerAPI::V1
{
 class Pick;
}
namespace UFilterPickerPickBroker
{
/// @name Database database.hpp
/// @brief A simple SQLite3 database for managing submitted picks.
/// @copyright Ben Baker (University of Utah) distributed under the
///            MIT NO AI license.
class Database
{
public:
    /// @brief Defines how to open the database.
    enum class Mode
    {
        Create,   /*! Creates the database if it does not exist.  
                      This will delete the existing database if it does exist.*/
        ReadWrite /*! Opens the database in read-write mode. */
    };
public:
    /// @brief Opens or creates the database.
    Database(const std::filesystem::path &sqliteFile,
             Mode mode,
             std::shared_ptr<spdlog::logger> logger);
    /// @result True indicates the database is open.
    [[nodiscard]] bool isOpen() const noexcept;

    /// @brief Add a pick. Throws DuplicatePickException if the pick already exists.
    void add(const std::chrono::nanoseconds &receivedTime,
             const UFilterPickerPickBrokerAPI::V1::Pick &pick);

    /// @result All picks currently in the database, ordered by load time.
    [[nodiscard]] std::vector<std::pair<std::chrono::nanoseconds,
                                        UFilterPickerPickBrokerAPI::V1::Pick>>
        load() const;

    /// @brief Deletes picks loaded before a given time.
    /// @param[in] time  Picks loaded before this time will be deleted.
    /// @result The number of picks deleted.
    [[nodiscard]] int deletePicksBefore(const std::chrono::nanoseconds &time);

    /// @brief Close the database.
    void close();

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
