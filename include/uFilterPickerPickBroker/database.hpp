#ifndef UFILTER_PICKER_PICK_BROKER_DATABASE_HPP
#define UFILTER_PICKER_PICK_BROKER_DATABASE_HPP
#include <memory>
#include <filesystem>
#include <chrono>
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
    enum class Mode
    {
        Create,
        ReadWrite,
        ReadOnly
    };
public:
    /// @brief Opens/creates the database.
    Database(std::shared_ptr<spdlog::logger> logger,
             const std::filesystem::path &sqliteFile,
             Mode mode);
    /// @result True indicates the database is open.
    [[nodiscard]] bool isOpen() const noexcept;
    /// @result True indicates the database is open in read-only mode
    ///         - i.e., no writing.
    [[nodiscard]] bool isReadOnly() const noexcept;
    /// @brief Add a pick.
    void add(const UFilterPickerPickBrokerAPI::V1::Pick &pick);

    /// @result All picks currently in the database.
    [[nodiscard]] std::vector<UFilterPickerPickBrokerAPI::V1::Pick> getAllPicks() const;
    /// @result The picks in the database whose time exceeds the given value.
    [[nodiscard]] std::vector<UFilterPickerPickBrokerAPI::V1::Pick> getPicksSince(
        const std::chrono::nanoseconds &startTime) const;
    /// @result Gets the most recently submitted picks.
    [[nodiscard]] std::vector<UFilterPickerPickBrokerAPI::V1::Pick> getMostRecentlySubmittedPicks() const;
    [[nodiscard]] std::vector<UFilterPickerPickBrokerAPI::V1::Pick> getMostRecentlySubmittedPicks(int limit) const;
    /// @brief Deletes picks before a given time.
    /// @result The number of picks deleted.
    [[nodiscard]] int deletePicksBefore(const std::chrono::nanoseconds &endTime);

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
