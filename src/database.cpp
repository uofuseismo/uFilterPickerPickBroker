#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>
#include <utility>
#include <vector>
#include <sqlite3.h>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include <uFilterPickerPickBrokerAPI/v1/pick.pb.h>
#include "uFilterPickerPickBroker/database.hpp"
#include "uFilterPickerPickBroker/exception.hpp"

using namespace UFilterPickerPickBroker;

namespace
{

void bindInt64(const int64_t value,
               const int index,
               sqlite3_stmt *statement,
               const char *label)
{
    if (sqlite3_bind_int64(statement, index, value) != SQLITE_OK)
    {
        sqlite3_finalize(statement);
        throw std::runtime_error(std::string{"Failed to bind int64 for "} + label);
    }
}

void bindBlob(const std::string &data,
              const int index,
              sqlite3_stmt *statement,
              const char *label)
{
    auto rc = sqlite3_bind_blob(statement, index,
                                data.data(),
                                static_cast<int>(data.size()),
                                SQLITE_STATIC);
    if (rc != SQLITE_OK)
    {
        sqlite3_finalize(statement);
        throw std::runtime_error(std::string{"Failed to bind blob for "} + label);
    }
}

}

class Database::DatabaseImpl
{
public:
    DatabaseImpl(const std::filesystem::path &fileName,
                 const Mode mode,
                 std::shared_ptr<spdlog::logger> logger) :
        mLogger(std::move(logger))
    {
        if (mLogger == nullptr)
        {
            //NOLINTBEGIN(misc-include-cleaner)
            auto classId = std::to_string(reinterpret_cast<std::uintptr_t>(this));
            mLogger = spdlog::stdout_color_mt("sqlite-db-" + classId);
            //NOLINTEND(misc-include-cleaner)
        }
        bool createSchema{false};
        if (mode == Mode::Create)
        {
            if (std::filesystem::exists(fileName))
            {
                SPDLOG_LOGGER_INFO(mLogger, 
                                   "Deleting existing database {}", 
                                   std::string{fileName});
                if (!std::filesystem::remove(fileName))
                {
                    throw std::runtime_error("Failed to delete existing database "
                                             + std::string{fileName});
                }
            }
            createSchema = true;
        }
        else
        {
            // If the file doesn't exist then I must create the schema
            createSchema = !std::filesystem::exists(fileName);
        }
        open(fileName, createSchema);
    }

    void open(const std::filesystem::path &fileName,
              const bool createSchema)
    {
        close();
        if (createSchema)
        {
            const auto directory = fileName.parent_path();
            if (!directory.empty() && !std::filesystem::exists(directory))
            {
                if (!std::filesystem::create_directories(directory))
                {
                    throw std::runtime_error("Failed to create directory "
                                           + std::string{directory});
                }
            }
        }
        const int flags = createSchema ? SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                                       : SQLITE_OPEN_READWRITE;
        auto rc = sqlite3_open_v2(fileName.c_str(), &mHandle, flags, nullptr);
        if (rc != SQLITE_OK)
        {
            throw std::runtime_error(
                std::string{"Failed to open database: "} + sqlite3_errstr(rc));
        }
        mIsOpen = true;
        if (createSchema)
        {
            SPDLOG_LOGGER_INFO(mLogger, "Creating database {}", std::string{fileName});
            createTables();
        }
        else
        {
            SPDLOG_LOGGER_INFO(mLogger, "Opened database {}", std::string{fileName});
        }
    }

    void createTables()
    {
        const char *sql =
            "CREATE TABLE picks("
            "  received_time INT8 NOT NULL,"
            "  proto         BLOB NOT NULL UNIQUE"
            ");";
        char *errMsg{nullptr};
        auto rc = sqlite3_exec(mHandle, sql, nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK)
        {
            auto msg = std::string{errMsg};
            sqlite3_free(errMsg);
            throw std::runtime_error("Failed to create picks table: " + msg);
        }
    }

    void add(const std::chrono::nanoseconds &receivedTime,
             const UFilterPickerPickBrokerAPI::V1::Pick &pick)
    {
        std::string proto;
        if (!pick.SerializeToString(&proto))
        {
            throw std::runtime_error("Failed to serialize pick");
        }

        const char *sql =
            "INSERT OR IGNORE INTO picks(received_time, proto) VALUES(?, ?);";
        const std::lock_guard<std::mutex> lock(mMutex);
        sqlite3_stmt *stmt{nullptr};
        auto rc = sqlite3_prepare_v2(mHandle, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK)
        {
            throw std::runtime_error("Failed to prepare pick insert statement");
        }
        ::bindInt64(receivedTime.count(), 1, stmt, "received_time");
        ::bindBlob(proto,                 2, stmt, "proto");
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE)
        {
            sqlite3_finalize(stmt);
            throw std::runtime_error("Failed to insert pick");
        }
        const bool inserted = sqlite3_changes(mHandle) > 0;
        sqlite3_finalize(stmt);
        if (!inserted)
        {
            throw DuplicatePickException("Pick already exists in database");
        }
    }

    [[nodiscard]] std::vector<std::pair<std::chrono::nanoseconds,
                                         UFilterPickerPickBrokerAPI::V1::Pick>>
        load() const
    {
        std::vector<std::pair<std::chrono::nanoseconds,
                               UFilterPickerPickBrokerAPI::V1::Pick>> result;
        const char *sql =
            "SELECT received_time, proto FROM picks ORDER BY received_time ASC;";
        const std::lock_guard<std::mutex> lock(mMutex);
        sqlite3_stmt *stmt{nullptr};
        auto rc = sqlite3_prepare_v2(mHandle, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK)
        {
            throw std::runtime_error("Failed to prepare load statement");
        }
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            const auto loadTime
                = std::chrono::nanoseconds{sqlite3_column_int64(stmt, 0)};
            const auto *blobData
                = reinterpret_cast<const char *>(sqlite3_column_blob(stmt, 1));
            const auto blobSize = sqlite3_column_bytes(stmt, 1);
            UFilterPickerPickBrokerAPI::V1::Pick pick;
            if (!pick.ParseFromArray(blobData, blobSize))
            {
                sqlite3_finalize(stmt);
                throw std::runtime_error("Failed to parse pick proto from database");
            }
            result.emplace_back(loadTime, std::move(pick));
        }
        sqlite3_finalize(stmt);
        return result;
    }

    [[nodiscard]] int deletePicksBefore(const std::chrono::nanoseconds &time)
    {
        const char *sql = "DELETE FROM picks WHERE received_time < ?;";
        const std::lock_guard<std::mutex> lock(mMutex);
        sqlite3_stmt *stmt{nullptr};
        auto rc = sqlite3_prepare_v2(mHandle, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK)
        {
            throw std::runtime_error("Failed to prepare delete statement");
        }
        ::bindInt64(time.count(), 1, stmt, "received_time");
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE)
        {
            SPDLOG_LOGGER_WARN(mLogger, "Delete statement did not complete cleanly");
        }
        const int nDeleted = sqlite3_changes(mHandle);
        sqlite3_finalize(stmt);
        return nDeleted;
    }

    [[nodiscard]] bool isOpen() const noexcept { return mIsOpen; }

    void close()
    {
        if (mIsOpen)
        {
            SPDLOG_LOGGER_INFO(mLogger, "Closing database");
            sqlite3_close(mHandle);
            mHandle = nullptr;
            mIsOpen = false;
        }
    }

    ~DatabaseImpl() { close(); }

    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    mutable std::mutex mMutex;
    sqlite3 *mHandle{nullptr};
    bool mIsOpen{false};
};

Database::Database(const std::filesystem::path &fileName,
                   const Mode mode,
                   std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<DatabaseImpl>(fileName, mode, std::move(logger)))
{
}

bool Database::isOpen() const noexcept
{
    return pImpl->isOpen();
}

void Database::add(const std::chrono::nanoseconds &receivedTime,
                   const UFilterPickerPickBrokerAPI::V1::Pick &pick)
{
    if (!pick.has_stream_identifier())
    {
        throw std::invalid_argument("Stream identifier not set");
    }
    if (!pick.has_time())
    {
        throw std::invalid_argument("Pick time not set");
    }
    if (!pick.has_algorithm())
    {
        throw std::invalid_argument("Algorithm not set");
    }
    if (!isOpen())
    {
        throw std::runtime_error("Database not open");
    }
    pImpl->add(receivedTime, pick);
}

std::vector<std::pair<std::chrono::nanoseconds,
                       UFilterPickerPickBrokerAPI::V1::Pick>>
Database::load() const
{
    if (!isOpen())
    {
        throw std::runtime_error("Database not open");
    }
    return pImpl->load();
}

int Database::deletePicksBefore(const std::chrono::nanoseconds &time)
{
    if (!isOpen())
    {
        throw std::runtime_error("Database not open");
    }
    return pImpl->deletePicksBefore(time);
}

void Database::close()
{
    pImpl->close();
}

Database::~Database() = default;
