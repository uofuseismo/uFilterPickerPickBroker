#include <cctype>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <memory>
#include <set>
#include <array>
#include <algorithm>
#include <mutex>
#include <string>
#include <stdexcept>
#include <filesystem>
#include <map>
#ifndef NDEBUG
#include <cassert>
#endif
#include <sqlite3.h>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <google/protobuf/util/time_util.h>
#include <uFilterPickerProxyAPI/v1/pick.pb.h>
#include <uFilterPickerProxyAPI/v1/stream_identifier.pb.h>
#include <uFilterPickerProxyAPI/v1/phase_hint.pb.h>
#include <uFilterPickerProxyAPI/v1/algorithm.pb.h>
#include "uFilterPickerProxy/database.hpp"
#include "uFilterPickerProxy/exception.hpp"

#define N_PHASE_HINTS 3

using namespace UFilterPickerProxy;

namespace
{

void bindText(const std::string &text,
              const int index,
              const std::string &column,
              const std::string &table,
              sqlite3_stmt *statement)
{
#ifndef NDEBUG
    assert(index > 0);
#endif
    auto returnCode
        = sqlite3_bind_text(
             statement, index,
             text.c_str(), static_cast<int> (text.size()), nullptr);
    if (returnCode != SQLITE_OK)
    {
        sqlite3_finalize(statement);
        throw std::runtime_error("Failed to bind " 
                               + text + " to " 
                               + column + " " + table);
    }
}

std::string removeBlanksAndCapitalize(const std::string &stringIn)
{
    auto string = stringIn; 
    string.erase(
        std::remove_if(string.begin(), string.end(), ::isspace),
        string.end());
    if (string.empty())
    {
        throw std::invalid_argument("SNCL has empty field");
    }
    std::transform(string.begin(), string.end(), string.begin(), ::toupper);
    return string;
}

std::string removeBlanksAndLowerCase(const std::string &stringIn)
{
    auto string = stringIn; 
    string.erase(
        std::remove_if(string.begin(), string.end(), ::isspace),
        string.end());
    if (string.empty())
    {   
        throw std::invalid_argument("Algorithm has empty field");
    }   
    std::transform(string.begin(), string.end(), string.begin(), ::tolower);
    return string;
}

}

class Database::DatabaseImpl
{
public:
    DatabaseImpl(std::shared_ptr<spdlog::logger> logger,
                 const std::filesystem::path &fileName,
                 const Database::Mode mode) :
        mLogger(std::move(logger))
    {
        if (mLogger == nullptr)
        {
            //NOLINTBEGIN(misc-include-cleaner)
            auto classId
                = std::to_string (reinterpret_cast<std::uintptr_t> (this));
            mLogger = spdlog::stdout_color_mt("sqlite-db-" + classId);
            //NOLINTEND(misc-include-cleaner)
        }
#ifndef NDEBUG
        assert(mLogger != nullptr);
#endif
        if (mode == Database::Mode::ReadOnly)
        {
            openReadOnly(fileName);
        }
        else
        {
            bool createDatabase{false};
            if (mode == Database::Mode::Create)
            {
                createDatabase = true;
                if (std::filesystem::exists(fileName))
                {
                    std::filesystem::remove(fileName);
                    SPDLOG_LOGGER_WARN(mLogger,
                                       "Removing existing database {}",
                                       std::string{fileName});
                }
                const auto directory = fileName.parent_path();
                if (!directory.empty())
                {
                    if (!std::filesystem::exists(directory))
                    {
                        if (!std::filesystem::create_directories(directory))
                        {   
                            throw std::runtime_error(
                                "Failed to create directory "
                               + std::string {directory});
                        }   
                    }   
                }   
            }
            else // We're in read-write mode
            {
                if (!std::filesystem::exists(fileName))
                {
                    createDatabase = true;
                    const auto directory = fileName.parent_path();
                    if (!directory.empty())
                    {
                        if (!std::filesystem::exists(directory))
                        {
                            if (!std::filesystem::create_directories(directory))
                            {   
                                throw std::runtime_error(
                                    "Failed to create directory for missing db "
                                   + std::string {directory});
                            }   
                        }   
                    }
                    SPDLOG_LOGGER_INFO(mLogger,
                                       "Will create missing database {}",
                                       std::string{fileName});
                }
            }
            openReadWrite(fileName, createDatabase);
        }
        // Always initialize phase hints
        initializePhaseHintMap();
    }
        
    void openReadOnly(const std::filesystem::path &fileName)
    {
        close();
        if (!std::filesystem::exists(fileName))
        {
            throw std::invalid_argument("Cannot open "
                              + std::string {fileName}
                              + " in read-only mode because it does not exist");
        }
        const char *vfs{nullptr};
        const int flags{SQLITE_OPEN_READWRITE};
        auto returnCode = sqlite3_open_v2(fileName.c_str(), 
                                          &mDatabaseHandle,
                                          flags,
                                          vfs);
        if (returnCode != SQLITE_OK)
        {
            auto errorMessage = std::string{sqlite3_errstr(returnCode)};
            throw std::runtime_error(
                "Failed to open read-only database because "
              + errorMessage);
        }
        mIsOpen = true;
        mTablesInitialized = true;
        mMode = Database::Mode::ReadOnly;
    }

    void openReadWrite(const std::filesystem::path &fileName,
                       const bool createDatabase)
    {
        int flags = SQLITE_OPEN_READWRITE;
        if (createDatabase)
        {
            SPDLOG_LOGGER_INFO(mLogger, "Will create database {}",
                               std::string{fileName});
            mTablesInitialized = false;
            flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
        }
        else
        {
            SPDLOG_LOGGER_INFO(mLogger,
                               "Will open database {} as read-write",
                               std::string{fileName});
            mTablesInitialized = true;
        }
        const char *vfs{nullptr};
        auto returnCode = sqlite3_open_v2(fileName.c_str(), 
                                          &mDatabaseHandle,
                                          flags,
                                          vfs);
        if (returnCode != SQLITE_OK)
        {
            auto errorMessage = std::string{sqlite3_errstr(returnCode)};
            if (createDatabase)
            {
                throw std::runtime_error(
                   "Failed to open database in create mode because "
                  + errorMessage);
            }
            throw std::runtime_error("Failed to open database because "
                                   + errorMessage);
        }
        mIsOpen = true;
        mMode = Database::Mode::ReadWrite;
        if (createDatabase)
        {
            create();
        }
    }

    void close()
    {
        if (isOpen())
        {
            SPDLOG_LOGGER_INFO(mLogger, "Closing database");
            sqlite3_close(mDatabaseHandle);
            mMode = Database::Mode::ReadOnly;
            mIsOpen = false;
            mDatabaseHandle = nullptr;
        }
    }

    [[nodiscard]] bool isOpen() const noexcept
    {
        return mIsOpen;
    }

    void create()
    {
        if (!isOpen())
        {
            throw std::runtime_error("Database not open");
        }
        if (isReadOnly())
        {
            throw std::runtime_error("Cannot create read-only database");
        }
        SPDLOG_LOGGER_INFO(mLogger, "Creating tables");
        mTablesInitialized = false;
        char *errorMessage{nullptr};
        const std::string phaseHintTable{
R"""(
CREATE TABLE phase_hints(
   identifier INTEGER PRIMARY KEY ASC,
   phase TEXT NOT NULL);
)"""
        };
        auto returnCode = sqlite3_exec(mDatabaseHandle,
                                       phaseHintTable.c_str(),
                                       nullptr,
                                       nullptr, 
                                       &errorMessage);
        if (returnCode != SQLITE_OK)
        {
            auto message = std::string{errorMessage}; 
            sqlite3_free(errorMessage); 
            throw std::runtime_error("Failed to create phasehint table because "
                                   + message);
        }

        const std::string defaultPhases{
R"""(
INSERT INTO phase_hints (phase)
 VALUES
    ('Unknown'),
    ('P'),
    ('S');
)"""
        };
        returnCode = sqlite3_exec(mDatabaseHandle,
                                  defaultPhases.c_str(),
                                  nullptr,
                                  nullptr, 
                                  &errorMessage);
        if (returnCode != SQLITE_OK)
        {
            auto message = std::string{errorMessage}; 
            sqlite3_free(errorMessage); 
            throw std::runtime_error("Failed to create default phases because "
                                   + message);
        }

        const std::string streamsTable{
R"""(
CREATE TABLE streams(
   identifier INTEGER PRIMARY KEY ASC,
   network TEXT NOT NULL,
   station TEXT NOT NULL,
   channel TEXT NOT NULL,
   location_code TEXT NOT NULL);
)"""
        };
        returnCode = sqlite3_exec(mDatabaseHandle,
                                  streamsTable.c_str(),
                                  nullptr,
                                  nullptr,
                                  &errorMessage);
        if (returnCode != SQLITE_OK)
        {
            auto message = std::string{errorMessage};
            sqlite3_free(errorMessage);
            throw std::runtime_error(
                "Failed to create streams table because "
               + message);
        }

        const std::string algorithmsTable{
R"""(
CREATE TABLE algorithms(
   identifier INTEGER PRIMARY KEY ASC,
   name TEXT NOT NULL DEFAULT('uFilterPicker'),
   version TEXT NOT NULL,
   tag TEXT DEFAULT(''),
   UNIQUE(name, version, tag)
   );
)"""
        };  
        returnCode = sqlite3_exec(mDatabaseHandle,
                                  algorithmsTable.c_str(),
                                  nullptr,
                                  nullptr,
                                  &errorMessage);
        if (returnCode != SQLITE_OK)
        {
            auto message = std::string{errorMessage};
            sqlite3_free(errorMessage);
            throw std::runtime_error(
                "Failed to create algorithms table because "
               + message);
        }

        const std::string pickTable{
R"""(
CREATE TABLE picks(
   stream INTEGER,
   time INT8, 
   phase_hint INTEGER,
   algorithm INTEGER,
   load_time DATETIME DEFAULT (DATETIME(current_timestamp)),
   UNIQUE(stream, time, phase_hint, algorithm),
   FOREIGN KEY(stream) REFERENCES streams(identifier),
   FOREIGN KEY(phase_hint) REFERENCES phase_hints(phase_identifier),
   FOREIGN KEY(algorithm) REFERENCES algorithms(identifier));
)"""};

        returnCode = sqlite3_exec(mDatabaseHandle,
                                       pickTable.c_str(),
                                       nullptr,
                                       nullptr, 
                                       &errorMessage);
        if (returnCode != SQLITE_OK)
        {
            auto message = std::string{errorMessage}; 
            sqlite3_free(errorMessage); 
            throw std::runtime_error("Failed to create picks table because "
                                   + message);
        }
        mTablesInitialized = true;
    }

    /// @result The stream identifier
    [[nodiscard]] int getStreamIdentifier(
        const UFilterPickerProxyAPI::V1::StreamIdentifier &identifier)
    {
        int streamIdentifier{-1};
        const auto network = ::removeBlanksAndCapitalize(identifier.network());
        const auto station = ::removeBlanksAndCapitalize(identifier.station());
        const auto channel = ::removeBlanksAndCapitalize(identifier.channel());
        std::string locationCode{"--"};
        if (identifier.has_location_code())
        {
            locationCode = ::removeBlanksAndCapitalize(identifier.location_code());
        } 
        const auto name = network + "."
                        + station + "."
                        + channel + "."
                        + locationCode;
        {
        const std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mStreamIdentifiersMap.find(name);
        if (idx != mStreamIdentifiersMap.end())
        {
            return idx->second;
        }
        SPDLOG_LOGGER_INFO(mLogger, "Will add {} to streams", name);
        const std::string insertSQL{
R"""(
INSERT INTO streams(network, station, channel, location_code) VALUES(?, ?, ?, ?) RETURNING identifier;
)"""
        };
	sqlite3_stmt *insertStatement{nullptr};
        auto returnCode = sqlite3_prepare_v2(mDatabaseHandle,
                                             insertSQL.c_str(),
                                             -1,
                                             &insertStatement, 
                                             nullptr);
        if (returnCode != SQLITE_OK)
        {
            sqlite3_finalize(insertStatement);
            throw std::runtime_error("Failed to prepare insert statement");
        }

        ::bindText(network,      1, "network",
                   "streams", insertStatement);
        ::bindText(station,      2, "station",
                   "streams", insertStatement);
        ::bindText(channel,      3, "channel",
                   "streams", insertStatement);
        ::bindText(locationCode, 4, "location_code",
                   "streams", insertStatement);
/*
        std::array<std::pair<std::string, std::string>, 4>
            insertMap
            {
                std::pair<std::string, std::string> {"network", network},
                std::pair<std::string, std::string> {"station", station},
                std::pair<std::string, std::string> {"channel", channel},
                std::pair<std::string, std::string> {"locationCode", locationCode}
            };
        for (size_t i = 1; i < insertMap.size(); ++i)
        {
            const auto index = static_cast<int> (i + 1);
            const auto element = insertMap[i];
            returnCode = sqlite3_bind_text(
                insertStatement,
                index,
                element.second.c_str(),
                static_cast<int> (element.second.size()),
                nullptr);
            if (returnCode != SQLITE_OK)
            {
                sqlite3_finalize(insertStatement);
                throw std::runtime_error("Failed to bind " + element.first);
            }
        }
*/
        // Send it
        returnCode = sqlite3_step(insertStatement);
        // Try to get the corresponding identifier
        if (returnCode == SQLITE_ROW)
        {
            streamIdentifier = sqlite3_column_int(insertStatement, 0);
            SPDLOG_LOGGER_DEBUG(mLogger,
                                "Got stream identifier {} from db",
                                std::to_string(streamIdentifier));
        }
        if (sqlite3_step(insertStatement) != SQLITE_DONE)
        {
            SPDLOG_LOGGER_WARN(mLogger,
                               "There exists more rows but terminating early");
        }
        // Clean up
        if (sqlite3_finalize(insertStatement) != SQLITE_OK)
        {
            throw std::runtime_error("Failed to finalize insert statement");
        }
        mStreamIdentifiersMap.insert( std::pair{name, streamIdentifier} );
        } // Release mutex
        return streamIdentifier;
    }

    /// @result The algorithm identifier
    [[nodiscard]] int getAlgorithmIdentifier(
        const UFilterPickerProxyAPI::V1::Algorithm &algorithm)
    {   
        int algorithmIdentifier{-1};
        const auto name = ::removeBlanksAndLowerCase(algorithm.name());
        const auto version = ::removeBlanksAndLowerCase(algorithm.version());
        std::string tag;
        if (algorithm.has_tag())
        {
            tag = algorithm.tag();
            if (!tag.empty()){tag = ::removeBlanksAndLowerCase(tag);}
        }
        const auto keyName = name + " " 
                           + version
                           + (!tag.empty() ? " " + tag : "");
        {
        const std::lock_guard<std::mutex> lock(mMutex);
        auto idx = mAlgorithmIdentifiersMap.find(keyName);
        if (idx != mAlgorithmIdentifiersMap.end())
        {
            return idx->second;
        }
        SPDLOG_LOGGER_INFO(mLogger, "Will add {} to algorithms", keyName);
        const std::string insertSQL{
R"""(
INSERT INTO algorithms(name, version, tag) VALUES(?, ?, ?) RETURNING identifier;
)"""
        };
        sqlite3_stmt *insertStatement{nullptr};
        auto returnCode = sqlite3_prepare_v2(mDatabaseHandle,
                                             insertSQL.c_str(),
                                             -1,
                                             &insertStatement,
                                             nullptr);
        if (returnCode != SQLITE_OK)
        {
            sqlite3_finalize(insertStatement);
            throw std::runtime_error(
                "Failed to prepare algorithm insert statement");
        }
        ::bindText(name,    1, "name",    "algorithms", insertStatement);
        ::bindText(version, 2, "version", "algorithms", insertStatement);
        ::bindText(tag,     3, "tag",     "algorithms", insertStatement);
/*
        std::array<std::pair<std::string, std::string>, 3>
            insertMap
            {
                std::pair<std::string, std::string> {"name", name},
                std::pair<std::string, std::string> {"version", version},
                std::pair<std::string, std::string> {"tag", tag}
            };
        for (size_t i = 0; i < insertMap.size(); ++i)
        {
            const auto index = static_cast<int> (i + 1);
            const auto element = insertMap.at(i);
            returnCode = sqlite3_bind_text(
                insertStatement,
                index,
                element.second.c_str(),
                static_cast<int> (element.second.size()),
                nullptr);
            if (returnCode != SQLITE_OK)
            {
                sqlite3_finalize(insertStatement); 
                throw std::runtime_error("Failed to bind " + element.first);
            }
        }
*/
        // Send it 
        returnCode = sqlite3_step(insertStatement);
        // Try to get the corresponding identifier
        if (returnCode == SQLITE_ROW)
        {
            algorithmIdentifier = sqlite3_column_int(insertStatement, 0);
            SPDLOG_LOGGER_DEBUG(mLogger,
                                "Got algorithm identifier {} from db",
                                std::to_string(streamIdentifier));
        }
        if (sqlite3_step(insertStatement) != SQLITE_DONE)
        {
            SPDLOG_LOGGER_WARN(mLogger,
                               "There exists more rows but terminating early");
        }
        mAlgorithmIdentifiersMap.insert(
           std::pair{keyName, algorithmIdentifier});
        } // Release mutex
        return algorithmIdentifier;
    }

    /// @brief Attempts to add the pick to the database
    void add(const UFilterPickerProxyAPI::V1::Pick &pick)
    {
        // Tabulate variables
        auto streamIdentifier = getStreamIdentifier(pick.stream_identifier());
        if (streamIdentifier ==-1)
        {
            throw std::runtime_error("Failed to get stream identifier");
        }
        auto algorithmIdentifier = getAlgorithmIdentifier(pick.algorithm());
        if (algorithmIdentifier ==-1)
        {
            throw std::runtime_error("Failed to get algorithm identifier");
        }
        auto phaseHintIdentifier = getPhaseHintIdentifier(pick.phase_hint());
        const int64_t time
            = google::protobuf::util::TimeUtil::TimestampToNanoseconds(
                 pick.time());
/*
        const std::string algorithm 
            = (pick.has_algorithm() ? pick.algorithm() : "uFilterPicker");
*/
//ON CONFLICT DO NOTHING
        const std::string insertSQL{
R"""(
INSERT INTO picks(stream, time, phase_hint, algorithm) VALUES(?, ?, ?, ?);
)"""
        };
        // Insert it
        {

        sqlite3_stmt *insertStatement{nullptr};
        auto returnCode = sqlite3_prepare_v2(mDatabaseHandle,
                                             insertSQL.c_str(),
                                             -1,
                                             &insertStatement,
                                             nullptr);
        returnCode = sqlite3_bind_int(insertStatement, 1, streamIdentifier);
        if (returnCode != SQLITE_OK)
        {
            sqlite3_finalize(insertStatement);
            throw std::runtime_error("Failed to bind stream identifier");
        }
        returnCode = sqlite3_bind_int64(insertStatement, 2, time);
        if (returnCode != SQLITE_OK)
        {
            sqlite3_finalize(insertStatement);
            throw std::runtime_error("Failed to bind time");
        }
        returnCode = sqlite3_bind_int(insertStatement, 3, phaseHintIdentifier);
        if (returnCode != SQLITE_OK)
        {
            sqlite3_finalize(insertStatement);
            throw std::runtime_error("Failed to bind phase hint");
        }
        returnCode = sqlite3_bind_int(insertStatement, 4, algorithmIdentifier);
        if (returnCode != SQLITE_OK)
        {
            sqlite3_finalize(insertStatement);
            throw std::runtime_error("Failed to bind algorithm");
        }
        // Send it
        returnCode = sqlite3_step(insertStatement);
        SPDLOG_LOGGER_INFO(mLogger, "rc {}", returnCode);
        // Clean up
        if (sqlite3_finalize(insertStatement) != SQLITE_OK)
        {
            throw std::runtime_error("Failed to finalize insert statement");
        }

        }
    }

    /// @result True indicates this is a read-only database
    [[nodiscard]] bool isReadOnly() const noexcept
    {
        return mMode == Database::Mode::ReadOnly;
    }

    /// @result Gets the currently available streams
    [[nodiscard]] std::set<std::string> getStreams() const
    {
        std::set<std::string> result;
        {
        const std::lock_guard<std::mutex> lock(mMutex);
        for (const auto &pair : mStreamIdentifiersMap)
        {
            result.insert(pair.first);
        }
        }
        return result;
    } 

    /// @Result Gets the phase hint identifier
    [[nodiscard]] 
    int getPhaseHintIdentifier(
        const UFilterPickerProxyAPI::V1::PhaseHint phaseHint) const
    {
        namespace UFPAPI = UFilterPickerProxyAPI::V1;
        if (phaseHint == UFPAPI::PhaseHint::PHASE_HINT_P)
        {
            return mPhaseHintMap.at("P");
        }
        else if (phaseHint == UFPAPI::PhaseHint::PHASE_HINT_S)
        {
            return mPhaseHintMap.at("S");
        }
        else if (phaseHint == UFPAPI::PhaseHint::PHASE_HINT_UNKNOWN)
        {
            return mPhaseHintMap.at("Unknown");
        }
        else
        {
            throw std::invalid_argument("Unhandled phase hint of type "
                                      + std::to_string(phaseHint)); 
        }
    }

    void initializePhaseHintMap()
    {
        if (!isOpen()){throw std::runtime_error("Database not open");} 
        const std::string phaseHintQuery{
            "SELECT identifier, phase FROM phase_hints"};
	sqlite3_stmt *statement{nullptr};
        {
        const std::lock_guard<std::mutex> lock(mMutex);
	auto returnCode
            = sqlite3_prepare_v2(mDatabaseHandle, 
                                 phaseHintQuery.c_str(),
                                 -1, &statement, nullptr);
        if (returnCode != SQLITE_OK)
        {
            sqlite3_finalize(statement);
            throw std::runtime_error("Failed to prepare select statement");
        }
        while (sqlite3_step(statement) != SQLITE_DONE)
        {
            auto identifier = sqlite3_column_int(statement, 0);
            auto phaseName
                = std::string{
                    reinterpret_cast<const char *>
                        (sqlite3_column_text(statement, 1))
                  };
            mPhaseHintMap.insert_or_assign(phaseName, identifier);
        }
        // Clean up
        if (sqlite3_finalize(statement) != SQLITE_OK)
        {
            throw std::runtime_error("Failed to finalize insert statement");
        }
        }
        if (mPhaseHintMap.size() != N_PHASE_HINTS)
        {
            throw std::runtime_error("Expecting 3 phase hints");
        }
    }

    ~DatabaseImpl()
    {
        close();
    }
//private:
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    mutable std::mutex mMutex;
    sqlite3 *mDatabaseHandle{nullptr};
    std::map<std::string, int> mStreamIdentifiersMap;
    std::map<std::string, int> mPhaseHintMap;
    std::map<std::string, int> mAlgorithmIdentifiersMap;
    Database::Mode mMode{Database::Mode::ReadOnly};
    bool mIsOpen{false}; 
    bool mTablesInitialized{false};
};

Database::Database(
    std::shared_ptr<spdlog::logger> logger,
    const std::filesystem::path &fileName,
    const Database::Mode mode) :
    pImpl(std::make_unique<DatabaseImpl> (std::move(logger), fileName, mode))
{
}

bool Database::isReadOnly() const noexcept
{
    return pImpl->isReadOnly();
}

bool Database::isOpen() const noexcept
{
    return pImpl->isOpen();
}

void Database::add(const UFilterPickerProxyAPI::V1::Pick &pick)
{
    if (!pick.has_stream_identifier())
    {
        throw std::invalid_argument("Stream identifier not set");
    }
    if (!pick.has_time())
    {
        throw std::invalid_argument("Pick time not set");
    }
    const auto &streamIdentifier = pick.stream_identifier();
    if (!streamIdentifier.has_network())
    { 
        throw std::invalid_argument("Network not set");
    }
    if (!streamIdentifier.has_station())
    {
        throw std::invalid_argument("Station not set");
    }
    if (!streamIdentifier.has_channel())
    {
        throw std::invalid_argument("Channel not set");
    }
    if (!isOpen())
    {
        throw std::runtime_error("Database not open");
    }
    if (isReadOnly())
    {
        throw std::invalid_argument("Cannot add pick to read-only database");
    }
    const auto &algorithmIdentifier = pick.algorithm();
    if (!algorithmIdentifier.has_name())
    {
        throw std::invalid_argument("Algorithm name not set");
    }
    if (!algorithmIdentifier.has_version())
    {
        throw std::invalid_argument("Algorithm version not set");
    }
    pImpl->add(pick);
}

/// Destructor
Database::~Database() = default;
