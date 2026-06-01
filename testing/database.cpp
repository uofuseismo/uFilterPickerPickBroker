#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>
//NOLINTBEGIN(misc-include-cleaner)
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
//NOLINTEND(misc-include-cleaner)
#include <google/protobuf/util/time_util.h>
#include "uFilterPickerPickBroker/database.hpp"
#include "uFilterPickerPickBroker/exception.hpp"
#include <uFilterPickerPickBrokerAPI/v1/pick.pb.h>
//#include <uFilterPickerPickBrokerAPI/v1/stream_identifier.pb.h>
//#include <uFilterPickerPickBrokerAPI/v1/phase_hint.pb.h>
#include <catch2/catch_test_macros.hpp>
#include "utilities.hpp"

namespace
{

[[nodiscard]] std::chrono::seconds getNow() 
{
     auto now 
        = std::chrono::duration_cast<std::chrono::seconds>
          ((std::chrono::high_resolution_clock::now()).time_since_epoch());
     return now;
}

/*
bool comparePicks(const UFilterPickerPickBrokerAPI::V1::Pick &lhs,
                  const UFilterPickerPickBrokerAPI::V1::Pick &rhs)
{
    if (lhs.stream_identifier().network() !=
        rhs.stream_identifier().network())
    {
        return false;
    }
    if (lhs.stream_identifier().station() !=
        rhs.stream_identifier().station())
    {
        return false;
    }
    if (lhs.stream_identifier().channel() !=
        rhs.stream_identifier().channel())
    {
        return false;
    }
    if (lhs.stream_identifier().location_code() !=
        rhs.stream_identifier().location_code())
    {
        return false;
    }
    if (google::protobuf::util::TimeUtil::TimestampToNanoseconds(lhs.time()) !=
        google::protobuf::util::TimeUtil::TimestampToNanoseconds(rhs.time()))
    {
        return false;
    }
    if (lhs.phase_hint() != rhs.phase_hint()){return false;}
    if (lhs.algorithm().name() != rhs.algorithm().name()){return false;}
    if (lhs.algorithm().version() != rhs.algorithm().version()){return false;}
    if (lhs.algorithm().tag() != rhs.algorithm().tag()){return false;}
    return true;
}

bool containsPick(const UFilterPickerPickBrokerAPI::V1::Pick &needle,
                  const std::vector<std::pair<std::chrono::nanoseconds,
                                               UFilterPickerPickBrokerAPI::V1::Pick>> &haystack)
{
    for (const auto &[t, p] : haystack)
    {
        if (::comparePicks(p, needle)){return true;}
    }
    return false;
}
*/

}

TEST_CASE("Database", "[Database]")
{
    namespace UFP = UFilterPickerPickBroker;
    const std::filesystem::path sqliteFile{"testDatabase.sqlite3"};

    SECTION("Add and duplicate pick")
    {
        const auto receivedTime = ::getNow();

        if (std::filesystem::exists(sqliteFile)){std::filesystem::remove(sqliteFile);}

        auto logger = spdlog::stdout_color_mt("db-test-1"); // NOLINT
        UFP::Database db{sqliteFile, UFP::Database::Mode::Create, logger};
        REQUIRE(db.isOpen());

        const auto phaseHint = UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P;
        auto identifier = ::createIdentifier("UU", "KNB", "HHZ", "01");
        auto algorithm  = ::createAlgorithm("ufp", "1.0.0", "abc");
        auto pick1 = ::createPick(std::chrono::seconds{1777408746},
                                  identifier, algorithm, phaseHint);

        // Add a duplicate pick
        REQUIRE_NOTHROW(db.add(receivedTime, pick1));
        REQUIRE_THROWS_AS(db.add(receivedTime + std::chrono::seconds {1}, pick1), UFP::DuplicatePickException);

        auto picks = db.load();
        REQUIRE(picks.size() == 1);
        REQUIRE(::comparePicks(picks.at(0).second, pick1));
    }

    SECTION("Add two picks and load")
    {
        const auto receivedTime = ::getNow();

        if (std::filesystem::exists(sqliteFile)){std::filesystem::remove(sqliteFile);}

        auto logger = spdlog::stdout_color_mt("db-test-2"); // NOLINT
        UFP::Database db{sqliteFile, UFP::Database::Mode::Create, logger};
        REQUIRE(db.isOpen());

        const auto phaseHint = UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P;
        auto identifier = ::createIdentifier("UU", "KNB", "HHZ", "01");
        auto algorithm  = ::createAlgorithm("ufp", "1.0.0", "abc");
        auto pick1 = ::createPick(std::chrono::seconds{1777408746},
                                  identifier, algorithm, phaseHint);

        auto identifier2 = ::createIdentifier("UU", "PCCW", "HHZ", "");
        auto pick2 = ::createPick(std::chrono::seconds{1777408800},
                                   identifier2, algorithm, phaseHint);

        REQUIRE_NOTHROW(db.add(receivedTime, pick1));
        REQUIRE_NOTHROW(db.add(receivedTime, pick2));

        auto picks = db.load();
        REQUIRE(picks.size() == 2);
        REQUIRE(::containsPick(pick1, picks));
        REQUIRE(::containsPick(pick2, picks));
    }

    SECTION("Delete picks before time")
    {
        auto logger = spdlog::stdout_color_mt("db-test-3"); // NOLINT
        UFP::Database db{sqliteFile, UFP::Database::Mode::Create, logger};
        REQUIRE(db.isOpen());

        const auto phaseHint = UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P;
        auto identifier1 = ::createIdentifier("UU", "KNB", "HHZ", "01");
        auto algorithm   = ::createAlgorithm("ufp", "1.0.0", "abc");
        // Do something a little wonky - add the "first" pick after the "second" pick
        const auto receivedTime1 = ::getNow() - std::chrono::seconds{2};
        const auto receivedTime2 = ::getNow() - std::chrono::seconds{3};
        auto pick1 = ::createPick(std::chrono::seconds{1777408746},
                                  identifier1, algorithm, phaseHint);

        auto identifier2 = ::createIdentifier("UU", "PCCW", "HHZ", "");
        auto pick2 = ::createPick(std::chrono::seconds{1777408744},
                                  identifier2, algorithm, phaseHint);

        REQUIRE_NOTHROW(db.add(receivedTime1, pick1));
        REQUIRE_NOTHROW(db.add(receivedTime2, pick2));

        auto picks = db.load();
        REQUIRE(picks.size() == 2);
        const auto deleteTime = std::min(receivedTime1, receivedTime2);

        REQUIRE(db.deletePicksBefore(deleteTime) == 0);
        REQUIRE(db.deletePicksBefore(deleteTime + std::chrono::nanoseconds{1}) == 1);
        REQUIRE(db.load().size() == 1);
    }
}

TEST_CASE("Database restore", "[DatabaseRestore]")
{
    namespace UFP = UFilterPickerPickBroker;
    const std::filesystem::path sqliteFile{"testDatabaseRestore.sqlite3"};

    const auto pickTime = ::getNow() - std::chrono::seconds{10};
    const auto phaseHint = UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P;
    auto identifier1 = ::createIdentifier("UU", "KNB",  "HHZ", "01");
    auto identifier2 = ::createIdentifier("UU", "PCCW", "HHZ", "");
    auto algorithm   = ::createAlgorithm("ufp", "1.0.0", "abc");
    
    auto pick1 = ::createPick(pickTime, identifier1, algorithm, phaseHint);
    auto pick2 = ::createPick(pickTime, identifier2, algorithm, phaseHint);

    SECTION("Write picks then close database as if it were a shutdown")
    {
        const auto receivedTime = ::getNow();

        auto logger = spdlog::stdout_color_mt("db-restore-write"); // NOLINT
        UFP::Database db{sqliteFile, UFP::Database::Mode::Create, logger};
        REQUIRE_NOTHROW(db.add(receivedTime, pick1));
        REQUIRE_NOTHROW(db.add(receivedTime, pick2));
        REQUIRE_NOTHROW(db.close());
    }

    SECTION("Reopen database and read picks as if it were a restart")
    {
        auto logger = spdlog::stdout_color_mt("db-restore-read"); // NOLINT
        REQUIRE(std::filesystem::exists(sqliteFile) == true);
        const UFP::Database db{sqliteFile, UFP::Database::Mode::ReadWrite, logger};
        REQUIRE(db.isOpen());

        auto picks = db.load();
        REQUIRE(picks.size() == 2);
        REQUIRE(::containsPick(pick1, picks));
        REQUIRE(::containsPick(pick2, picks));
        REQUIRE(picks.at(0).first <= picks.at(1).first);

        std::filesystem::remove(sqliteFile);
    }
}
