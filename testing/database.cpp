//#include <bit>
//#include <type_traits>
#include <filesystem>
//#include <limits>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <vector>
#include <numeric>
#include <string>
#include <chrono>
#ifndef NDEBUG
#include <cassert>
#endif
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <google/protobuf/util/time_util.h>
#include "uFilterPickerPickBroker/database.hpp"
#include "uFilterPickerPickBroker/exception.hpp"
#include <uFilterPickerPickBrokerAPI/v1/pick.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/phase_hint.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/stream_identifier.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/algorithm.pb.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
//#include <catch2/catch_approx.hpp>
//#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include "utilities.hpp"

namespace
{

bool comparePicks(const auto &lhs, const auto &rhs)
{
    if (lhs.stream_identifier().network() !=
        rhs.stream_identifier().network()){return false;}
    if (lhs.stream_identifier().station() !=
        rhs.stream_identifier().station()){return false;}
    if (lhs.stream_identifier().channel() !=
        rhs.stream_identifier().channel()){return false;}
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

bool contains(const auto &pick, const std::vector<UFilterPickerPickBrokerAPI::V1::Pick> &vector)
{
    for (const auto &item : vector)
    {
        if (comparePicks(item, pick)){return true;}
    }
    return false;
}

bool contains(const std::vector<UFilterPickerPickBrokerAPI::V1::Pick> &vector,
              const auto &pick)
{
    return contains(pick, vector);
}

bool comparePicks(const std::vector<UFilterPickerPickBrokerAPI::V1::Pick> &lhs,
                  const std::vector<UFilterPickerPickBrokerAPI::V1::Pick> &rhs)
{
    if (lhs.size() != rhs.size()){return false;}
    for (const auto &lhsi : lhs)
    {
        if (!::contains(lhsi, rhs)){return false;}
    }
    return true;
}

}

TEST_CASE("UFilterPickerPickBroker", "[Database]")
{
    namespace UFP = UFilterPickerPickBroker;
    SECTION("Simple create and delete")
    {
        auto logger = spdlog::stdout_color_mt("create-db-logger-1"); // NOLINT
        const std::filesystem::path sqliteFile{"testFile.sqlite3"};
        if (std::filesystem::exists(sqliteFile)){std::filesystem::remove(sqliteFile);}
        UFP::Database db{sqliteFile, UFP::Database::Mode::Create, logger};
        REQUIRE(db.isOpen());
        REQUIRE(!db.isReadOnly());

        const std::string network{"UU"};
        const std::string station{"KNb "};
        const std::string channel{" hHZ"};
        const std::string locationCode{"01"};
        const std::string algorithmName{"ufp-test"};
        const std::string algorithmVersion{"0.1.0"};
        const std::string algorithmTag{"322389ds"};
        const std::chrono::seconds pickTime{1777408746};
        const auto phaseHint{UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P};
        auto identifier = ::createIdentifier(network, station, channel, locationCode);
        auto algorithm = ::createAlgorithm(algorithmName, algorithmVersion, algorithmTag);
        auto pick = ::createPick(pickTime, identifier, algorithm, phaseHint);

        REQUIRE_NOTHROW(db.add(pick));
        REQUIRE_THROWS_AS(db.add(pick), UFilterPickerPickBroker::DuplicatePickException);

        auto allPicks = db.getAllPicks();
        REQUIRE(allPicks.size() == 1);
        REQUIRE(::comparePicks(allPicks.at(0), pick) == true);
        constexpr bool useLoadTime{false};
        REQUIRE(db.deletePicksBefore(pickTime + std::chrono::seconds {0}, useLoadTime) == 0);
        REQUIRE(db.deletePicksBefore(pickTime + std::chrono::seconds {1}, useLoadTime) == 1);
    }

    SECTION("Create two")
    {
        auto logger = spdlog::stdout_color_mt("create-db-logger-2"); // NOLINT
        const std::filesystem::path sqliteFile{"testFile.sqlite3"};
        if (std::filesystem::exists(sqliteFile)){std::filesystem::remove(sqliteFile);}
        UFP::Database db{sqliteFile, UFP::Database::Mode::Create, logger};
        REQUIRE(db.isOpen());
        REQUIRE(!db.isReadOnly());

        const std::string network{"UU"};
        const std::string station1{"KNb "};
        const std::string station2{"PCCW"};
        const std::string channel{" hHZ"};
        const std::string locationCode{"01"};
        const std::string algorithmName{"ufp-test"};
        const std::string algorithmVersion{"0.1.0"};
        const std::string algorithmTag{"322389ds"};
        const std::chrono::seconds pickTime{1777408746};
        const auto phaseHint{UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P};
        auto identifier = ::createIdentifier(network, station1, channel, locationCode);
        auto algorithm = ::createAlgorithm(algorithmName, algorithmVersion, algorithmTag);
        auto pick1 = ::createPick(pickTime, identifier, algorithm, phaseHint);

        REQUIRE_NOTHROW(db.add(pick1));
        REQUIRE_THROWS_AS(db.add(pick1), UFilterPickerPickBroker::DuplicatePickException);

        identifier = ::createIdentifier(network, station2, channel, locationCode);
        auto pick2 = ::createPick(pickTime, identifier, algorithm, phaseHint);
        REQUIRE_NOTHROW(db.add(pick2));

        auto allPicks = db.getAllPicks();
        REQUIRE(allPicks.size() == 2); 
        REQUIRE(::contains(pick1, allPicks) == true);
        REQUIRE(::contains(pick2, allPicks) == true);
        //REQUIRE(::comparePicks(allPicks.at(0), pick) == true);
        constexpr bool useLoadTime{false};
        REQUIRE(db.deletePicksBefore(pickTime + std::chrono::seconds {0}, useLoadTime) == 0);
        REQUIRE(db.deletePicksBefore(pickTime + std::chrono::seconds {1}, useLoadTime) == 2); 
    }
}
