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
}

TEST_CASE("UFilterPickerPickBroker", "[Database]")
{
    namespace UFP = UFilterPickerPickBroker;
    SECTION("Create")
    {
        auto logger = spdlog::stdout_color_mt("create-db-logger"); // NOLINT
        const std::filesystem::path sqliteFile{"testFile.sqlite3"};
        if (std::filesystem::exists(sqliteFile)){std::filesystem::remove(sqliteFile);}
        UFP::Database db{logger, sqliteFile, UFP::Database::Mode::Create};
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
        UFilterPickerPickBrokerAPI::V1::StreamIdentifier identifier;
        identifier.set_network(network);
        identifier.set_station(station);
        identifier.set_channel(channel);
        identifier.set_location_code(locationCode);

        UFilterPickerPickBrokerAPI::V1::Algorithm algorithm;
        algorithm.set_name(algorithmName);
        algorithm.set_version(algorithmVersion);
        algorithm.set_tag(algorithmTag);

        UFilterPickerPickBrokerAPI::V1::Pick pick;
        *pick.mutable_stream_identifier() = identifier;
        *pick.mutable_algorithm() = algorithm;
        *pick.mutable_time() = google::protobuf::util::TimeUtil::SecondsToTimestamp(pickTime.count());
        pick.set_phase_hint(phaseHint);

        REQUIRE_NOTHROW(db.add(pick));
        REQUIRE_THROWS_AS(db.add(pick), UFilterPickerPickBroker::DuplicatePickException);

        auto allPicks = db.getAllPicks();
        REQUIRE(allPicks.size() == 1);
        REQUIRE(::comparePicks(allPicks.at(0), pick) == true);
        REQUIRE(db.deletePicksBefore(pickTime + std::chrono::seconds {0}) == 0);
        REQUIRE(db.deletePicksBefore(pickTime + std::chrono::seconds {1}) == 1);
    }

}
