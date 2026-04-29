#include <bit>
#include <type_traits>
#include <filesystem>
#include <limits>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <vector>
#include <algorithm>
#include <numeric>
#include <string>
#include <chrono>
#ifndef NDEBUG
#include <cassert>
#endif
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <google/protobuf/util/time_util.h>
#include "uFilterPickerProxy/database.hpp"
#include <uFilterPickerProxyAPI/v1/pick.pb.h>
#include <uFilterPickerProxyAPI/v1/phase_hint.pb.h>
#include <uFilterPickerProxyAPI/v1/stream_identifier.pb.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
//#include <catch2/catch_approx.hpp>
//#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

TEST_CASE("UFilterPickerProxy", "[Database]")
{
    namespace UFP = UFilterPickerProxy;
    SECTION("Create")
    {
        auto logger = spdlog::stdout_color_mt("create-db-logger");
        const std::filesystem::path sqliteFile{"testFile.sqlite3"};
        if (std::filesystem::exists(sqliteFile)){std::filesystem::remove(sqliteFile);}
        UFP::Database db{logger, sqliteFile, UFP::Database::Mode::Create};
        REQUIRE(db.isOpen()); 
        REQUIRE(!db.isReadOnly());

        const std::string network{"UU"};
        const std::string station{"KNb "};
        const std::string channel{" hHZ"};
        const std::string locationCode{"01"};
        const std::string algorithm{"ufp-test"};
        const std::chrono::seconds pickTime{1777408746};
        UFilterPickerProxyAPI::V1::StreamIdentifier identifier;
        identifier.set_network(network);
        identifier.set_station(station);
        identifier.set_channel(channel);
        identifier.set_location_code(locationCode);
        UFilterPickerProxyAPI::V1::Pick pick;
        *pick.mutable_stream_identifier() = identifier;
        pick.set_algorithm(algorithm);
        *pick.mutable_time() = google::protobuf::util::TimeUtil::SecondsToTimestamp(pickTime.count());

        REQUIRE_NOTHROW(db.add(pick));
         
    }
    
}
