#include <algorithm>
#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>
//NOLINTBEGIN(misc-include-cleaner)
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
//NOLINTEND(misc-include-cleaner)
#include <catch2/catch_test_macros.hpp>
#include <uFilterPickerPickBrokerAPI/v1/pick.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/phase_hint.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/stream_identifier.pb.h>
#include "uFilterPickerPickBroker/pickStore.hpp"
#include "uFilterPickerPickBroker/pickStoreOptions.hpp"
#include "utilities.hpp"

using namespace UFilterPickerPickBroker;

namespace
{
template<typename T> T getNow()
{
     auto now
        = std::chrono::duration_cast<T>
          ((std::chrono::high_resolution_clock::now()).time_since_epoch());
     return now;
}

std::vector
<
    std::pair
    <
        std::chrono::nanoseconds,
        UFilterPickerPickBrokerAPI::V1::Pick
    >
>
generatePicks(const int nPicks = 100)
{
    const auto phaseHint
        = UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P;
    auto identifier = ::createIdentifier("UU", "KNB", "HHZ", "01");
    auto algorithm  = ::createAlgorithm("ufp", "1.0.0", "abc");
    std::vector
    <
        std::pair
        <
            std::chrono::nanoseconds,
            UFilterPickerPickBrokerAPI::V1::Pick
        >
    > result;
    auto thisTime = ::getNow<std::chrono::seconds> ();
    for (int iPick = 0; iPick < nPicks; ++iPick)
    {
        auto pickTime = thisTime
                      - std::chrono::seconds (nPicks - iPick);
        auto pick = ::createPick(pickTime,
                                 identifier,
                                 algorithm,
                                 phaseHint); 
        result.push_back(std::pair {pickTime, pick});
    }
    return result;        
}

}

TEST_CASE("UFilterPickerPickBroker", "[PickStore]")
{
    SECTION("No backfill")
    {
        const int nPicks = 100;
        auto picks = ::generatePicks(nPicks);
        auto logger = spdlog::stdout_color_mt("ps-no-backfill-1"); // NOLINT
        PickStoreOptions options;
        PickStore store{options, logger};
        // Send the first half
        const auto nHalf = static_cast<int> (nPicks/2);
        for (int i = 0; i < nHalf; ++i)
        {
            store.enqueue(picks.at(i).first, picks.at(i).second);
        }
        // Subscribe
        const std::uintptr_t id1{101};
        const std::uintptr_t id2{102};
        REQUIRE(store.getNumberOfSubscribers() == 0);
        store.subscribe(id1, picks.at(0).first - std::chrono::seconds {1});
        REQUIRE(store.isSubscribed(id1) == true);
        store.subscribe(id2); // Subscribe to first new pick
        REQUIRE(store.isSubscribed(id2) == true);
        REQUIRE(store.getNumberOfSubscribers() == 2);
        // Get first half
        auto picksBack11 = store.getPicks(id1);
        auto picksBack21 = store.getPicks(id2); 
        REQUIRE(static_cast<int> (picksBack11.size()) == nHalf);
        REQUIRE(picksBack21.empty() == true);
        for (int i = 0; i < nHalf; ++i)
        {
            REQUIRE(comparePicks(picks.at(i).second,
                                 picksBack11.at(i)) == true);
        }
        // Send some more
        auto nHalfPicks10 = std::min(nHalf + 10, nPicks);
        for (int i = nHalf; i < nHalfPicks10; ++i)
        {
            store.enqueue(picks.at(i).first, picks.at(i).second);
        }
        // Get those and id1 gives up
        auto picksBack12 = store.getPicks(id1);
        auto picksBack22 = store.getPicks(id2);
        REQUIRE(static_cast<int> (picksBack12.size()) ==
                std::max(0, nHalfPicks10 - nHalf));
        REQUIRE(static_cast<int> (picksBack22.size()) ==
                std::max(0, nHalfPicks10 - nHalf - 1));
        for (int i = nHalf; i < nHalfPicks10; ++i)
        {
            REQUIRE(comparePicks(picksBack12.at(i - nHalf),
                                 picks.at(i).second) == true);
            if (i > nHalf)
            {
                REQUIRE(comparePicks(picksBack22.at(i - nHalf - 1),
                                     picks.at(i).second) == true);
            }
        }

        store.unsubscribe(id1);
        REQUIRE(store.isSubscribed(id1) == false);
        REQUIRE(store.getNumberOfSubscribers() == 1);
        // Get rest
        for (int i = nHalfPicks10; i < nPicks; ++i)
        {
            store.enqueue(picks.at(i).first, picks.at(i).second);
        }
        auto picksBack23 = store.getPicks(id2);
        REQUIRE(static_cast<int> (picksBack23.size()) ==
                std::max(0, nPicks - nHalfPicks10));
        for (int i = nHalfPicks10; i < nPicks; ++i)
        {
            REQUIRE(comparePicks(picksBack23.at(i - nHalfPicks10),
                                 picks.at(i).second) == true);
        }

        store.unsubscribe(id2);
        REQUIRE(store.getNumberOfSubscribers() == 0);
    }
}
