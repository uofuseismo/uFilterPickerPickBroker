#include <iostream>
#include <algorithm>
#ifndef NDEBUG
#include <cassert>
#endif
#include <cstdint>
#include <cstddef>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <chrono>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include "uFilterPickerPickBroker/pickStore.hpp"
#include "uFilterPickerPickBroker/pickStoreOptions.hpp"
#include "uFilterPickerPickBrokerAPI/v1/pick.pb.h"

using namespace UFilterPickerPickBroker;

constexpr std::chrono::nanoseconds MAX_TIME{std::numeric_limits<int64_t>::max()};

class PickStore::PickStoreImpl
{
public:
    PickStoreImpl(const PickStoreOptions &options,
                  std::shared_ptr<spdlog::logger> logger) :
        mOptions(options),
        mLogger(std::move(logger))
    {   
        if (mLogger == nullptr)
        {
            // NOLINTBEGIN(misc-include-cleaner)
            auto classId = std::to_string(reinterpret_cast<std::uintptr_t>(this));
            mLogger = spdlog::stdout_color_mt("PickStoreConsole-" + classId);
            // NOLINTEND(misc-include-cleaner)
        }
    } 

    PickStoreImpl(const PickStoreOptions &options,
                  std::vector
                  <
                     std::pair
                     <
                         std::chrono::nanoseconds,
                         UFilterPickerPickBrokerAPI::V1::Pick
                     >
                  > &backfillPicks,
                  std::shared_ptr<spdlog::logger> logger) :
        mOptions(options),
        mLogger(std::move(logger))
    {
        if (mLogger == nullptr)
        {
            // NOLINTBEGIN(misc-include-cleaner)
            auto classId
                 = std::to_string(reinterpret_cast<std::uintptr_t> (this));
            mLogger = spdlog::stdout_color_mt("PickStoreConsole-" + classId);
            // NOLINTEND(misc-include-cleaner)
        }
        mMaxHistory = mOptions.getMaximumRetentionDuration();
        mMaxQueueCapacity = mOptions.getMaximumQueueCapacity();
        for (auto &[t, p] : backfillPicks)
        {
            mDeque.push_back({t, std::move(p)});
        }
        std::ranges::sort(mDeque, [](const auto &lhs, const auto &rhs)
                         {
                             return lhs.first < rhs.first;
                         });
        cleanDeque();
    }

    void subscribe(const uintptr_t contextAddress)
    {
        const std::lock_guard lock(mMutex);
        mCursorMap.insert_or_assign(contextAddress, MAX_TIME);
    }

    void subscribe(const uintptr_t contextAddress,
                   const std::chrono::nanoseconds &startTime)
    {
        const auto now
            = std::chrono::high_resolution_clock::now().time_since_epoch();
        if (startTime > now && startTime != MAX_TIME)
        {
            throw std::invalid_argument("startTime cannot be in the future");
        }
        {
        const std::lock_guard lock(mMutex);
        mCursorMap.insert_or_assign(contextAddress, startTime);
        }
    }

    void unsubscribe(const uintptr_t contextAddress)
    {
        const std::lock_guard lock(mMutex);
        mCursorMap.erase(contextAddress);
    }

    // The key point about the contract is that it is about a minimum
    // retention time.  But if there is space then we can be generous.
    void cleanDeque(const std::chrono::nanoseconds &oldestPickToKeep)
    {
    const auto halfQueueCapacity
        = static_cast<size_t> (mMaxQueueCapacity/2);
    const std::lock_guard lock(mMutex);
    if (!mDeque.empty())
    {
        // There's a ton of space - who cares
        if (mDeque.size() < halfQueueCapacity){return;}
        // Okay, let's clean up a bit to make space 
        while (mDeque.front().first < oldestPickToKeep)
        {
            mDeque.pop_front();
            if (mDeque.empty()){break;}
        }
    }

    }

    void cleanDeque()
    {
        const std::chrono::nanoseconds now
        {
            std::chrono::high_resolution_clock::now().time_since_epoch()
        };
        const auto oldestPickToKeep = now - mMaxHistory;
        cleanDeque(oldestPickToKeep);
    }
   

    void enqueue(const std::chrono::nanoseconds &receivedTime,
                 UFilterPickerPickBrokerAPI::V1::Pick &&pick)
    {
        const std::chrono::nanoseconds now
        {
            std::chrono::high_resolution_clock::now().time_since_epoch()
        };
        const auto oldestPickToKeep = now - mMaxHistory;
        if (receivedTime < oldestPickToKeep)
        {
            SPDLOG_LOGGER_WARN(mLogger, "Pick receipt time past expiration");
        }
        std::pair
        < 
            std::chrono::nanoseconds,
            UFilterPickerPickBrokerAPI::V1::Pick
        > itemToInsert{receivedTime, std::move(pick)};
        cleanDeque();
        {
        const std::lock_guard lock(mMutex);
        bool sortDeque = false;
        if (!mDeque.empty())
        {
            sortDeque = mDeque.back().first > receivedTime;
        }
        mDeque.emplace_back(std::move(itemToInsert));
        if (sortDeque)
        {
            std::sort(mDeque.begin(), mDeque.end(),
                      [](const auto &lhs, const auto &rhs)
            {
                return lhs.first < rhs.first;
            });
        }
        // Let newly arrived subscribers start getting picks
        for (auto &item : mCursorMap)
        {
            if (item.second == MAX_TIME){item.second = mDeque.back().first;}
        }
        }
    }

    [[nodiscard]]
    std::vector<UFilterPickerPickBrokerAPI::V1::Pick> getPicks(
        const uintptr_t contextAddress) const
    {
        std::vector<UFilterPickerPickBrokerAPI::V1::Pick> result;
        {
        const std::lock_guard lock(mMutex);
        auto it = mCursorMap.find(contextAddress);
        if (it == mCursorMap.end()) 
        {
            SPDLOG_LOGGER_WARN(mLogger,
                               "{} not in cursor map",
                               contextAddress);
            return result;
        }
        const auto cursor = it->second;
        std::chrono::nanoseconds lastReadTime{cursor};
        for (const auto &[time, pick] : mDeque)
        {
            if (time >= cursor)
            {
                result.push_back(pick);
                lastReadTime = time;
            }
        }
        if (!result.empty())
        {
#ifndef NDEBUG
            assert(lastReadTime > cursor);
#endif
            // Update the read time but add a slight tolerance so we
            // don't re-read the last pick.
            constexpr std::chrono::nanoseconds epsilon{1};
            it->second = lastReadTime + epsilon;
        }
        }
        return result;
    }

    [[nodiscard]] int getNumberOfSubscribers() const noexcept
    {
        const std::lock_guard lock(mMutex);
        return static_cast<int> (mCursorMap.size());
    }

    [[nodiscard]]
    bool isSubscribed(const uintptr_t contextAddress) const noexcept
    {
        const std::lock_guard lock(mMutex);
        return mCursorMap.contains(contextAddress);
    }

//private:
    void purgeOldPicksWhileMutexLocked()
    {
        const auto now
            = std::chrono::high_resolution_clock::now().time_since_epoch();
        const auto oldestPickToKeep = now - mMaxHistory;
        while (!mDeque.empty() && mDeque.front().first < oldestPickToKeep)
        {
            mDeque.pop_front();
        }
    }

    PickStoreOptions mOptions;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    mutable std::map<uintptr_t, std::chrono::nanoseconds> mCursorMap;
    std::deque
    <
        std::pair
        <
           std::chrono::nanoseconds,
           UFilterPickerPickBrokerAPI::V1::Pick
        >
    > mDeque;
    mutable std::mutex mMutex;
    std::chrono::nanoseconds mMaxHistory{std::chrono::minutes{15}};
    size_t mMaxQueueCapacity{8192}; 
};

PickStore::PickStore(const PickStoreOptions &options,
                     std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<PickStoreImpl> (options,
                                           std::move(logger)))
{
}

PickStore::PickStore(const PickStoreOptions &options,
                     std::vector<std::pair<std::chrono::nanoseconds,
                                           UFilterPickerPickBrokerAPI::V1::Pick>> &backfillPicks,
                     std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<PickStoreImpl> (options, 
                                           backfillPicks,
                                           std::move(logger)))
{
}

void PickStore::subscribe(const uintptr_t contextAddress,
                          const std::chrono::nanoseconds &startTime)
{
    pImpl->subscribe(contextAddress, startTime);
}

void PickStore::subscribe(const uintptr_t contextAddress)
{
    pImpl->subscribe(contextAddress);
}

void PickStore::unsubscribe(const uintptr_t contextAddress)
{
    pImpl->unsubscribe(contextAddress);
}

void PickStore::enqueue(const std::chrono::nanoseconds &receivedTime,
                        UFilterPickerPickBrokerAPI::V1::Pick &&pick)
{
    pImpl->enqueue(receivedTime, std::move(pick));
}

void PickStore::enqueue(const std::chrono::nanoseconds &receivedTime,
                        const UFilterPickerPickBrokerAPI::V1::Pick &pick)
{
   auto copy = pick;
   enqueue(receivedTime, std::move(copy));
}

std::vector<UFilterPickerPickBrokerAPI::V1::Pick>
PickStore::getPicks(const uintptr_t contextAddress) const
{
    return pImpl->getPicks(contextAddress);
}

int PickStore::getNumberOfSubscribers() const noexcept
{
    return pImpl->getNumberOfSubscribers();
}

bool PickStore::isSubscribed(const uintptr_t contextAddress) const noexcept
{
    return pImpl->isSubscribed(contextAddress);
}

PickStore::~PickStore() = default;
