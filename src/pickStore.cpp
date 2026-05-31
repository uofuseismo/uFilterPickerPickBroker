#include <cstdint>
#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <chrono>
#include <stdexcept>
#include <algorithm>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include "uFilterPickerPickBroker/pickStore.hpp"
#include "uFilterPickerPickBroker/pickStoreOptions.hpp"
#include "uFilterPickerPickBrokerAPI/v1/pick.pb.h"

using namespace UFilterPickerPickBroker;

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
        const auto now
            = std::chrono::high_resolution_clock::now().time_since_epoch();
        const std::lock_guard lock(mMutex);
        mCursorMap.insert_or_assign(contextAddress, now);
    }

    void subscribe(const uintptr_t contextAddress,
                   const std::chrono::nanoseconds &startTime)
    {
        const auto now
            = std::chrono::high_resolution_clock::now().time_since_epoch();
        if (startTime > now)
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
        }
    }

    [[nodiscard]]
    std::vector<UFilterPickerPickBrokerAPI::V1::Pick> getPicks(
        const uintptr_t contextAddress) const
    {
        const std::lock_guard lock(mMutex);
        auto it = mCursorMap.find(contextAddress);
        if (it == mCursorMap.end()) { return {}; }
        auto &cursor = it->second;
        std::vector<UFilterPickerPickBrokerAPI::V1::Pick> result;
        for (const auto &[time, pick] : mDeque)
        {
            if (time > cursor) { result.push_back(pick); }
        }
        if (!result.empty())
        {
            cursor = mDeque.back().first;
        }
        return result;
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

std::vector<UFilterPickerPickBrokerAPI::V1::Pick>
PickStore::getPicks(const uintptr_t contextAddress) const
{
    return pImpl->getPicks(contextAddress);
}

PickStore::~PickStore() = default;
