#include <cstdint>
#include <queue>
#include <deque>
#include <utility>
#include <map>
#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include <mutex>
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
            auto classId
                = std::to_string (reinterpret_cast<std::uintptr_t> (this));
            mLogger = spdlog::stdout_color_mt("PickStoreConsole-"
                                            + classId);
            // NOLINTEND(misc-include-cleaner)
        }
    }
    ~PickStoreImpl()
    {
        unsubscribeAll();
    }
    void unsubscribeAll()
    {
    }

    void subscribe(const uintptr_t contextAddress)
    {
        std::lock_guard lock(mMutex);
        if (!mSubscriberPickMap.contains(contextAddress))
        {
            auto newQueue = std::queue<UFilterPickerPickBrokerAPI::V1::Pick> ();
            auto newElement = std::make_pair(contextAddress, std::move(newQueue));
            mSubscriberPickMap.insert( std::move(newElement) );
        }
    }

    void purgeOldPicks()
    {
        const auto now
            = std::chrono::high_resolution_clock::now().time_since_epoch();
        const std::chrono::nanoseconds oldestTime = now - mMaxHistory;
        std::ranges::remove_if(mPickDeque, [&](const auto &p)
                               {
                                   return p.first < oldestTime;
                               });
    }

    void enqueue(const std::chrono::nanoseconds &timeReceived,
                 UFilterPickerPickBrokerAPI::V1::Pick &&pick)
    {
        const auto now
            = std::chrono::high_resolution_clock::now().time_since_epoch();
        if (timeReceived > now)
        {
            throw std::invalid_argument("Cannot enqueue pick from future");
        }
        const std::chrono::nanoseconds oldestTime = now - mMaxHistory;
        if (timeReceived < oldestTime)
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Cannot enqueue expired pick");
            return;
        }
        {
        const std::lock_guard lock(mMutex);
        if (timeReceived > mPickDeque.back().first)
        {
            mPickDeque.push_front( std::pair{timeReceived, std::move(pick)} );
            return;
        }
        if (timeReceived < mPickDeque.front().first)
        {
            mPickDeque.push_front( std::pair{timeReceived, std::move(pick)} );
            return;
        }
        mPickDeque.push_front( std::pair{timeReceived, std::move(pick)} );
        std::ranges::sort(mPickDeque, [](const auto &lhs, const auto &rhs)
                          {
                              return lhs.first < rhs.first;
                          });
        } // End lock
    }

//private:
    PickStoreOptions mOptions;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::map<uintptr_t, std::queue<UFilterPickerPickBrokerAPI::V1::Pick>> mSubscriberPickMap;
    std::deque
    <
        std::pair
        <
            std::chrono::nanoseconds,
            UFilterPickerPickBrokerAPI::V1::Pick
        >
    > mPickDeque;
    std::mutex mMutex;
    std::chrono::nanoseconds mMaxHistory{std::chrono::minutes {15}};
};

/// @brief Destructor.
PickStore::~PickStore() = default;
