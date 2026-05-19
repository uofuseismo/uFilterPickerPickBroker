#include <cstdint>
#include <cstddef>
#include <cctype>
#include <atomic>
#include <thread>
#include <utility>
#include <memory>
#include <string>
#include <vector>
#include <exception>
#include <functional>
#include <future>
#include <chrono>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <ranges>
#include <stdexcept>
#ifndef NDEBUG
#include <cassert>
#endif
#include <google/protobuf/util/time_util.h>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h> //NOLINT
#include "uFilterPickerPickBroker/broker.hpp"
#include "uFilterPickerPickBroker/brokerOptions.hpp"
#include "uFilterPickerPickBroker/database.hpp"
#include "uFilterPickerPickBroker/publishService.hpp"
#include "uFilterPickerPickBroker/publishServiceOptions.hpp"
#include "uFilterPickerPickBroker/subscribeService.hpp"
#include "uFilterPickerPickBroker/subscribeServiceOptions.hpp"
#include "uFilterPickerPickBroker/metricsSingleton.hpp"
#include "uFilterPickerPickBroker/pickStore.hpp"
#include "uFilterPickerPickBroker/pickStoreOptions.hpp"
#include "uFilterPickerPickBroker/exception.hpp"
#include "uFilterPickerPickBrokerAPI/v1/pick.pb.h"
#include "uFilterPickerPickBrokerAPI/v1/algorithm.pb.h"
#include "uFilterPickerPickBrokerAPI/v1/stream_identifier.pb.h"

using namespace UFilterPickerPickBroker;

namespace
{

std::string capitalizeAndRemoveBlanks(const std::string &input)
{
    std::string result{input};
    const auto ret = std::ranges::remove_if(result, ::isspace);
    result.erase(ret.begin(), ret.end());
    std::ranges::transform(result, result.end(), ::toupper);
    return result;
}

UFilterPickerPickBrokerAPI::V1::StreamIdentifier checkAndFixStreamIdentifier(
    UFilterPickerPickBrokerAPI::V1::StreamIdentifier &&identifierIn)
{
    UFilterPickerPickBrokerAPI::V1::StreamIdentifier identifier{std::move(identifierIn)};
    if (!identifier.has_network())
    {
        throw std::invalid_argument("Network not set");
    }
    auto network = ::capitalizeAndRemoveBlanks(identifier.network());
    if (network.empty()){throw std::invalid_argument("Network is empty");}
    identifier.set_network(network);

    if (!identifier.has_station())
    {
        throw std::invalid_argument("Station not set");
    }
    auto station = ::capitalizeAndRemoveBlanks(identifier.station());
    if (station.empty()){throw std::invalid_argument("Station is empty");}
    identifier.set_station(station);

    if (!identifier.has_channel())
    {
        throw std::invalid_argument("Channel not set");
    }
    auto channel = ::capitalizeAndRemoveBlanks(identifier.channel());
    if (channel.empty()){throw std::invalid_argument("Channel is empty");}
    identifier.set_channel(channel);

    if (!identifier.has_location_code())
    {
        identifier.set_location_code("--");
    }
    else
    {
        auto location = capitalizeAndRemoveBlanks(identifier.location_code());
        if (location.empty())
        {
            identifier.set_location_code("--");
        }
        else
        {
            identifier.set_location_code(location);
        }
    }
    return identifier;
}

UFilterPickerPickBrokerAPI::V1::Algorithm checkAndFixAlgorithm(
    UFilterPickerPickBrokerAPI::V1::Algorithm &&algorithmIn)
{
    UFilterPickerPickBrokerAPI::V1::Algorithm algorithm{std::move(algorithmIn)};
    if (!algorithm.has_name())
    {
        algorithm.set_name("uFilterPicker");
    }
    if (!algorithm.has_version())
    {
        throw std::invalid_argument("Algorithm version not set");
    }
    return algorithm;
}

}

class Broker::BrokerImpl
{
public:
    BrokerImpl(const BrokerOptions &options,
               std::unique_ptr<Database> &&database,
               std::shared_ptr<spdlog::logger> logger) :
        mOptions(options),
        mDatabase(std::move(database)),
        mLogger(std::move(logger))
    {
        if (mDatabase == nullptr)
        {
            throw std::invalid_argument("Database is null");
        }
        if (!mDatabase->isOpen())
        {
            throw std::invalid_argument("Database is not open");
        }
        if (!mOptions.hasPublishServiceOptions())
        {
            throw std::invalid_argument(
                "Publish service options not set on broker");
        }
        if (!mOptions.hasSubscribeServiceOptions())
        {
            throw std::invalid_argument(
                "Subscribe service options not set on broker");
        }
        if (mLogger == nullptr)
        {
            // NOLINTBEGIN(misc-include-cleaner)
            auto classId
                = std::to_string (reinterpret_cast<std::uintptr_t> (this));
            mLogger = spdlog::stdout_color_mt("BrokerConsole-" + classId);
            // NOLINTEND(misc-include-cleaner)
        }
        mMaximumInputQueueSize = static_cast<size_t> (mOptions.getQueueCapacity());

        // Create publish service (picks come in)
        const auto publishServiceOptions = mOptions.getPublishServiceOptions();
        mPublishService
            = std::make_unique<PublishService> (publishServiceOptions,
                                                mAddPickCallbackFunction,
                                                mLogger);

        // Create subscribe service (picks go out)
        const auto subscribeServiceOptions = mOptions.getSubscribeServiceOptions();
        auto loadedPicks = mDatabase->load();
        /*
        mSubscribeService
            = std::make_unique<SubscribeService> (mOptions.getSubscribeServiceOptions(),
                                                  loadedPicks,
                                                  mLogger);
        */
        mInitialized = true;
    }

    /// Start the broker
    void start()
    {
        if (mStarted)
        {
            throw std::runtime_error("Cannot restart broker");
        }
#ifndef NDEBUG
        assert(mPublishService != nullptr);
        //assert(mSubscribeService != nullptr);
        assert(mDatabase->isOpen());
#endif
        mKeepRunning.store(true);
        // Start receiving things ASAP
        mPublishServiceFuture = mPublishService->start();
        // Get the propagator pick going
        mPickProcessorFuture = std::async(&BrokerImpl::processPick, this);
        // Make a thread to clean the database
        mDatabaseCleanerFuture = std::async(&BrokerImpl::cleanDatabase, this);
        // Start broadcasting things
        //mSubscribeServiceFuture = mSubsribeService->start();
        mStarted = true;
    }

    /// Stop the broker
    void stop()
    {
        mKeepRunning.store(false);
        constexpr std::chrono::milliseconds pause{15};
        if (mPublishService)
        {
            SPDLOG_LOGGER_INFO(mLogger, "Stopping publish service");
            mPublishService->stop();
            std::this_thread::sleep_for(pause);
        }
        if (mSubscribeService)
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Stopping subscribe service");
            mSubscribeService->stop();
            std::this_thread::sleep_for(pause);
        }
        mDatabase->close();
        mIsRunning.store(false);
        mShutdownRequested = true;
        mShutdownCondition.notify_all();

        if (mPublishServiceFuture.valid()){mPublishServiceFuture.get();}
        if (mPickProcessorFuture.valid()){mPickProcessorFuture.get();}
        if (mSubscribeServiceFuture.valid()){mSubscribeServiceFuture.get();}
        if (mDatabaseCleanerFuture.valid()){mDatabaseCleanerFuture.get();}
    }

    [[nodiscard]] bool checkFuturesOkay(
        const std::chrono::milliseconds &waitForFuture)
    {
        bool isOkay{true};
/*
        try
        {
            auto status = mSubscribeServiceFuture.wait_for(waitForFuture);
            if (status == std::future_status::ready)
            {
                mSubscribeServiceFuture.get();
            }
        }
        catch (const std::exception &e) 
        {
            SPDLOG_LOGGER_CRITICAL(mLogger,
                                   "Fatal error in subscribe service: {}",
                                   std::string {e.what()});
            isOkay = false;
        }
*/
        try
        {
            auto status = mPublishServiceFuture.wait_for(waitForFuture);
            if (status == std::future_status::ready)
            {
                mPublishServiceFuture.get();
            }
        }   
        catch (const std::exception &e) 
        {   
            SPDLOG_LOGGER_CRITICAL(mLogger,
                                   "Fatal error in publish service: {}",
                                   std::string {e.what()});
            isOkay = false;
        }

        try
        {
            auto status = mPickProcessorFuture.wait_for(waitForFuture);
            if (status == std::future_status::ready)
            {
                mPickProcessorFuture.get();
            }
        }
        catch (const std::exception &e) 
        {
            SPDLOG_LOGGER_CRITICAL(mLogger,
                                   "Fatal error in broker pick processor: {}",
                                   std::string {e.what()});
            isOkay = false;
        }

        try
        {
            auto status = mDatabaseCleanerFuture.wait_for(waitForFuture);
            if (status == std::future_status::ready)
            {
                mDatabaseCleanerFuture.get();
            }
        }
        catch (const std::exception &e) 
        {
            SPDLOG_LOGGER_CRITICAL(mLogger,
                                   "Fatal error in database cleaner: {}",
                                   std::string {e.what()});
            isOkay = false;
        }
        return isOkay;
    }

    void processPick()
    {
        while (mKeepRunning.load(std::memory_order_relaxed))
        {
            UFilterPickerPickBrokerAPI::V1::Pick pick;
            bool gotPick{false};
            {
            const std::lock_guard lock(mMutex);
            if (!mInputQueue.empty())
            {
                pick = std::move(mInputQueue.front());
                mInputQueue.pop();
                gotPick = true;
            }
            }
            if (gotPick)
            {
SPDLOG_LOGGER_INFO(mLogger, "REceived pick!");
                bool enqueuePick{false};
                try
                {
                    enqueuePick = true;
                    const auto now 
                        = std::chrono::high_resolution_clock::now()
                          .time_since_epoch();
                    const std::chrono::nanoseconds receivedTime
                    {
                        std::chrono::nanoseconds(now)
                    };
                    mDatabase->add(receivedTime, pick);
                }
                catch (const UFilterPickerPickBroker::DuplicatePickException &e)
                {
                    SPDLOG_LOGGER_DEBUG(
                        mLogger,
                        "Detected duplicate pick - will not propagate - details {}",
                        std::string {e.what()});
                    mMetrics.incrementDuplicatePicksReceivedCounter();
                    enqueuePick = false;
                }
                catch (const std::exception &e)
                {
                    SPDLOG_LOGGER_ERROR(mLogger,
                                        "Pick to database failed because {}",
                                        std::string {e.what()});
                    enqueuePick = false;
                }
                if (enqueuePick)
                {
                    try
                    {
                        mSubscribeService->enqueue(std::move(pick));
                    }
                    catch (const std::exception &e)
                    {
                        SPDLOG_LOGGER_ERROR(mLogger,
                                            "Failed to enqueue pick because {}",
                                            std::string {e.what()});
                    }
                }
            }
            else
            {
                constexpr std::chrono::milliseconds timeOut{25};
                std::this_thread::sleep_for(timeOut);
            }
        }
    }

    void cleanDatabase()
    {
        auto pickRetentionInterval = mOptions.getPickRetentionInterval();
        constexpr std::chrono::seconds cleanEvery{60};
        while (mKeepRunning.load(std::memory_order_relaxed))
        {
            if (mDatabase->isOpen())
            {
                const auto now
                    = std::chrono::high_resolution_clock::now()
                      .time_since_epoch();
                const std::chrono::nanoseconds oldestLoadTime
                    = std::chrono::nanoseconds(now) - pickRetentionInterval;
                auto nDeleted
                    = mDatabase->deletePicksBefore(oldestLoadTime);
                if (nDeleted > 0)
                {
                    SPDLOG_LOGGER_INFO(mLogger, "Deleted {} picks", nDeleted);
                }
                else
                {
                    SPDLOG_LOGGER_DEBUG(mLogger, "No picks to delete");
                }
            }
            std::unique_lock<std::mutex> lock(mShutdownMutex);
            mShutdownCondition.wait_for(lock, cleanEvery,
                                        [this]
                                        {
                                           return mShutdownRequested;
                                        });
        }
    }

    /// @brief Defines the callback for getting/propagating a pick.
    void addPickCallback(UFilterPickerPickBrokerAPI::V1::Pick &&pick)
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
        auto identifier = pick.stream_identifier();
        auto newIdentifier = ::checkAndFixStreamIdentifier(std::move(identifier));
        *pick.mutable_stream_identifier() = std::move(newIdentifier);
        auto algorithm = pick.algorithm();
        auto newAlgorithm = ::checkAndFixAlgorithm(std::move(algorithm));
        *pick.mutable_algorithm() = std::move(newAlgorithm);
        const auto pickTime
            = google::protobuf::util::TimeUtil::TimestampToNanoseconds(
                 pick.time());
        const auto now
            = std::chrono::duration_cast<std::chrono::microseconds>
              ((std::chrono::high_resolution_clock::now()).time_since_epoch());
        if (pickTime > now.count())
        {
            throw std::invalid_argument("Pick cannot be from future");
        }
        {
        const std::lock_guard lock{mMutex};
        while (mInputQueue.size() >= mMaximumInputQueueSize)
        {
            SPDLOG_LOGGER_WARN(mLogger, "Popping pick from queue");
            mInputQueue.pop();
        }
        mInputQueue.push(std::move(pick));
std::cout << mInputQueue.size() << std::endl;
        }
    }

    /// @result True indicates the broker is still running
    [[nodiscard]] bool isRunning() const noexcept
    {
        return mIsRunning.load();
    }
//private:
    BrokerOptions mOptions;
    std::unique_ptr<Database> mDatabase{nullptr};
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::future<void> mPublishServiceFuture;
    std::future<void> mSubscribeServiceFuture;
    std::future<void> mPickProcessorFuture;
    std::future<void> mDatabaseCleanerFuture;
    std::mutex mMutex;
    std::function<void(UFilterPickerPickBrokerAPI::V1::Pick &&)>
        mAddPickCallbackFunction
    {
        std::bind(&BrokerImpl::addPickCallback, this,
                  std::placeholders::_1)
    };
    UFilterPickerPickBroker::MetricsSingleton &mMetrics
    {
        UFilterPickerPickBroker::MetricsSingleton::getInstance()
    };
    std::unique_ptr<PublishService> mPublishService{nullptr};
    std::unique_ptr<SubscribeService> mSubscribeService{nullptr};
    std::queue<UFilterPickerPickBrokerAPI::V1::Pick> mInputQueue;
    size_t mMaximumInputQueueSize{4096};
    std::atomic<bool> mIsRunning{false};
    std::atomic<bool> mKeepRunning{true};
    std::mutex mShutdownMutex;
    std::condition_variable mShutdownCondition;
    bool mShutdownRequested{false};
    bool mInitialized{false};
    bool mStarted{false};
};

/// Constructor
Broker::Broker(const BrokerOptions &options,
               std::unique_ptr<Database> &&database,
               std::shared_ptr<spdlog::logger> logger) :
    pImpl(std::make_unique<BrokerImpl> (options,
                                        std::move(database),
                                        std::move(logger)))
{
}

/// Destructor
Broker::~Broker() = default;

/// Start the broker
void Broker::start()
{
    if (!isInitialized())
    {
        throw std::runtime_error("Broker not initialized");
    }
    pImpl->start(); 
}

/// Stop the broker
void Broker::stop()
{
    pImpl->stop();
}

/// Is the broker running?
bool Broker::isRunning() const noexcept
{
    return pImpl->isRunning();
}

/// Is the broker initialized?
bool Broker::isInitialized() const noexcept
{
    return pImpl->mInitialized;
}

/// Futures okay?
bool Broker::checkFuturesOkay(const std::chrono::milliseconds &waitFor)
{
    return pImpl->checkFuturesOkay(waitFor);
}
