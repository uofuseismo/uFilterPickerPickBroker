#include <cstdint>
#include <atomic>
#include <thread>
#include <utility>
#include <memory>
#include <functional>
#include <chrono>
#include <queue>
#include <mutex>
#include <stdexcept>
#include <google/protobuf/util/time_util.h>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include "uFilterPickerProxy/proxy.hpp"
#include "uFilterPickerProxy/proxyOptions.hpp"
#include "uFilterPickerProxy/database.hpp"
#include "uFilterPickerProxy/frontend.hpp"
#include "uFilterPickerProxy/frontendOptions.hpp"
#include "uFilterPickerProxy/backend.hpp"
#include "uFilterPickerProxy/backendOptions.hpp"
#include "uFilterPickerProxy/metricsSingleton.hpp"
#include "uFilterPickerProxyAPI/v1/pick.pb.h"

using namespace UFilterPickerProxy;

class Proxy::ProxyImpl
{
public:
    /// Stop the proxy
    void stop()
    {
        mKeepRunning.store(false);
        constexpr std::chrono::milliseconds pause{15};
        // Stop receiving picks first to give subscribers a chance
        if (mFrontend)
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Stopping frontend");
            mFrontend->stop();
            std::this_thread::sleep_for(pause);
        }
        if (mBackend)
        {
            SPDLOG_LOGGER_DEBUG(mLogger, "Stopping backend");
            mBackend->stop();
            std::this_thread::sleep_for(pause);
        }
        // Now shutdown the database

        mIsRunning.store(false);
    }

    void processPick()
    {
        while (mKeepRunning.load(std::memory_order_relaxed))
        {
            UFilterPickerProxyAPI::V1::Pick pick;
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

            }
            else
            {
                constexpr std::chrono::milliseconds timeOut{25};
                std::this_thread::sleep_for(timeOut);
            }
        }
    }

    /// @brief Defines the callback for getting/propagating a pick.
    void addPickCallback(UFilterPickerProxyAPI::V1::Pick &&pick)
    {
        // Check the pick
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
        // Enqueue the pick
        {
        const std::lock_guard lock{mMutex};
        while (mInputQueue.size() >= mMaximumInputQueueSize)
        {
            SPDLOG_LOGGER_WARN(mLogger, "Popping pick from queue");
            //mMetrics.incrementOverflowPicksCounter();
            mInputQueue.pop();
        }
        mInputQueue.push(std::move(pick));
        }
    }

    /// @result True indicates the proxy is still running
    [[nodiscard]] bool isRunning() const noexcept
    {
        return mIsRunning.load();
    }
private:
//private:
    ProxyOptions mOptions;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::mutex mMutex;
    std::function<void(UFilterPickerProxyAPI::V1::Pick &&)> 
        mAddPickCallbackFunction
    {    
        std::bind(&ProxyImpl::addPickCallback, this,
                  std::placeholders::_1)
    };   
    std::unique_ptr<Frontend> mFrontend{nullptr};
    std::unique_ptr<Backend> mBackend{nullptr};
    std::queue<UFilterPickerProxyAPI::V1::Pick> mInputQueue;
    size_t mMaximumInputQueueSize{4096};
    std::atomic<bool> mIsRunning{false};
    std::atomic<bool> mKeepRunning{true};
};

/// Constructor

/// Destructor
Proxy::~Proxy() = default;

/// Stop the proxy
void Proxy::stop()
{
    pImpl->stop();
}

/// Is the proxy running?
bool Proxy::isRunning() const noexcept
{
    return pImpl->isRunning();
}
