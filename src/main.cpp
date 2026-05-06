#include <utility>
#include <memory>
#include <csignal>
#include <cstdlib>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <chrono>
#include <future>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include "uFilterPickerProxy/proxy.hpp"
#include "uFilterPickerProxy/proxyOptions.hpp"
#include "uFilterPickerProxy/metricsSingleton.hpp"

namespace
{

volatile std::sig_atomic_t mSignalStatus;
std::atomic_bool mInterrupted{false};

}


class Process
{
public:
    Process(std::shared_ptr<spdlog::logger> &&logger) :
        mLogger(std::move(logger))        
    {
        if (mLogger == nullptr)
        {
            //NOLINTBEGIN(misc-include-cleaner)
            auto classId
                = std::to_string (reinterpret_cast<std::uintptr_t> (this));
            mLogger = spdlog::stdout_color_mt("process-" + classId);
            //NOLINTEND(misc-include-cleaner)
        }
    }

    /// Start the process.
    void start()
    {
        if (mIsRunning)
        {
            SPDLOG_LOGGER_WARN(mLogger, "Process is already running");
            stop();
        }
    }

    /// Stops the process.
    void stop()
    {
        if (mIsRunning)
        {
            mIsRunning = false;
        }
        if (mProxy){mProxy->stop();}
    }

    /// Main thread can print a summary of processing to logs.
    void printSummary()
    {
    }

    /// @result True indicates that all futures are okay; false if any future threw an exception.
    [[nodiscard]] bool checkFuturesOkay(const std::chrono::milliseconds &waitForFuture)
    {
        return true;
    }

    /// Keep the main process busy.
    void handleMainThread()
    {
        while (!mStopRequested)
        {
            if (mInterrupted)
            {
                 SPDLOG_LOGGER_INFO(mLogger,
                                   "SIGINT/SIGTERM signal received!");
                mStopRequested = true;
                break;
            }
            constexpr std::chrono::milliseconds waitForFuture {5};
            if (!checkFuturesOkay(waitForFuture))
                {
                SPDLOG_LOGGER_CRITICAL(mLogger,
                   "Futures exception caught; terminating app");
                mStopRequested = true;
                break;
            }
            printSummary();
            std::unique_lock<std::mutex> lock(mStopMutex);
            constexpr std::chrono::milliseconds pause{100};
            mStopCondition.wait_for(lock, pause,
                                    [this]
                                    {
                                        return mStopRequested;
                                    });
        }
    }

    /// Set up signal handlers for SIGINT and SIGTERM.
    void stdCatchSignals()
    {
        std::signal(SIGINT,  Process::stdSignalHandler);
        std::signal(SIGTERM, Process::stdSignalHandler);
    }

    static void stdSignalHandler(const int signal)
    {
        mSignalStatus = signal;
        mInterrupted = true;
    }
    
    /// Destructor.
    ~Process()
    {
        stop();
    }
//private:
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::unique_ptr<UFilterPickerProxy::Proxy> mProxy{nullptr};
    mutable std::mutex mStopMutex;
    std::condition_variable mStopCondition;
    bool mStopRequested{false};
    bool mIsRunning{false};    
};

int main(int argc, char *argv[])
{
    UFilterPickerProxy::initializeMetricsSingleton();

    return EXIT_SUCCESS;
}
