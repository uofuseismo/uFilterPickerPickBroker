#include <atomic>
#include <csignal>
#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
//#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <spdlog/spdlog.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include "uFilterPickerPickBroker/broker.hpp"
#include "uFilterPickerPickBroker/brokerOptions.hpp"
#include "uFilterPickerPickBroker/metricsSingleton.hpp"
#include "programOptions.hpp"
#include "logger.hpp"
#include "metrics.hpp"

namespace
{

volatile std::sig_atomic_t mSignalStatus;
std::atomic_bool mInterrupted{false};

}


class Process
{
public:
    Process(ProgramOptions &&programOptions,
            std::shared_ptr<spdlog::logger> logger) :
        mOptions(std::move(programOptions)),
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
        // Need a database
        auto databaseOpenMode
        {
            UFilterPickerPickBroker::Database::Mode::ReadWrite
        };
        if (std::filesystem::exists(mOptions.sqlite3File))
        {
            if (mOptions.deleteDatabaseIfExists)
            {
                databaseOpenMode
                    = UFilterPickerPickBroker::Database::Mode::Create;
            }
        }
        auto database
            = std::make_unique<UFilterPickerPickBroker::Database> (
                mOptions.sqlite3File,
                databaseOpenMode,
                mLogger);
        // Now we make the broker
        mBroker
            = std::make_unique<UFilterPickerPickBroker::Broker> (
                mOptions.brokerOptions,
                std::move(database),
                mLogger);

    }

    void start()
    {
        if (mIsRunning)
        {
            SPDLOG_LOGGER_WARN(mLogger, "Process is already running");
            stop();
        }
        mIsRunning = true;
        mBroker->start();
        handleMainThread();
    }

    void stop()
    {
        if (mIsRunning)
        {
            mIsRunning = false;
        }
        if (mBroker){mBroker->stop();}
    }

    /// @brief Periodically prints a summary to a log file as another way
    ///        to demonstrate this application is alive and doing what it is
    ///        supposed to.
    void printSummary()
    {
        /// 
        if (mOptions.printSummaryInterval.count() <= 0){return;}
    }

    /// @brief Checks for errors on the threads.
    /// @result True indicates everything appears okay.
    [[nodiscard]] bool checkFuturesOkay(
        const std::chrono::milliseconds &waitForFuture)
    {
        bool isOkay{true};
        if (mBroker)
        {
            isOkay = mBroker->checkFuturesOkay(waitForFuture);
        }
        return isOkay;
    }

    /// @brief Handles main thread activities.
    void handleMainThread()
    {
        SPDLOG_LOGGER_INFO(mLogger, "Main thread entering waiting loop");
        stdCatchSignals();
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
        if (mStopRequested)
        {
            SPDLOG_LOGGER_DEBUG(mLogger,
                                "Stop request received.  Terminating...");
            stop();
            std::this_thread::sleep_for(std::chrono::milliseconds {15});
        }
    }

    void stdCatchSignals()
    {
        std::signal(SIGINT,  Process::stdSignalHandler);
        std::signal(SIGTERM, Process::stdSignalHandler);
    }

    static void stdSignalHandler(const int signal)
    {
        mSignalStatus = signal;
        mInterrupted.store(true);
    }

    ~Process()
    {
        stop();
    }
//private:
    ProgramOptions mOptions;
    std::shared_ptr<spdlog::logger> mLogger{nullptr};
    std::unique_ptr<UFilterPickerPickBroker::Broker> mBroker{nullptr};
    mutable std::mutex mStopMutex;
    std::condition_variable mStopCondition;
    bool mStopRequested{false};
    bool mIsRunning{false};
};

int main(int argc, char *argv[])
{
    // Get this out of the way
    UFilterPickerPickBroker::initializeMetricsSingleton();

    // Parse the command line arguments
    std::filesystem::path iniFile;
    try
    {
        auto [iniFileName, isHelp] = ::parseCommandLineOptions(argc, argv);
        if (isHelp){return EXIT_SUCCESS;}
        if (iniFileName.empty())
        {
            throw std::runtime_error("No initialization file specified");
        }
        iniFile = iniFileName;

    }
    catch (const std::exception &e)
    {
        //NOLINTNEXTLINE(misc-include-cleaner)
        auto logger = spdlog::stdout_color_st("console");
        SPDLOG_LOGGER_ERROR(logger,
                            "Failed to read command line options because {}",
                            std::string {e.what()});
        return EXIT_FAILURE;
    }

    // Read the program options from the ini file
    ::ProgramOptions programOptions;
    try 
    {   
        programOptions = ::parseIniFile(iniFile);
    }   
    catch (const std::exception &e) 
    {
        //NOLINTNEXTLINE(misc-include-cleaner
        auto consoleLogger = spdlog::stdout_color_st("console");
        SPDLOG_LOGGER_CRITICAL(consoleLogger,
                               "Failed to read program options because {}",
                               std::string {e.what()});
        return EXIT_FAILURE;
    }  

    // Create the logger
    std::shared_ptr<spdlog::logger> logger{nullptr};
    try 
    {   
        logger = UFilterPickerPickBroker::Logger::initialize(programOptions);
    }   
    catch (const std::exception &e) 
    {   
        //NOLINTNEXTLINE(misc-include-cleaner)
        auto consoleLogger = spdlog::stdout_color_st("console");
        SPDLOG_LOGGER_CRITICAL(consoleLogger,
                               "Failed to initialize logger because {}",
                               std::string {e.what()});
        return EXIT_FAILURE;
    }   

    // Initialize the metrics
    try
    {
        UFilterPickerPickBroker::Metrics::initialize(programOptions);
    }
    catch (const std::exception &e)
    {
        SPDLOG_LOGGER_CRITICAL(logger,
                               "Failed to initialize metrics because {}",
                               std::string {e.what()});
        UFilterPickerPickBroker::Logger::cleanup();
        return EXIT_FAILURE;
    }

    std::unique_ptr<::Process> process{nullptr};
    try
    {
        SPDLOG_LOGGER_INFO(logger, "Initializing main thread");
        process
            = std::make_unique<::Process> (std::move(programOptions), logger);
    }
    catch (const std::exception &e)
    {
        SPDLOG_LOGGER_CRITICAL(logger,
                               "Failed to initialize main process because {}",
                               std::string {e.what()});
        UFilterPickerPickBroker::Metrics::cleanup();
        UFilterPickerPickBroker::Logger::cleanup();
        return EXIT_FAILURE;
    }

    try
    {
        SPDLOG_LOGGER_INFO(logger, "Launching processes");
        process->start();
    }
    catch (const std::exception &e)
    {
        UFilterPickerPickBroker::Metrics::cleanup();
        UFilterPickerPickBroker::Logger::cleanup();
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

