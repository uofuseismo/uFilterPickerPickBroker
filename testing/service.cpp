#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <filesystem>
#include <format>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <utility>
#include <grpcpp/grpcpp.h>
#include <google/protobuf/util/time_util.h>
#include <google/protobuf/duration.pb.h>
#include <grpcpp/channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/client_callback.h>
#include <grpcpp/support/status.h>
//NOLINTBEGIN(misc-include-cleaner)
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
//NOLINTEND(misc-include-cleaner)`
#include <catch2/catch_test_macros.hpp>
#include <uFilterPickerPickBrokerAPI/v1/subscribe_service.grpc.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/publish_service.grpc.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/stream_since_request.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/publish_response.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/phase_hint.pb.h>
#include "uFilterPickerPickBroker/database.hpp"
#include "uFilterPickerPickBroker/grpcServerOptions.hpp"
#include "uFilterPickerPickBroker/publishServiceOptions.hpp"
#include "uFilterPickerPickBroker/subscribeServiceOptions.hpp"
#include "uFilterPickerPickBroker/broker.hpp"
#include "uFilterPickerPickBroker/brokerOptions.hpp"
#include "uFilterPickerPickBroker/pickStoreOptions.hpp"
#include "uFilterPickerPickBroker/metricsSingleton.hpp"
#include "uFilterPickerPickBroker/version.hpp"
#include "utilities.hpp"

using namespace UFilterPickerPickBroker;

namespace
{

#define PUBLISH_SERVICE_BIND_HOST "0.0.0.0"
#define PUBLISH_SERVICE_HOST "localhost"
#define PUBLISH_SERVICE_PORT 58153

#define SUBSCRIBE_SERVICE_BIND_HOST "0.0.0.0"
#define SUBSCRIBE_SERVICE_HOST "localhost"
#define SUBSCRIBE_SERVICE_PORT 58154


void runBroker()
{
    auto metricsSingleton = &MetricsSingleton::getInstance();
    metricsSingleton->resetCounters();

    auto logger = spdlog::stdout_color_mt("broker-test"); // NOLINT
    const std::filesystem::path dbFile{"broker-test.sqlite3"};
    auto database = std::make_unique<Database> (dbFile, Database::Mode::Create, logger);

    GRPCServerOptions publishServiceGRPCOptions;
    publishServiceGRPCOptions.setHost(PUBLISH_SERVICE_BIND_HOST);
    publishServiceGRPCOptions.setPort(PUBLISH_SERVICE_PORT);
    PublishServiceOptions publishServiceOptions;
    publishServiceOptions.setGRPCOptions(publishServiceGRPCOptions);

    GRPCServerOptions subscribeServiceGRPCOptions;
    subscribeServiceGRPCOptions.setHost(SUBSCRIBE_SERVICE_BIND_HOST);
    subscribeServiceGRPCOptions.setPort(SUBSCRIBE_SERVICE_PORT);
    SubscribeServiceOptions subscribeServiceOptions;
    subscribeServiceOptions.setGRPCOptions(subscribeServiceGRPCOptions);

    BrokerOptions brokerOptions;
    constexpr std::chrono::minutes pickRetentionInterval{15};
    brokerOptions.setPickRetentionInterval(pickRetentionInterval);
    brokerOptions.setPublishServiceOptions(publishServiceOptions);
    brokerOptions.setSubscribeServiceOptions(subscribeServiceOptions);

    auto broker
        = std::make_unique<Broker> (brokerOptions,
                                    std::move(database),
                                    logger); 
    REQUIRE(broker != nullptr);
    REQUIRE(broker->isInitialized() == true);
    broker->start();  // Runs in different thread
    std::this_thread::sleep_for(std::chrono::seconds {3});
    broker->stop();

    std::filesystem::remove(dbFile);
}

// Create a consumer
void runPublisher()
{
    class Publisher final :
        public grpc::ClientWriteReactor<UFilterPickerPickBrokerAPI::V1::Pick>
    {
    public:
        Publisher(UFilterPickerPickBrokerAPI::V1::PublishService::Stub *stub,
                  const std::vector<UFilterPickerPickBrokerAPI::V1::Pick> &picksIn)
        {
            stub->async()->Publish(&mContext, &mSummary, this);
            auto picks = picksIn;
            std::sort(picks.begin(), picks.end(), 
                      [](const auto &lhs, const auto &rhs)
                      {
                          return lhs.time() < rhs.time();
                      }); 
            for (auto &p : picks)
            {
                mPicks.push(std::move(p));
            } 
            AddHold();
            nextWrite();
            StartCall();
        }

        void OnWriteDone(bool ok) override
        {
            if (ok)
            {
                std::this_thread::sleep_for(mWaitBetweenWrites);
                nextWrite();
            }
        }

        void OnDone(const grpc::Status &status) override
        {
            const std::unique_lock<std::mutex> lock(mMutex);
            mStatus = status;
            mDone = true;
            mConditionVariable.notify_one();
        }

        [[nodiscard]] grpc::Status await(UFilterPickerPickBrokerAPI::V1::PublishResponse *response)
        {
            std::unique_lock<std::mutex> lock(mMutex);
            mConditionVariable.wait(lock, [this] {return mDone;});
            *response = mSummary;
            return std::move(mStatus);
        }

        void nextWrite()
        {
            if (!mPicks.empty())
            {
                mPickToWrite = mPicks.front();
                mPicks.pop();
                StartWrite(&mPickToWrite);
            }
            else
            {
                StartWritesDone();
                RemoveHold();
            }
        }

        std::queue<UFilterPickerPickBrokerAPI::V1::Pick> mPicks;
        grpc::ClientContext mContext;
        UFilterPickerPickBrokerAPI::V1::Pick mPickToWrite;
        UFilterPickerPickBrokerAPI::V1::PublishResponse mSummary;
        std::mutex mMutex;
        std::condition_variable mConditionVariable;
        grpc::Status mStatus;
        std::chrono::milliseconds mWaitBetweenWrites{10};
        bool mDone{false};
    };

    // Let's create some picks to publish
    constexpr int nPicks{10};
    constexpr auto phaseHint
    {
        UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P
    };
    const auto id1 = ::createIdentifier("UU", "CWU", "HHZ", "01");
    const auto id2 = ::createIdentifier("UU", "CTU", "EHZ", "01");
    const auto algorithm
        = ::createAlgorithm("uFilterPicker-test", 
                            Version::getVersion(),
                            Version::getTag());

    auto now 
        = std::chrono::duration_cast<std::chrono::seconds>
          ((std::chrono::high_resolution_clock::now()).time_since_epoch());
    auto baseTime = now - std::chrono::seconds {nPicks + 1};
    std::vector<UFilterPickerPickBrokerAPI::V1::Pick> picks;
    for (int iPick = 0; iPick < nPicks; ++iPick)
    {
        auto pickTime = baseTime + std::chrono::seconds {iPick};
        auto pick = ::createPick(pickTime, 
                                 iPick%2 == 0 ? id1 : id2,
                                 algorithm,
                                 phaseHint);
        picks.push_back(std::move(pick));
    }

    // Create the publisher
    const auto address = std::string {PUBLISH_SERVICE_HOST} + ":"
                       + std::to_string(PUBLISH_SERVICE_PORT);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = UFilterPickerPickBrokerAPI::V1::PublishService::NewStub(channel);
    UFilterPickerPickBrokerAPI::V1::PublishResponse summary;
    Publisher publisher(stub.get(), picks);
    auto status = publisher.await(&summary);
    CHECK(status.ok() == true);
}

void runSubscriber()
{
    class Subscriber final :
        public grpc::ClientReadReactor<UFilterPickerPickBrokerAPI::V1::Pick>
    {
    public:
        Subscriber(UFilterPickerPickBrokerAPI::V1::SubscribeService::Stub *stub,
             std::vector<UFilterPickerPickBrokerAPI::V1::Pick> *receivedPicks) :
            mReceivedPicks(receivedPicks)
        {
            google::protobuf::Duration backfillDuration;
	    backfillDuration.set_seconds(0);
            backfillDuration.set_nanos(0);
            std::ostringstream oss;
            oss << std::this_thread::get_id();
            mRequest.set_identifier(oss.str());
            *mRequest.mutable_backfill_duration() = backfillDuration;
            stub->async()->StreamSince(&mContext, &mRequest, this);
            StartRead(&mPick);
            StartCall();    
        }
        void OnReadDone(bool ok) override
        {
            if (ok)
            {
                mReceivedPicks->push_back(mPick);
                StartRead(&mPick);
            } 
        }
        void OnDone(const grpc::Status &status) override
        {
            const std::unique_lock<std::mutex> lock(mMutex);
            mStatus = status;
            mDone = true;
            mConditionVariable.notify_one();
        }
        [[nodiscard]] grpc::Status await()
        {
            std::unique_lock<std::mutex> lock(mMutex);
            mConditionVariable.wait(lock, [this] {return mDone;});
            return std::move(mStatus);
        } 
        std::vector<UFilterPickerPickBrokerAPI::V1::Pick> 
            *mReceivedPicks{nullptr};
        grpc::ClientContext mContext;
        std::mutex mMutex;
        std::condition_variable mConditionVariable; 
        grpc::Status mStatus;
        UFilterPickerPickBrokerAPI::V1::StreamSinceRequest mRequest;
        UFilterPickerPickBrokerAPI::V1::Pick mPick;
        bool mDone{false};
    };

    auto address = std::string {SUBSCRIBE_SERVICE_HOST}
                 + ":"
                 + std::to_string(SUBSCRIBE_SERVICE_PORT);
    auto channel
        = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub
        = UFilterPickerPickBrokerAPI::V1::SubscribeService::NewStub(channel);
    std::vector<UFilterPickerPickBrokerAPI::V1::Pick> receivedPicks;
    Subscriber subscriber(stub.get(), &receivedPicks);
    auto status = subscriber.await();
    REQUIRE(status.ok());
}

}

TEST_CASE("UFilterPickerPickBroker", "Service")
{
    auto brokerThread = std::thread(&runBroker);
    std::this_thread::sleep_for(std::chrono::milliseconds {25});
//    auto subscriberThread1 = std::thread(&runSubscriber);
    std::this_thread::sleep_for(std::chrono::milliseconds {25});
    auto publisherThread1 = std::thread(&runPublisher);
     
    if (publisherThread1.joinable()){publisherThread1.join();}
//    if (subscriberThread1.joinable()){subscriberThread1.join();}
    if (brokerThread.joinable()){brokerThread.join();}
}
