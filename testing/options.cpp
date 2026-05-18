#include <chrono>
#include <cstdint>
#include <string>
#include <optional>
#include <catch2/catch_test_macros.hpp>
#include "uFilterPickerPickBroker/grpcServerOptions.hpp"
#include "uFilterPickerPickBroker/publishServiceOptions.hpp"
#include "uFilterPickerPickBroker/subscribeServiceOptions.hpp"
#include "uFilterPickerPickBroker/brokerOptions.hpp"
#include "uFilterPickerPickBroker/pickStoreOptions.hpp"

TEST_CASE("UFilterPickerPickBroker", "[grpcServerOptions]")
{
    SECTION("Defaults")
    {
        const UFilterPickerPickBroker::GRPCServerOptions options;
        REQUIRE(options.getHost() == "localhost");
        REQUIRE(options.getPort() == 50000);
        REQUIRE(options.getAccessToken() == std::nullopt);
        REQUIRE(options.getServerCertificate() == std::nullopt);
        REQUIRE(options.getServerKey() == std::nullopt);
        REQUIRE(options.getClientCertificate() == std::nullopt);
        REQUIRE(options.isReflectionEnabled() == false);
    }

    SECTION("Options")
    {
        const std::string host{"some.host.org"};
        const std::string token{"super-secret-token"};
        const std::string serverCertificate{"some-wonky-hash"};
        const std::string serverKey{"some-private-wonky-hash"};
        const std::string clientCertificate{"some-rad-hash"};
        constexpr uint16_t port{12345};
        UFilterPickerPickBroker::GRPCServerOptions options;

        options.setHost(host);
        options.setPort(port);
        options.setServerCertificate(serverCertificate);
        options.setServerKey(serverKey);
        options.setClientCertificate(clientCertificate);
        options.setAccessToken(token);
        options.enableReflection();

        const UFilterPickerPickBroker::GRPCServerOptions copy{options};

        REQUIRE(copy.getHost() == host);
        REQUIRE(copy.getPort() == port);
        REQUIRE(copy.getServerCertificate() != std::nullopt);
        REQUIRE(copy.getServerKey() != std::nullopt);
        REQUIRE(copy.getClientCertificate() != std::nullopt);
        REQUIRE(copy.getAccessToken() != std::nullopt);
        REQUIRE(*copy.getServerCertificate() == serverCertificate); //NOLINT
        REQUIRE(*copy.getServerKey() == serverKey); //NOLINT
        REQUIRE(*copy.getClientCertificate() == clientCertificate); //NOLINT
        REQUIRE(*copy.getAccessToken() == token); //NOLINT
        REQUIRE(copy.isReflectionEnabled() == true);
    }
}

TEST_CASE("UFilterPickerPickBroker", "[PublishServiceOptions]")
{
    SECTION("Defaults")
    {
        const UFilterPickerPickBroker::PublishServiceOptions options;
        REQUIRE(options.getMaximumMessageSizeInBytes() == std::nullopt);
        REQUIRE(options.getMaximumNumberOfPublishers() == 2048);
        REQUIRE(options.getMaximumConsecutiveInvalidMessages() == 8);
        REQUIRE(options.hasGRPCOptions() == false);
    }

    SECTION("Options")
    {
        const std::string host{"some.host.org"};
        constexpr uint16_t port{12345};
        constexpr int maxPublishers{383};
        constexpr int maxMessageSize{83393};
        constexpr uint32_t maxInvalidMessages{38};
        UFilterPickerPickBroker::GRPCServerOptions grpcOptions;

        grpcOptions.setHost(host);
        grpcOptions.setPort(port);

        UFilterPickerPickBroker::PublishServiceOptions options;
        REQUIRE_NOTHROW(options.setGRPCOptions(grpcOptions));
        REQUIRE_NOTHROW(options.setMaximumNumberOfPublishers(maxPublishers));
        REQUIRE_NOTHROW(options.setMaximumMessageSizeInBytes(maxMessageSize));
        options.setMaximumConsecutiveInvalidMessages(maxInvalidMessages);

        const UFilterPickerPickBroker::PublishServiceOptions copy{options};
        REQUIRE(copy.getGRPCOptions().getHost() == host);
        REQUIRE(copy.getGRPCOptions().getPort() == port);
        REQUIRE(copy.getMaximumNumberOfPublishers() == maxPublishers);
        REQUIRE(copy.getMaximumConsecutiveInvalidMessages() == maxInvalidMessages);
        REQUIRE(copy.getMaximumMessageSizeInBytes() != std::nullopt);
        REQUIRE(*copy.getMaximumMessageSizeInBytes() == maxMessageSize); //NOLINT
    }
}

TEST_CASE("UFilterPickerPickBroker", "[SubscribeServiceOptions]")
{
    SECTION("Defaults")
    {
        const UFilterPickerPickBroker::SubscribeServiceOptions options;
        REQUIRE(options.getMaximumNumberOfSubscribers() == 64);
        REQUIRE(options.getQueueCapacity() == 8192);
        REQUIRE(options.hasGRPCOptions() == false);
    }

    SECTION("Options")
    {
        const std::string host{"another.host.org"};
        constexpr uint16_t port{6432};
        constexpr int maxSubscribers{3833};
        constexpr int queueCapacity{910};
        UFilterPickerPickBroker::GRPCServerOptions grpcOptions;

        grpcOptions.setHost(host);
        grpcOptions.setPort(port);

        UFilterPickerPickBroker::SubscribeServiceOptions options;
        REQUIRE_NOTHROW(options.setGRPCOptions(grpcOptions));
        REQUIRE_NOTHROW(options.setMaximumNumberOfSubscribers(maxSubscribers));
        REQUIRE_NOTHROW(options.setQueueCapacity(queueCapacity));

        const UFilterPickerPickBroker::SubscribeServiceOptions copy{options};
        REQUIRE(copy.getGRPCOptions().getHost() == host);
        REQUIRE(copy.getGRPCOptions().getPort() == port);
        REQUIRE(copy.getMaximumNumberOfSubscribers() == maxSubscribers);
        REQUIRE(copy.getQueueCapacity() == queueCapacity);
    }
}

TEST_CASE("UFilterPickerPickBroker", "[BrokerOptions]")
{
    SECTION("Defaults")
    {
        const UFilterPickerPickBroker::BrokerOptions options;
        constexpr std::chrono::milliseconds retentionInterval
        {
            std::chrono::minutes {30}
        };
        REQUIRE(options.hasPublishServiceOptions() == false);
        REQUIRE(options.hasSubscribeServiceOptions() == false);
        REQUIRE(options.getQueueCapacity() == 8192);
        REQUIRE(options.getPickRetentionInterval() == retentionInterval);
    }

    SECTION("Options")
    {
        const std::string host{"this.host.org"};
        constexpr uint16_t publishPort{6432};
        constexpr uint16_t subscribePort{6433};
        constexpr int queueCapacity{832};
        constexpr std::chrono::milliseconds 
            retentionInterval{std::chrono::minutes {11}};
        UFilterPickerPickBroker::GRPCServerOptions grpcPublishOptions;
        grpcPublishOptions.setHost(host);
        grpcPublishOptions.setPort(publishPort);

        UFilterPickerPickBroker::GRPCServerOptions grpcSubscribeOptions;
        grpcSubscribeOptions.setHost(host);
        grpcSubscribeOptions.setPort(subscribePort);

        UFilterPickerPickBroker::PublishServiceOptions publishOptions;
        publishOptions.setGRPCOptions(grpcPublishOptions);

        UFilterPickerPickBroker::SubscribeServiceOptions subscribeOptions;
        subscribeOptions.setGRPCOptions(grpcSubscribeOptions);

        UFilterPickerPickBroker::BrokerOptions options;
        REQUIRE_NOTHROW(options.setPublishServiceOptions(publishOptions));
        REQUIRE_NOTHROW(options.setSubscribeServiceOptions(subscribeOptions));
        REQUIRE_NOTHROW(options.setQueueCapacity(queueCapacity));
        REQUIRE_NOTHROW(options.setPickRetentionInterval(retentionInterval));

        REQUIRE(options.getPublishServiceOptions().getGRPCOptions().getPort() == publishPort);
        REQUIRE(options.getSubscribeServiceOptions().getGRPCOptions().getPort() == subscribePort);
        REQUIRE(options.getQueueCapacity() == queueCapacity);
        REQUIRE(options.getPickRetentionInterval() == retentionInterval);
    }
}

TEST_CASE("UFilterPickerPickBroker", "[PickStoreOptions]")
{
    SECTION("Defaults")
    {
        const UFilterPickerPickBroker::PickStoreOptions options;
        REQUIRE(options.getMaximumQueueSize() == 2048);
    }
    SECTION("Options")
    {
        constexpr int maxQueueSize{732};
        UFilterPickerPickBroker::PickStoreOptions options;
        REQUIRE_THROWS(options.setMaximumQueueSize(0));
        REQUIRE_NOTHROW(options.setMaximumQueueSize(maxQueueSize));

        const UFilterPickerPickBroker::PickStoreOptions copy{options};
        REQUIRE(options.getMaximumQueueSize() == maxQueueSize);
    }
}
