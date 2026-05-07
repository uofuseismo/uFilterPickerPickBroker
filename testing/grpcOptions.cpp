#include <cstddef>
#include <cstdint>
#include <string>
#include <optional>
#include <catch2/catch_test_macros.hpp>
#include "uFilterPickerProxy/grpcServerOptions.hpp"
#include "uFilterPickerProxy/frontendOptions.hpp"
#include "uFilterPickerProxy/backendOptions.hpp"

TEST_CASE("UFilterPickerProxy", "[grpcServerOptions]")
{
    SECTION("Defaults")
    {   
        const UFilterPickerProxy::GRPCServerOptions options;
        REQUIRE(options.getHost() == "localhost");
        REQUIRE(options.getPort() == 50000);
        REQUIRE(options.getAccessToken() == std::nullopt);
        REQUIRE(options.getServerCertificate() == std::nullopt);
        REQUIRE(options.getServerKey() == std::nullopt);
        REQUIRE(options.isReflectionEnabled() == false);
    } 

    SECTION("Options")
    {   
        const std::string host{"some.host.org"};
        const std::string token{"super-secret-token"};
        const std::string serverCertificate{"some-wonky-hash"};
        const std::string serverKey{"some-private-wonky-hash"};
        constexpr uint16_t port{12345};
        UFilterPickerProxy::GRPCServerOptions options;

        options.setHost(host);
        options.setPort(port);
        options.setServerCertificate(serverCertificate);
        options.setServerKey(serverKey);
        options.setAccessToken(token);
        options.enableReflection();

        const UFilterPickerProxy::GRPCServerOptions copy{options};
        
        REQUIRE(copy.getHost() == host);
        REQUIRE(copy.getPort() == port);
        REQUIRE(copy.getServerCertificate() != std::nullopt);
        REQUIRE(copy.getServerKey() != std::nullopt);
        REQUIRE(copy.getAccessToken() != std::nullopt);
        REQUIRE(*copy.getServerCertificate() == serverCertificate); //NOLINT
        REQUIRE(*copy.getServerKey() == serverKey); //NOLINT
        REQUIRE(*copy.getAccessToken() == token); //NOLINT
        REQUIRE(copy.isReflectionEnabled() == true); 
    }   
}

TEST_CASE("UFilterPickerProxy", "[FrontendOptions]")
{
    SECTION("Defaults")
    {
        const UFilterPickerProxy::FrontendOptions options;
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
        UFilterPickerProxy::GRPCServerOptions grpcOptions;

        grpcOptions.setHost(host);
        grpcOptions.setPort(port);

        UFilterPickerProxy::FrontendOptions options;
        REQUIRE_NOTHROW(options.setGRPCOptions(grpcOptions));
        REQUIRE_NOTHROW(options.setMaximumNumberOfPublishers(maxPublishers));
        REQUIRE_NOTHROW(options.setMaximumMessageSizeInBytes(maxMessageSize));
        options.setMaximumConsecutiveInvalidMessages(maxInvalidMessages);
 
        const UFilterPickerProxy::FrontendOptions copy{options};
        REQUIRE(copy.getGRPCOptions().getHost() == host);
        REQUIRE(copy.getGRPCOptions().getPort() == port);
        REQUIRE(copy.getMaximumNumberOfPublishers() == maxPublishers);
        REQUIRE(copy.getMaximumConsecutiveInvalidMessages() == maxInvalidMessages);
        REQUIRE(copy.getMaximumMessageSizeInBytes() != std::nullopt);
        REQUIRE(*copy.getMaximumMessageSizeInBytes() == maxMessageSize); //NOLINT
    }
}

TEST_CASE("UFilterPickerProxy", "[BackendOptions]")
{
    SECTION("BEDefaults")
    {
        const UFilterPickerProxy::BackendOptions options;
        REQUIRE(options.getMaximumNumberOfSubscribers() == 64);
        REQUIRE(options.hasGRPCOptions() == false);
    }

    SECTION("BEOptions")
    {
        const std::string host{"another.host.org"};
        constexpr uint16_t port{6432};
        constexpr int maxSubscribers{3833};
        UFilterPickerProxy::GRPCServerOptions grpcOptions;

        grpcOptions.setHost(host);
        grpcOptions.setPort(port);

        UFilterPickerProxy::BackendOptions options;
        REQUIRE_NOTHROW(options.setGRPCOptions(grpcOptions));
        REQUIRE_NOTHROW(options.setMaximumNumberOfSubscribers(maxSubscribers));

        const UFilterPickerProxy::BackendOptions copy{options};
        REQUIRE(copy.getGRPCOptions().getHost() == host);
        REQUIRE(copy.getGRPCOptions().getPort() == port);
        REQUIRE(copy.getMaximumNumberOfSubscribers() == maxSubscribers);
    }
}