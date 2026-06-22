#ifndef PROGRAM_OPTIONS_HPP
#define PROGRAM_OPTIONS_HPP
#include <iostream>
#include <filesystem>
#include <stdexcept>
#include <sstream>
#include <string>
#include <utility>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ptree_fwd.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "uFilterPickerPickBroker/brokerOptions.hpp"
#include "uFilterPickerPickBroker/subscribeServiceOptions.hpp"
#include "uFilterPickerPickBroker/publishServiceOptions.hpp"
#include "uFilterPickerPickBroker/grpcServerOptions.hpp"
#include "uFilterPickerPickBroker/database.hpp"
#include "otelOptions.hpp"

namespace
{

#define APPLICATION_NAME "uFilterPickerPickBroker"

struct ProgramOptions
{
    UFilterPickerPickBroker::BrokerOptions brokerOptions;
    UFilterPickerPickBroker::OTelOptions::HTTPMetrics otelHTTPMetricsOptions;
    UFilterPickerPickBroker::OTelOptions::HTTPLog otelHTTPLogOptions;
    UFilterPickerPickBroker::OTelOptions::GRPCMetrics otelGRPCMetricsOptions;
    UFilterPickerPickBroker::OTelOptions::GRPCLog otelGRPCLogOptions;
    std::string applicationName{APPLICATION_NAME};
    std::filesystem::path sqlite3File{"uFilterPickerMessageStore.sqlite3"};
    std::chrono::seconds printSummaryInterval{std::chrono::minutes {15}};
    int verbosity{3};
    bool exportLogs{false};
    bool exportLogsWithHTTP{true};
    bool exportMetrics{false};
    bool exportMetricsWithHTTP{true};
    bool deleteDatabaseIfExists{false};
};

std::pair<std::filesystem::path, bool>
    parseCommandLineOptions(int argc, char *argv[])
{
    std::filesystem::path iniFile;
    boost::program_options::options_description desc(R"""(
The uFilterPickerPickDetector is pub/sub middleware.  It receives picks from the
uFilterPicker and forwards those picks to interested parties.

    uFilterPickerPickBroker --ini=broker.ini

Allowed options)""");
    desc.add_options()
        ("help", "Produces this help message")
        ("ini",  boost::program_options::value<std::string> (), 
                 "The initialization file for this executable");
    boost::program_options::variables_map vm; 
    //NOLINTBEGIN(misc-include-cleaner)
    auto parsedMap
        = boost::program_options::parse_command_line(argc, argv, desc);
    //NOLINTEND(misc-include-cleaner)
    boost::program_options::store(parsedMap, vm);
    boost::program_options::notify(vm);
    if (vm.count("help"))
    {   
        std::cout << desc << "\n";
        return {iniFile, true};
    }   
    if (vm.count("ini"))
    {   
        iniFile = vm["ini"].as<std::string> (); 
        if (!std::filesystem::exists(iniFile))
        {
            throw std::runtime_error("Initialization file: "
                                   + std::string {iniFile}
                                   + " does not exist");
        }
    }   
    return {iniFile, false};
}

[[nodiscard]] std::string
loadStringFromFile(const std::filesystem::path &path)
{
    std::string result;
    if (!std::filesystem::exists(path)){return result;}
    std::ifstream file(path);
    if (!file.is_open())
    {   
        throw std::runtime_error("Failed to open " + path.string());
    }   
    std::stringstream sstr;
    sstr << file.rdbuf();
    file.close(); 
    result = sstr.str();
    return result;
}

[[nodiscard]]
std::pair<std::chrono::milliseconds, std::chrono::milliseconds>
getOTelMetricsIntervalAndTimeOut(
    boost::property_tree::ptree &propertyTree,
    const std::string &section,
    const std::chrono::milliseconds &defaultExportInterval,
    const std::chrono::milliseconds &defaultExportTimeOut)
{
    int64_t exportInterval = defaultExportInterval.count();
    exportInterval
        = propertyTree.get<int64_t> (
            section + ".exportIntervalInMilliSeconds",
            exportInterval);
    if (exportInterval <= 0)
    {    
        throw std::runtime_error("Export interval must be positive");
    }    
    int64_t exportTimeOut = defaultExportTimeOut.count();
    exportTimeOut
        = propertyTree.get<int64_t> (
            section + ".exportTimeOutInMilliSeconds",
            exportTimeOut);
    if (exportTimeOut <= 0)
    {    
        throw std::invalid_argument("Export time out must be positive");
    }    
    return std::pair {std::chrono::milliseconds {exportInterval},
                      std::chrono::milliseconds {exportTimeOut}};
}


[[nodiscard]]
std::string getOTelCollectorURL(boost::property_tree::ptree &propertyTree,
                                const std::string &section)
{
    std::string result;
    const std::string otelCollectorHost
        = propertyTree.get<std::string> (section + ".host", "");
    const uint16_t otelCollectorPort
        = propertyTree.get<uint16_t> (section + ".port", 4218);
    if (!otelCollectorHost.empty())
    {     
        result = otelCollectorHost + ":"  
               + std::to_string(otelCollectorPort);
    }        
    return result;
}

[[nodiscard]] UFilterPickerPickBroker::GRPCServerOptions
    getGRPCServerOptions(
    const boost::property_tree::ptree &propertyTree,
    const std::string &section)
{
    UFilterPickerPickBroker::GRPCServerOptions options;

    auto host
        = propertyTree.get<std::string> (section + ".host",
                                         options.getHost());
    if (host.empty())
    {   
        throw std::runtime_error(section + ".host is empty");
    }   
    options.setHost(host);

    uint16_t port{50000};
    options.setPort(port);

    port = propertyTree.get<uint16_t> (section + ".port", options.getPort());
    options.setPort(port); 

    auto serverKey
        = propertyTree.get<std::string> (section + ".clientKey", "");
    auto serverCertificate
        = propertyTree.get<std::string> (section + ".serverCertificate", "");
    if (!serverKey.empty() && !serverCertificate.empty())
    { 
        if (!std::filesystem::exists(serverKey))
        {
            throw std::invalid_argument("gRPC server Key file "
                                      + serverKey
                                      + " does not exist");
        }
        if (!std::filesystem::exists(serverCertificate))
        {
            throw std::invalid_argument("gRPC server certificate file "
                                      + serverCertificate
                                      + " does not exist");
        }
        options.setServerKey(loadStringFromFile(serverKey));
        options.setServerCertificate(loadStringFromFile(serverCertificate));
    }

    auto accessToken
        = propertyTree.get_optional<std::string> (section + ".accessToken");
    if (accessToken)
    {   
        if (options.getServerCertificate() == std::nullopt)
        {
            throw std::invalid_argument(
                "Must set server certificate to use access token");
        }
        options.setAccessToken(*accessToken);
    }   

    auto clientCertificate
        = propertyTree.get<std::string> (section + ".clientCertificate", "");
    if (!clientCertificate.empty())
    {
        if (!std::filesystem::exists(clientCertificate))
        {
            throw std::invalid_argument("gRPC client certificate file "
                                      + clientCertificate
                                      + " does not exist");
        }
        options.setClientCertificate(loadStringFromFile(clientCertificate));
    }   
 
    auto enableReflection
        = propertyTree.get<bool> (section + ".enableReflection", false);
    options.disableReflection();
    if (enableReflection){options.enableReflection();}
 
    return options;
}

UFilterPickerPickBroker::PublishServiceOptions
   getPublishServiceOptions(const boost::property_tree::ptree &propertyTree)
{
    UFilterPickerPickBroker::PublishServiceOptions options;
    auto grpcOptions
        = ::getGRPCServerOptions(propertyTree, "PublishService");
    options.setGRPCOptions(grpcOptions);
    auto maxPublishers 
        = propertyTree.get<int>
             ("PublishService.maximumNumberOfPublishers",
              options.getMaximumNumberOfPublishers());
    options.setMaximumNumberOfPublishers(maxPublishers);
    auto maximumMessageSizeInBytes
        = propertyTree.get_optional<int>
             ("PublishService.maximumMessageSizeInBytes");
    if (maximumMessageSizeInBytes)
    {
        options.setMaximumMessageSizeInBytes(*maximumMessageSizeInBytes);
    }
    auto maximumConsecutiveInvalidMessages
        = propertyTree.get<int>
             ("PublishService.maximumNumberOfConsecutiveInvalidMessages",
              options.getMaximumConsecutiveInvalidMessages());
    options.setMaximumConsecutiveInvalidMessages(
        maximumConsecutiveInvalidMessages);
    return options;
}

UFilterPickerPickBroker::SubscribeServiceOptions
   getSubscribeServiceOptions(const boost::property_tree::ptree &propertyTree)
{
    UFilterPickerPickBroker::SubscribeServiceOptions options;
    auto grpcOptions
        = ::getGRPCServerOptions(propertyTree, "SubscribeService");
    options.setGRPCOptions(grpcOptions);

    auto maxSubscribers
        = propertyTree.get<int>
          ("SubscribeService.maximumNumberOfSubscribers",
           options.getMaximumNumberOfSubscribers());
    options.setMaximumNumberOfSubscribers(maxSubscribers);
    //void setQueueCapacity(int queueCapacity);

    return options;
}


UFilterPickerPickBroker::BrokerOptions
   getBrokerOptions(const boost::property_tree::ptree &propertyTree)
{
    UFilterPickerPickBroker::BrokerOptions options;
    options.setSubscribeServiceOptions(
        ::getSubscribeServiceOptions(propertyTree));
    options.setPublishServiceOptions(
        ::getPublishServiceOptions(propertyTree)); 
 
    auto queueCapacity
        = propertyTree.get<int> ("Broker.queueCapacity",
                                 options.getQueueCapacity());
    options.setQueueCapacity(queueCapacity);

    // Some checks before leaving
    if (!options.hasSubscribeServiceOptions())
    {   
        throw std::invalid_argument("Frontend options not set");
    }   
    if (!options.hasPublishServiceOptions())
    {   
        throw std::invalid_argument("Backend options not set");
    }   

   return options; 
}

ProgramOptions parseIniFile(const std::filesystem::path &iniFile)
{
    ProgramOptions options;
    if (!std::filesystem::exists(iniFile)){return options;}
    // Parse the initialization file
    boost::property_tree::ptree propertyTree;
    boost::property_tree::ini_parser::read_ini(iniFile, propertyTree);

    // Application name
    options.applicationName
        = propertyTree.get<std::string> ("General.applicationName",
                                         options.applicationName);
    if (options.applicationName.empty())
    {   
        options.applicationName = APPLICATION_NAME;
    }   
    options.verbosity
        = propertyTree.get<int> ("General.verbosity", options.verbosity);

    auto summaryIntervalInMinutes
        = static_cast<int> (options.printSummaryInterval.count());
    summaryIntervalInMinutes
        = propertyTree.get<int> ("General.printSummaryIntervalInMinutes",
                                 summaryIntervalInMinutes);
    options.printSummaryInterval 
        = std::chrono::minutes {summaryIntervalInMinutes};
                                 

    // Database
    auto sqlite3FileName = std::string {options.sqlite3File};
    sqlite3FileName
        = propertyTree.get<std::string> ("Database.sqlite3File",
                                         sqlite3FileName);
    options.sqlite3File = std::filesystem::path {sqlite3FileName};
    if (options.sqlite3File.empty())
    {
        throw std::invalid_argument("sqlite3 file is empty");
    }
    options.deleteDatabaseIfExists
        = propertyTree.get<bool> ("Database.deleteIfExists", 
                                  options.deleteDatabaseIfExists);

    // Logging
    options.exportLogs = false;
    if (propertyTree.get_optional<std::string> ("OTelHTTPLogOptions"))
    {
        UFilterPickerPickBroker::OTelOptions::HTTPLog logOptions;
        logOptions.url
            = ::getOTelCollectorURL(propertyTree, "OTelHTTPLogOptions");
        logOptions.suffix
            = propertyTree.get<std::string>
              ("OTelHTTPLogOptions.suffix", "/v1/logs");
        if (!logOptions.url.empty())
        {
            if (!logOptions.suffix.empty())
            {
                if (!logOptions.url.ends_with("/") &&
                    !logOptions.suffix.starts_with("/"))
                {   
                    logOptions.suffix = "/" + logOptions.suffix;
                }
            }
        }
        if (!logOptions.url.empty())
        {   
            options.exportLogs = true;
            options.exportLogsWithHTTP = true;
            options.otelHTTPLogOptions = logOptions;
        }
    }
    else if (propertyTree.get_optional<std::string> ("OTelGRPCLogOptions"))
    {
#ifndef WITH_OTLP_GRPC
        throw std::runtime_error(
            "Recompile with Conan to use gRPC logs exporter option");
#endif
        UFilterPickerPickBroker::OTelOptions::GRPCLog logOptions;
        logOptions.url
            = ::getOTelCollectorURL(propertyTree, "OTelGRPCLogOptions");
        auto certificatePath
            = propertyTree.get_optional<std::string>
              ("OTelGRPCLogOptions.certificate");
        if (certificatePath)
        {
            if (std::filesystem::exists(*certificatePath))
            {
                logOptions.certificatePath = *certificatePath;
            }
        }
        if (!logOptions.url.empty())
        {
            options.exportLogs = true;
            options.exportLogsWithHTTP = false;
            options.otelGRPCLogOptions = logOptions;
        }
    }

    // Metrics
    options.exportMetrics = false;
    if (propertyTree.get_optional<std::string> ("OTelHTTPMetricsOptions"))
    {
        UFilterPickerPickBroker::OTelOptions::HTTPMetrics metricsOptions;
        metricsOptions.url
            = ::getOTelCollectorURL(propertyTree, "OTelHTTPMetricsOptions");
        metricsOptions.suffix
            = propertyTree.get<std::string> ("OTelHTTPMetricsOptions.suffix",
                                             "/v1/metrics");
        if (!metricsOptions.url.empty())
        {
            if (!metricsOptions.suffix.empty())
            {
                if (!metricsOptions.url.ends_with("/") &&
                    !metricsOptions.suffix.starts_with("/"))
                {
                    metricsOptions.suffix = "/" + metricsOptions.suffix;
                }
            }
        }
        if (!metricsOptions.url.empty())
        {
            auto [exportInterval, exportTimeOut]
                = ::getOTelMetricsIntervalAndTimeOut(
                      propertyTree,
                      "OTelHTTPMetricsOptions",
                      metricsOptions.exportInterval,
                      metricsOptions.exportTimeOut);
            metricsOptions.exportInterval = exportInterval;
            metricsOptions.exportTimeOut = exportTimeOut;
            options.otelHTTPMetricsOptions = metricsOptions;
            options.exportMetrics = true;
            options.exportMetricsWithHTTP = true;
        }
    }
    else if (propertyTree.get_optional<std::string> ("OTelGRPCMetricsOptions"))
    {
#ifndef WITH_OTLP_GRPC
        throw std::runtime_error(
            "Recompile with Conan to use gRPC metrics exporter option");
#endif
        UFilterPickerPickBroker::OTelOptions::GRPCMetrics metricsOptions;
        metricsOptions.url
            = getOTelCollectorURL(propertyTree, "OTelGRPCMetricsOptions");
        auto [exportInterval, exportTimeOut]
            = ::getOTelMetricsIntervalAndTimeOut(
                  propertyTree,
                  "OTelGRPCMetricsOptions",
                  metricsOptions.exportInterval,
                  metricsOptions.exportTimeOut);
        metricsOptions.exportInterval = exportInterval;
        metricsOptions.exportTimeOut = exportTimeOut;
        auto certificatePath
            = propertyTree.get_optional<std::string>
              ("OTelGRPCMetricsOptions.certificate");
        if (certificatePath)
        {
            if (std::filesystem::exists(*certificatePath))
            {
                metricsOptions.certificatePath = *certificatePath;
            }
        }
        if (!metricsOptions.url.empty())
        {
            options.otelGRPCMetricsOptions = metricsOptions;
            options.exportMetrics = true;
            options.exportMetricsWithHTTP = false;
        }
    }

    // Broker options
    options.brokerOptions = ::getBrokerOptions(propertyTree);

    return options;
}

}
#endif
