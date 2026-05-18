#ifndef TESTING_UTILITIES_HPP
#define TESTING_UTILITIES_HPP
#include <uFilterPickerPickBrokerAPI/v1/pick.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/phase_hint.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/stream_identifier.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/algorithm.pb.h>


namespace
{

[[nodiscard]] UFilterPickerPickBrokerAPI::V1::Pick
    createPick(const std::chrono::seconds &pickTime,
       const UFilterPickerPickBrokerAPI::V1::StreamIdentifier &identifier,
       const UFilterPickerPickBrokerAPI::V1::Algorithm &algorithm,
       const auto phaseHint = UFilterPickerPickBrokerAPI::V1::PhaseHint::PHASE_HINT_P)
{
    UFilterPickerPickBrokerAPI::V1::Pick pick;
    *pick.mutable_stream_identifier() = identifier;
    *pick.mutable_algorithm() = algorithm;
    *pick.mutable_time()
        = google::protobuf::util::TimeUtil::SecondsToTimestamp(
             pickTime.count());
    pick.set_phase_hint(phaseHint);
    return pick;
}

[[nodiscard]] UFilterPickerPickBrokerAPI::V1::StreamIdentifier
    createIdentifier(const std::string &network,
                     const std::string &station,
                     const std::string &channel,
                     const std::string &locationCode)
{
    UFilterPickerPickBrokerAPI::V1::StreamIdentifier identifier;
    identifier.set_network(network);
    identifier.set_station(station);
    identifier.set_channel(channel);
    identifier.set_location_code(locationCode);
    return identifier;
}

[[nodiscard]] UFilterPickerPickBrokerAPI::V1::Algorithm
    createAlgorithm(const std::string &algorithmName,
                    const std::string &algorithmVersion,
                    const std::string &algorithmTag)
{
    UFilterPickerPickBrokerAPI::V1::Algorithm algorithm;
    algorithm.set_name(algorithmName);
    algorithm.set_version(algorithmVersion);
    algorithm.set_tag(algorithmTag);
    return algorithm;
}

}
#endif
