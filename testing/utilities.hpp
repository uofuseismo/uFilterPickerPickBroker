#ifndef TESTING_UTILITIES_HPP
#define TESTING_UTILITIES_HPP
#include <chrono>
#include <vector>
#include <utility>
#include <google/protobuf/util/time_util.h>
#include <uFilterPickerPickBrokerAPI/v1/pick.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/phase_hint.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/stream_identifier.pb.h>
#include <uFilterPickerPickBrokerAPI/v1/algorithm.pb.h>

namespace
{

[[maybe_unused]] [[nodiscard]] 
UFilterPickerPickBrokerAPI::V1::Pick
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

[[maybe_unused]] [[nodiscard]] 
UFilterPickerPickBrokerAPI::V1::StreamIdentifier
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

[[maybe_unused]] [[nodiscard]] 
UFilterPickerPickBrokerAPI::V1::Algorithm
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

[[maybe_unused]] [[nodiscard]]
bool comparePicks(const UFilterPickerPickBrokerAPI::V1::Pick &lhs,
                  const UFilterPickerPickBrokerAPI::V1::Pick &rhs)
{
    if (lhs.stream_identifier().network() !=
        rhs.stream_identifier().network())
    {
        return false;
    }
    if (lhs.stream_identifier().station() !=
        rhs.stream_identifier().station())
    {
        return false;
    }
    if (lhs.stream_identifier().channel() !=
        rhs.stream_identifier().channel())
    {
        return false;
    }
    if (lhs.stream_identifier().location_code() !=
        rhs.stream_identifier().location_code())
    {
        return false;
    }
    if (google::protobuf::util::TimeUtil::TimestampToNanoseconds(lhs.time()) !=
        google::protobuf::util::TimeUtil::TimestampToNanoseconds(rhs.time()))
    {
        return false;
    }
    if (lhs.phase_hint() != rhs.phase_hint()){return false;}
    if (lhs.algorithm().name() != rhs.algorithm().name()){return false;}
    if (lhs.algorithm().version() != rhs.algorithm().version()){return false;}
    if (lhs.algorithm().tag() != rhs.algorithm().tag()){return false;}
    return true;
}

[[maybe_unused]] [[nodiscard]]
bool containsPick(const UFilterPickerPickBrokerAPI::V1::Pick &needle,
                  const std::vector<std::pair<std::chrono::nanoseconds,
                                               UFilterPickerPickBrokerAPI::V1::Pick>> &haystack)
{
    for (const auto &[t, p] : haystack)
    {
        if (::comparePicks(p, needle)){return true;}
    }
    return false;
}

}
#endif
