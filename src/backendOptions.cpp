#include <memory>
#include <utility>
#include "uFilterPickerProxy/backendOptions.hpp"
#include "uFilterPickerProxy/grpcServerOptions.hpp"

using namespace UFilterPickerProxy;

class BackendOptions::BackendOptionsImpl
{
public:
    GRPCServerOptions mGRPCOptions;
    bool mHaveGRPCOptions{false};
};

/// Constructor
BackendOptions::BackendOptions() :
    pImpl(std::make_unique<BackendOptionsImpl> ())
{
}

/// Copy constructor
BackendOptions::BackendOptions(const BackendOptions &options)
{
    *this = options;
}

/// Move constructor
BackendOptions::BackendOptions(BackendOptions &&options) noexcept
{
    *this = std::move(options);
}

/// Copy assignment
BackendOptions &BackendOptions::operator=(const BackendOptions &options)
{
    if (&options == this){return *this;}
    pImpl = std::make_unique<BackendOptionsImpl> (*options.pImpl);
    return *this;
}

/// Move assignment
BackendOptions &BackendOptions::operator=(BackendOptions &&options) noexcept
{
    if (&options == this){return *this;}
    pImpl = std::move(options.pImpl);
    return *this;
}

/// Destructor
BackendOptions::~BackendOptions() = default;
