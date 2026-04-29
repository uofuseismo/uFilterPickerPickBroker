#include <string>
#include "uFilterPickerProxy/version.hpp"

using namespace UFilterPickerProxy;

int Version::getMajor() noexcept
{
    return uFilterPickerProxy_MAJOR;
}

int Version::getMinor() noexcept
{
    return uFilterPickerProxy_MINOR;
}

int Version::getPatch() noexcept
{
    return uFilterPickerProxy_PATCH;
}

//NOLINTBEGIN(bugprone-easily-swappable-parameters)
bool Version::isAtLeast(const int major, const int minor,
                        const int patch) noexcept
//NOLINTEND(bugprone-easily-swappable-parameters)
{
    if (uFilterPickerProxy_MAJOR < major){return false;}
    if (uFilterPickerProxy_MAJOR > major){return true;}
    if (uFilterPickerProxy_MINOR < minor){return false;}
    if (uFilterPickerProxy_MINOR > minor){return true;}
    if (uFilterPickerProxy_PATCH < patch){return false;}
    return true;
}

std::string Version::getVersion() noexcept
{
    std::string version{uFilterPickerProxy_VERSION};
    return version;
}

std::string Version::getTag() noexcept
{
    std::string tag{uFilterPickerProxy_GITTAG};
    return tag;
}

std::string Version::getVersionWithTag() noexcept
{
    auto tag = Version::getTag();
    if (tag.empty())
    {
        return Version::getVersion();
    }
    else
    {
        return Version::getVersion() + "-" + tag;
    }
}
