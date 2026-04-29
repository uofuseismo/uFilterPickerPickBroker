#include <string>
#include "uFilterPickerProxy/exception.hpp"

using namespace UFilterPickerProxy;

AlreadyExists::AlreadyExists(const std::string &message) :
    mMessage(message)
{   
}

const char *AlreadyExists::what() const noexcept
{   
   return mMessage.c_str();
}   

