#include <trantor/utils/Logger.h>

using namespace trantor;

int main()
{
    trantor::Logger::setLogLevel(trantor::Logger::kInfo);
    if (0)
        LOG_INFO << "dummy";
    else
        LOG_WARN << "it works";
}