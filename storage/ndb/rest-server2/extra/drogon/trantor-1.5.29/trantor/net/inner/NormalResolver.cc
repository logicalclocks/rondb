#include "NormalResolver.h"
#include <trantor/utils/Logger.h>
#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <strings.h>  // memset
#endif

using namespace trantor;

std::shared_ptr<Resolver> Resolver::newResolver(trantor::EventLoop *,
                                                size_t timeout)
{
    return std::make_shared<NormalResolver>(timeout);
}
bool Resolver::isCAresUsed()
{
    return false;
}
void NormalResolver::resolve(const std::string &hostname,
                             const Callback &callback)
{
    resolve(hostname,
            [callback](const std::vector<trantor::InetAddress> &addresses) {
                callback(addresses.empty() ? InetAddress{} : addresses.front());
            });
}

void NormalResolver::resolve(const std::string &hostname,
                             const ResolverResultsCallback &callback)
{
    {
        std::lock_guard<std::mutex> guard(globalMutex());
        auto iter = globalCache().find(hostname);
        if (iter != globalCache().end())
        {
            auto &cachedAddr = iter->second;
            if (timeout_ == 0 || cachedAddr.second.after(static_cast<double>(
                                     timeout_)) > trantor::Date::date())
            {
                callback(*cachedAddr.first);
                return;
            }
        }
    }

    concurrentTaskQueue().runTaskInQueue([thisPtr = shared_from_this(),
                                          callback,
                                          hostname]() {
        {
            std::lock_guard<std::mutex> guard(thisPtr->globalMutex());
            auto iter = thisPtr->globalCache().find(hostname);
            if (iter != thisPtr->globalCache().end())
            {
                auto &cachedAddr = iter->second;
                if (thisPtr->timeout_ == 0 ||
                    cachedAddr.second.after(static_cast<double>(
                        thisPtr->timeout_)) > trantor::Date::date())
                {
                    callback(*cachedAddr.first);
                    return;
                }
            }
        }
        struct addrinfo hints, *res = nullptr;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = PF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;
        auto error = getaddrinfo(hostname.data(), nullptr, &hints, &res);
        if (error != 0 || res == nullptr)
        {
            LOG_SYSERR << "InetAddress::resolve";
            if (res != nullptr)
            {
                freeaddrinfo(res);
            }
            callback({});
            return;
        }
        auto addresses = std::make_shared<std::vector<InetAddress>>();
        for (auto *item = res; item != nullptr; item = item->ai_next)
        {
            if (item->ai_family == AF_INET)
            {
                const auto *address =
                    reinterpret_cast<const struct sockaddr_in *>(item->ai_addr);
                addresses->emplace_back(*address);
            }
            else if (item->ai_family == AF_INET6)
            {
                const auto *address =
                    reinterpret_cast<const struct sockaddr_in6 *>(
                        item->ai_addr);
                addresses->emplace_back(*address);
            }
        }
        freeaddrinfo(res);
        callback(*addresses);
        {
            std::lock_guard<std::mutex> guard(thisPtr->globalMutex());
            auto &addrItem = thisPtr->globalCache()[hostname];
            addrItem.first = std::move(addresses);
            addrItem.second = trantor::Date::date();
        }
        return;
    });
}
