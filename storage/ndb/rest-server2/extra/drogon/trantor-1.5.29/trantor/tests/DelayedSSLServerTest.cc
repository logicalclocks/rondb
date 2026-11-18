#include <trantor/net/TcpServer.h>
#include <trantor/utils/Logger.h>
#include <trantor/net/EventLoopThread.h>
#include <string>
#include <iostream>
using namespace trantor;
#define USE_IPV6 0
int main()
{
    LOG_DEBUG << "test start";
    Logger::setLogLevel(Logger::kTrace);
    EventLoopThread loopThread;
    loopThread.run();
#if USE_IPV6
    InetAddress addr(8888, true, true);
#else
    InetAddress addr(8888);
#endif
    TcpServer server(loopThread.getLoop(), addr, "test");
    // auto ctx = newSSLServerContext("server.pem", "server.pem", {});
    LOG_INFO << "start";
    server.setRecvMessageCallback(
        [](const TcpConnectionPtr &connectionPtr, MsgBuffer *buffer) {
            const std::string message{buffer->peek(), buffer->readableBytes()};
            LOG_DEBUG << message;
            if (message != "EHLO mail.example\r\n")
            {
                LOG_ERROR << "Unexpected encrypted request: " << message;
            }
            connectionPtr->send(*buffer);
            buffer->retrieveAll();
            connectionPtr->shutdown();
        });
    server.setConnectionCallback([](const TcpConnectionPtr &connPtr) {
        if (connPtr->connected() && !connPtr->isSSLConnection())
        {
            LOG_DEBUG << "New connection";
            connPtr->send("hello");
            auto policy =
                TLSPolicy::defaultServerPolicy("server.crt", "server.key");
            policy->setAlpnProtocols({"server-preferred", "client-preferred"});
            connPtr->startEncryption(
                policy, true, [](const TcpConnectionPtr &connection) {
                    if (connection->applicationProtocol() != "server-preferred")
                    {
                        LOG_ERROR << "Unexpected ALPN protocol: "
                                  << connection->applicationProtocol();
                    }
                });
        }
        else if (connPtr->disconnected())
        {
            LOG_DEBUG << "connection disconnected";
        }
    });
    server.setIoLoopNum(3);
    server.start();
    loopThread.wait();
}
