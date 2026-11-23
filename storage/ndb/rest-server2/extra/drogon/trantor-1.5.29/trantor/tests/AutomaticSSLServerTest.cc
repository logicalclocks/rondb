#include <trantor/net/TcpServer.h>
#include <trantor/utils/Logger.h>
#include <trantor/net/EventLoopThread.h>
#include <string>
#include <iostream>
using namespace trantor;
#define USE_IPV6 0

bool has_ssl(MsgBuffer *buffer)
{
    if (buffer->readableBytes() < 3)
        return false;
    const char *data = buffer->peek();
    unsigned char byte1 = static_cast<unsigned char>(data[0]);
    unsigned char byte2 = static_cast<unsigned char>(data[1]);
    unsigned char byte3 = static_cast<unsigned char>(data[2]);
    return (byte1 == 0x16) && (byte2 == 0x03) && (byte3 == 0x01);
}

int main()
{
    LOG_DEBUG << "test start";
    Logger::setLogLevel(Logger::kDebug);
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
            if (has_ssl(buffer))
            {
                LOG_DEBUG << "SSL data received";
                auto policy =
                    TLSPolicy::defaultServerPolicy("server.crt", "server.key");
                connectionPtr->startEncryption(policy, true);
                return;
            }
            LOG_DEBUG << std::string{buffer->peek(), buffer->readableBytes()};
            connectionPtr->send(*buffer);
            buffer->retrieveAll();
            connectionPtr->shutdown();
        });
    server.setConnectionCallback([](const TcpConnectionPtr &connPtr) {
        if (connPtr->connected())
        {
            LOG_DEBUG << "New connection";
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
