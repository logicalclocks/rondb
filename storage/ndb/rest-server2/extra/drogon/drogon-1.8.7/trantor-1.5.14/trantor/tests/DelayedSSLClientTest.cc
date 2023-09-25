#include <trantor/net/TcpClient.h>
#include <trantor/utils/Logger.h>
#include <trantor/net/EventLoopThread.h>
#include <string>
#include <iostream>
#include <atomic>
using namespace trantor;
#define USE_IPV6 0
int main()
{
    trantor::Logger::setLogLevel(trantor::Logger::kTrace);
    LOG_DEBUG << "TcpClient class test!";
    EventLoop loop;
#if USE_IPV6
    InetAddress serverAddr("::1", 8888, true);
#else
    InetAddress serverAddr("127.0.0.1", 8888);
#endif
    std::shared_ptr<trantor::TcpClient> client[10];
    std::atomic_int connCount;
    connCount = 1;
    for (int i = 0; i < 1; ++i)
    {
        client[i] = std::make_shared<trantor::TcpClient>(&loop,
                                                         serverAddr,
                                                         "tcpclienttest");
        client[i]->setConnectionCallback(
            [i, &loop, &connCount](const TcpConnectionPtr &conn) {
                if (conn->connected())
                {
                }
                else
                {
                    LOG_DEBUG << i << " disconnected";
                    --connCount;
                    if (connCount == 0)
                        loop.quit();
                }
            });
        client[i]->setMessageCallback([](const TcpConnectionPtr &conn,
                                         MsgBuffer *buf) {
            auto msg = std::string(buf->peek(), buf->readableBytes());

            LOG_INFO << msg;
            if (msg == "hello")
            {
                buf->retrieveAll();
                auto policy = TLSPolicy::defaultClientPolicy();
                policy->setValidate(false);
                conn->startEncryption(
                    policy, false, [](const TcpConnectionPtr &encryptedConn) {
                        LOG_INFO << "SSL established";
                        encryptedConn->send("Hello");
                    });
            }
            if (conn->isSSLConnection())
            {
                buf->retrieveAll();
            }
        });
        client[i]->connect();
    }
    loop.loop();
}
