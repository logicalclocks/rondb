#include <trantor/net/TcpClient.h>
#include <trantor/utils/Logger.h>
#include <trantor/net/EventLoopThread.h>
#include <string>
#include <iostream>
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
    int connectionCount = 1;
    for (int i = 0; i < 1; ++i)
    {
        client[i] = std::make_shared<trantor::TcpClient>(&loop,
                                                         serverAddr,
                                                         "tcpclienttest");
        client[i]->setConnectionCallback(
            [i, &loop, &connectionCount](const TcpConnectionPtr &conn) {
                if (conn->connected())
                {
                }
                else
                {
                    LOG_DEBUG << i << " disconnected";
                    --connectionCount;
                    if (connectionCount == 0)
                        loop.quit();
                }
            });
        client[i]->setMessageCallback(
            [](const TcpConnectionPtr &conn, MsgBuffer *buf) {
                auto msg = std::string(buf->peek(), buf->readableBytes());

                LOG_INFO << msg;
                if (msg == "hello")
                {
                    buf->retrieveAll();
                    auto policy = TLSPolicy::defaultClientPolicy();
                    policy->setValidate(false);
                    policy->setAlpnProtocols(
                        {"client-preferred", "server-preferred"});
                    conn->startEncryption(
                        policy, false, [](const TcpConnectionPtr &connection) {
                            LOG_INFO << "SSL established";
                            if (connection->applicationProtocol() !=
                                "server-preferred")
                            {
                                LOG_ERROR << "Unexpected ALPN "
                                             "protocol: "
                                          << connection->applicationProtocol();
                            }
                        });
                    // STARTTLS users commonly send the next protocol command
                    // immediately. It must be buffered until the handshake is
                    // complete, not written as plaintext or dropped.
                    conn->send("EHLO mail.example\r\n");
                    return;
                }
                if (conn->isSSLConnection())
                {
                    if (msg != "EHLO mail.example\r\n")
                    {
                        LOG_ERROR << "Unexpected encrypted response: " << msg;
                    }
                    buf->retrieveAll();
                }
            });
        client[i]->connect();
    }
    loop.loop();
}
