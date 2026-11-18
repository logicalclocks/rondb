#include <trantor/net/EventLoopThread.h>
#include <trantor/net/TcpClient.h>
#include <trantor/net/TcpServer.h>
// Connector.h is not an installed header; include it by path. TcpClient
// exposes no way to invoke restart(), so the regression test below drives
// the connector directly.
#include "../net/inner/Connector.h"
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <cstring>
#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

using namespace trantor;
using namespace std::chrono_literals;

namespace
{
template <typename Pred>
bool waitFor(Pred pred, std::chrono::milliseconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred())
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::sleep_for(10ms);
    }
    return true;
}

void closeSocket(int sockfd)
{
#ifndef _WIN32
    ::close(sockfd);
#else
    closesocket(sockfd);
#endif
}

// Take an ephemeral port and hand it straight back. Connecting to the result
// is refused until a server binds it again. A socket left bound but not
// listening is not equivalent: some platforms drop the SYN instead of
// refusing it, so the attempt hangs rather than failing, and no retry is ever
// scheduled.
uint16_t reserveClosedPort()
{
    int fd = static_cast<int>(::socket(AF_INET, SOCK_STREAM, 0));
    sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = 0;
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
    ::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof addr);
    socklen_t len = sizeof addr;
    ::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len);
    uint16_t port = ntohs(addr.sin_port);
    closeSocket(fd);
    return port;
}

class TcpClientRetry : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        loopThread_.run();
        loop_ = loopThread_.getLoop();
        port_ = reserveClosedPort();

        client_ = std::make_shared<TcpClient>(loop_,
                                              InetAddress("127.0.0.1", port_),
                                              "retry-test-client");
        client_->enableRetry();
        client_->setConnectionCallback([this](const TcpConnectionPtr &conn) {
            if (conn->connected())
                ++clientConnects_;
        });
        client_->setConnectionErrorCallback([this]() { ++clientErrors_; });
        client_->setMessageCallback([](const TcpConnectionPtr &,
                                       MsgBuffer *buf) { buf->retrieveAll(); });
    }

    void TearDown() override
    {
        client_->stop();
        if (server_)
            server_->stop();
        std::promise<void> done;
        loop_->runInLoop([this, &done]() {
            client_.reset();
            done.set_value();
        });
        done.get_future().wait();
        std::this_thread::sleep_for(50ms);
    }

    void startServer()
    {
        server_ = std::make_shared<TcpServer>(loop_,
                                              InetAddress("127.0.0.1", port_),
                                              "retry-test-server");
        server_->setConnectionCallback([this](const TcpConnectionPtr &conn) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (conn->connected())
            {
                serverConn_ = conn;
                ++serverAccepts_;
            }
            else if (serverConn_ == conn)
            {
                // The socket is only closed once the last reference to the
                // connection is gone.
                serverConn_.reset();
            }
        });
        server_->setRecvMessageCallback(
            [](const TcpConnectionPtr &, MsgBuffer *buf) {
                buf->retrieveAll();
            });
        server_->start();
    }

    TcpConnectionPtr serverConn()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return serverConn_;
    }

    EventLoopThread loopThread_;
    EventLoop *loop_{nullptr};
    uint16_t port_{0};
    std::shared_ptr<TcpServer> server_;
    std::shared_ptr<TcpClient> client_;
    std::mutex mutex_;
    TcpConnectionPtr serverConn_;
    std::atomic<int> serverAccepts_{0};
    std::atomic<int> clientConnects_{0};
    std::atomic<int> clientErrors_{0};
};
}  // namespace

// A refused connection does not reliably reach the connection error callback:
// when connect(2) returns ECONNREFUSED synchronously, Connector::connect()
// schedules the retry without calling errorCallback_. POSIX lets a
// non-blocking connect(2) report a refusal either immediately or later, so the
// tests below observe connections rather than errors.

TEST_F(TcpClientRetry, RetriesFailedInitialConnection)
{
    client_->connect();
    // The port is bound but not listening, so the attempt is refused and a
    // retry is scheduled. Nothing can connect until the server listens.
    ASSERT_FALSE(waitFor([this]() { return clientConnects_ >= 1; }, 500ms));
    startServer();
    // Only a retry can establish this connection.
    EXPECT_TRUE(waitFor([this]() { return clientConnects_ >= 1; }, 5s));
}

TEST_F(TcpClientRetry, ReconnectsAfterConnectionIsLost)
{
    startServer();
    client_->connect();
    ASSERT_TRUE(waitFor([this]() { return serverAccepts_ == 1; }, 3s));
    serverConn()->forceClose();
    EXPECT_TRUE(waitFor([this]() { return serverAccepts_ >= 2; }, 5s));
}

TEST_F(TcpClientRetry, DisconnectDoesNotReconnect)
{
    startServer();
    client_->connect();
    ASSERT_TRUE(waitFor([this]() { return clientConnects_ == 1; }, 3s));
    client_->disconnect();
    // Longer than the initial retry delay (500 ms).
    std::this_thread::sleep_for(1500ms);
    EXPECT_EQ(serverAccepts_, 1);
}

TEST_F(TcpClientRetry, StopCancelsPendingRetry)
{
    client_->connect();
    // The first attempt is refused straight away and every failure schedules
    // another try, so by now a retry is pending.
    std::this_thread::sleep_for(1s);
    ASSERT_EQ(clientConnects_, 0);
    client_->stop();
    startServer();
    std::this_thread::sleep_for(2500ms);
    EXPECT_EQ(serverAccepts_, 0);
    EXPECT_EQ(clientConnects_, 0);
}

TEST_F(TcpClientRetry, RestartAfterStopDoesNotReconnect)
{
    startServer();

    auto connections = std::make_shared<std::atomic<int>>(0);
    auto connector =
        std::make_shared<Connector>(loop_,
                                    InetAddress("127.0.0.1", port_),
                                    true);
    connector->setNewConnectionCallback([connections](int sockfd) {
        ++(*connections);
        closeSocket(sockfd);
    });

    connector->start();
    ASSERT_TRUE(waitFor([&]() { return *connections == 1; }, 3s));

    // stop() clears the connection intent before restart() is queued, so a
    // restart must not connect again.
    connector->stop();
    loop_->queueInLoop([connector]() { connector->restart(); });

    std::this_thread::sleep_for(1s);
    EXPECT_EQ(*connections, 1);
    EXPECT_EQ(serverAccepts_, 1);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
