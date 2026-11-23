#include <trantor/net/TcpClient.h>
#include <trantor/net/TcpServer.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <string>

using namespace trantor;

static std::string readFile(const std::string &path)
{
    std::ifstream file(path);
    return {std::istreambuf_iterator<char>(file), {}};
}

int main()
{
    constexpr char kHostname[] = "vhost.example.test";
    const auto certificatePem = readFile("server.crt");
    const auto privateKeyPem = readFile("server.key");
    if (certificatePem.empty() || privateKeyPem.empty())
    {
        std::cerr << "Failed to read the test certificate or key\n";
        return 1;
    }

    EventLoop loop;
    std::string selectedHostname;
    std::atomic<size_t> providerCalls{0};
    TcpServer server(&loop, InetAddress(0), "sync-certificate-provider");
    auto serverPolicy = std::make_shared<TLSPolicy>();
    serverPolicy->setServerCertificateProvider(
        [&selectedHostname,
         &providerCalls,
         certificatePem,
         privateKeyPem,
         &kHostname](std::string hostname) {
            ++providerCalls;
            selectedHostname = std::move(hostname);
            if (selectedHostname != kHostname)
                return ServerCertificate{certificatePem + privateKeyPem,
                                         std::string{}};
            return ServerCertificate{certificatePem, privateKeyPem};
        });
    server.enableSSL(serverPolicy);
    serverPolicy->setServerCertificateProvider(
        [](std::string) { return ServerCertificate{}; });
    server.start();

    std::atomic<bool> timedOut{false};
    std::atomic<bool> handshakeComplete{false};
    std::atomic<bool> serverSniComplete{false};
    std::atomic<bool> serverCertificateComplete{false};
    server.setConnectionCallback(
        [&loop,
         &handshakeComplete,
         &serverSniComplete,
         &serverCertificateComplete,
         &kHostname](const TcpConnectionPtr &connection) {
            if (connection->connected())
            {
                serverSniComplete = connection->sniName() == kHostname;
                serverCertificateComplete =
                    connection->localCertificate() != nullptr;
                if (handshakeComplete && serverSniComplete &&
                    serverCertificateComplete)
                    loop.quit();
            }
        });
    auto client =
        std::make_shared<TcpClient>(&loop,
                                    InetAddress("127.0.0.1",
                                                server.address().toPort()),
                                    "sync-certificate-provider-client");
    auto clientPolicy = TLSPolicy::defaultClientPolicy(kHostname);
    clientPolicy->setValidate(false);
    client->enableSSL(clientPolicy);
    client->setConnectionCallback(
        [&loop,
         &handshakeComplete,
         &serverSniComplete,
         &serverCertificateComplete](const TcpConnectionPtr &connection) {
            if (connection->connected())
            {
                handshakeComplete = true;
                if (serverSniComplete && serverCertificateComplete)
                    loop.quit();
            }
            else
                loop.quit();
        });
    loop.runAfter(3.0, [&loop, &timedOut]() {
        timedOut = true;
        loop.quit();
    });
    client->connect();
    loop.loop();
    client.reset();

    if (timedOut || !handshakeComplete || !serverSniComplete ||
        !serverCertificateComplete || selectedHostname != kHostname ||
        providerCalls != 1)
    {
        std::cerr << "Synchronous SNI certificate selection failed: timeout="
                  << timedOut << ", handshake=" << handshakeComplete
                  << ", server-sni=" << serverSniComplete
                  << ", server-certificate=" << serverCertificateComplete
                  << ", provider-calls=" << providerCalls
                  << ", hostname=" << selectedHostname << '\n';
        return 1;
    }

    std::atomic<bool> rejectionTimedOut{false};
    std::atomic<bool> rejectionReported{false};
    std::atomic<bool> rejectedConnectionEstablished{false};
    auto rejectedClient =
        std::make_shared<TcpClient>(&loop,
                                    InetAddress("127.0.0.1",
                                                server.address().toPort()),
                                    "rejected-certificate-provider-client");
    auto rejectedPolicy =
        TLSPolicy::defaultClientPolicy("rejected.example.test");
    rejectedPolicy->setValidate(false);
    rejectedClient->enableSSL(rejectedPolicy);
    rejectedClient->setSSLErrorCallback([&loop, &rejectionReported](SSLError) {
        rejectionReported = true;
        loop.quit();
    });
    rejectedClient->setConnectionCallback(
        [&loop,
         &rejectedConnectionEstablished](const TcpConnectionPtr &connection) {
            if (connection->connected())
            {
                rejectedConnectionEstablished = true;
                loop.quit();
            }
        });
    loop.runAfter(3.0, [&loop, &rejectionTimedOut]() {
        rejectionTimedOut = true;
        loop.quit();
    });
    rejectedClient->connect();
    loop.loop();
    rejectedClient.reset();
    server.stop();

    if (rejectionTimedOut || !rejectionReported ||
        rejectedConnectionEstablished || providerCalls != 2)
    {
        std::cerr << "Rejected SNI handling failed: timeout="
                  << rejectionTimedOut << ", error=" << rejectionReported
                  << ", connected=" << rejectedConnectionEstablished
                  << ", provider-calls=" << providerCalls << '\n';
        return 1;
    }
    return 0;
}
