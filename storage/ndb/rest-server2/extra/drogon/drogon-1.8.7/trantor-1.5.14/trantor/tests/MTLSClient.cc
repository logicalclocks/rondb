/**
 *
 * # Generate CA file
 * openssl req -new -x509 -days 365 -keyout ca-key.pem -out ca-crt.pem
 *
 * # Generate Key File (ie: same for client and server, but you can create one
 * for each one) openssl genrsa -out server-key.pem 4096
 *
 * # Generate Server certificate:
 * openssl req -new -sha256 -key server-key.pem -out ca-csr.pem
 * openssl x509 -req -days 365 -in ca-csr.pem -CA ca-crt.pem -CAkey ca-key.pem
 * -CAcreateserial -out server-crt.pem openssl verify -CAfile ca-crt.pem
 * server-crt.pem
 *
 *
 * # For client (to specify a certificate client mode only - no domain):
 * # Create file client_cert_ext.cnf:
 * cat client_cert_ext.cnf
 *
 *   keyUsage = critical, digitalSignature, keyEncipherment
 *   extendedKeyUsage = clientAuth
 *   basicConstraints = critical, CA:FALSE
 *   authorityKeyIdentifier = keyid,issuer
 *   subjectAltName = DNS:Client
 *
 * Create client cert (using the same serve key and CA)
 * openssl x509 -req -in ca-csr.pem -days 1000 -CA ca-crt.pem -CAkey ca-key.pem
 * -set_serial 01 -extfile client_cert_ext.cnf  > client-crt.pem
 *
 * openssl verify -CAfile ca-crt.pem client-crt.pem
 * openssl x509 -in client-crt.pem -text -noout -purpose
 *
 * # Compile sample:
 *
 * g++ -o MTLSClient MTLSClient.cc -ltrantor -lssl -lcrypto -lpthread
 *
 * # Tests
 *
 * # Listen generic SSL server
 * openssl s_server -accept 8888  -CAfile ./ca-crt.pem  -cert ./server-crt.pem
 * -key ./server-key.pem  -state
 *
 * # Listen generic SSL server with mTLS verification
 * openssl s_server -accept 8888  -CAfile ./ca-crt.pem  -cert ./server-crt.pem
 * -key ./server-key.pem  -state -verify_return_error -Verify 1
 *
 * # Test the mTLS client bin
 * ./MTLSClient
 *
 * **/

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
    for (int i = 0; i < connCount; ++i)
    {
        client[i] = std::make_shared<trantor::TcpClient>(&loop,
                                                         serverAddr,
                                                         "tcpclienttest");
        std::vector<std::pair<std::string, std::string>> sslcmd = {};
        // That key is common for client and server
        // The CA file must be the client CA, for this sample the CA is common
        // for both
        auto policy = TLSPolicy::defaultClientPolicy();
        policy->setCertPath("./client-crt.pem")
            .setKeyPath("./server-key.pem")
            .setCaPath("./ca-crt.pem")
            .setHostname("localhost");
        client[i]->enableSSL(policy);
        client[i]->setConnectionCallback(
            [i, &loop, &connCount](const TcpConnectionPtr &conn) {
                if (conn->connected())
                {
                    LOG_DEBUG << i << " connected!";
                    char tmp[20];
                    sprintf(tmp, "%d client!!", i);
                    conn->send(tmp);
                }
                else
                {
                    LOG_DEBUG << i << " disconnected";
                    --connCount;
                    if (connCount == 0)
                        loop.quit();
                }
            });
        client[i]->setMessageCallback(
            [](const TcpConnectionPtr &conn, MsgBuffer *buf) {
                LOG_DEBUG << std::string(buf->peek(), buf->readableBytes());
                buf->retrieveAll();
                conn->shutdown();
            });
        client[i]->connect();
    }
    loop.loop();
}
