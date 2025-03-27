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
 * g++ -o MTLSServer MTLSServer.cc -ltrantor -lssl -lcrypto -lpthread
 *
 * # Tests
 *
 * # Should Fail
 * openssl s_client -connect 0.0.0.0:8888 -state
 *
 * # Should Connect (the CA file must be the server CA), for this sample the CA
 * is common for both openssl s_client -connect 0.0.0.0:8888 -key
 * ./server-key.pem -cert ./server-crt.pem -CAfile ./ca-crt.pem -state
 *
 * **/

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
    std::vector<std::pair<std::string, std::string>> sslcmd = {};

    // the CA file must be the client CA, for this sample the CA is common for
    // both
    auto policy =
        TLSPolicy::defaultServerPolicy("server-crt.pem", "server-key.pem");
    policy->setCaPath("ca-crt.pem")
        .setValidateChain(true)
        .setValidateDate(true)
        .setValidateDomain(false);  // client's don't have a domain name
    server.enableSSL(policy);
    server.setRecvMessageCallback(
        [](const TcpConnectionPtr &connectionPtr, MsgBuffer *buffer) {
            // LOG_DEBUG<<"recv callback!";
            std::cout << std::string(buffer->peek(), buffer->readableBytes());
            connectionPtr->send(buffer->peek(), buffer->readableBytes());
            buffer->retrieveAll();
            connectionPtr->forceClose();
        });
    server.setConnectionCallback([](const TcpConnectionPtr &connPtr) {
        if (connPtr->connected())
        {
            LOG_DEBUG << "New connection";
            connPtr->send("Hello world\r\n");
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
