#include <trantor/net/TcpServer.h>
#include <trantor/utils/Logger.h>
#include <trantor/net/EventLoopThread.h>
#include <string>
#include <iostream>
#include <thread>
#include <sys/stat.h>
#include <fcntl.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#endif

std::size_t fileCallback(const std::string &, int, char *, std::size_t);

using namespace trantor;
#define USE_IPV6 0
int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        std::cout << "usage:" << argv[0] << " filename" << std::endl;
        return 1;
    }
    std::cout << "filename:" << argv[1] << std::endl;
    struct stat filestat;
    if (stat(argv[1], &filestat) < 0)
    {
        perror("");
        exit(1);
    }
    std::cout << "file len=" << filestat.st_size << std::endl;

    auto fp = fopen(argv[1], "rb");

    if (fp == nullptr)
    {
        perror("");
        exit(1);
    }
    fclose(fp);

    LOG_DEBUG << "test start";

    Logger::setLogLevel(Logger::kTrace);
    EventLoopThread loopThread;
    loopThread.run();

#if USE_IPV6
    InetAddress addr(1207, true, true);
#else
    InetAddress addr(1207);
#endif
    TcpServer server(loopThread.getLoop(), addr, "test");
    server.setRecvMessageCallback(
        [](const TcpConnectionPtr &connectionPtr, MsgBuffer *buffer) {
            // LOG_DEBUG<<"recv callback!";
        });
    int counter = 0;
    server.setConnectionCallback([argv,
                                  &counter](const TcpConnectionPtr &connPtr) {
        if (connPtr->connected())
        {
            LOG_DEBUG << "New connection";
            std::thread t([connPtr, argv, &counter]() {
                for (int i = 0; i < 5; ++i)
                {
                    int fd;
#ifdef _WIN32
                    _sopen_s(
                        &fd, argv[1], _O_BINARY | _O_RDONLY, _SH_DENYNO, 0);
#else
                    fd = open(argv[1], O_RDONLY);
#endif
                    auto callback = std::bind(fileCallback,
                                              argv[1],
                                              fd,
                                              std::placeholders::_1,
                                              std::placeholders::_2);
                    connPtr->sendStream(callback);
                    ++counter;
                    std::string str =
                        "\n" + std::to_string(counter) + " streams sent!\n";
                    connPtr->send(std::move(str));
                }
            });
            t.detach();

            for (int i = 0; i < 3; ++i)
            {
                int fd;
#ifdef _WIN32
                _sopen_s(&fd, argv[1], _O_BINARY | _O_RDONLY, _SH_DENYNO, 0);
#else
                fd = open(argv[1], O_RDONLY);
#endif
                auto callback = std::bind(fileCallback,
                                          argv[1],
                                          fd,
                                          std::placeholders::_1,
                                          std::placeholders::_2);
                connPtr->sendStream(callback);
                ++counter;
                std::string str =
                    "\n" + std::to_string(counter) + " streams sent!\n";
                connPtr->send(std::move(str));
            }
        }
        else if (connPtr->disconnected())
        {
            LOG_DEBUG << "connection disconnected";
        }
    });
    server.setIoLoopNum(3);
    server.start();
    loopThread.wait();
    return 0;
}

std::size_t fileCallback(const std::string &strFile,
                         int nFd,
                         char *pBuffer,
                         std::size_t nBuffSize)
{
    if (nFd < 0)
        return 0;
    if (pBuffer == nullptr)
    {
        LOG_DEBUG << strFile.c_str() << " closed.";
#ifdef _WIN32
        _close(nFd);
#else
        close(nFd);
#endif
        return 0;
    }
#ifdef _WIN32
    int nRead = _read(nFd, pBuffer, (unsigned int)nBuffSize);
#else
    ssize_t nRead = read(nFd, pBuffer, nBuffSize);
#endif
    if (nRead < 0)
        return 0;
    return std::size_t(nRead);
}
