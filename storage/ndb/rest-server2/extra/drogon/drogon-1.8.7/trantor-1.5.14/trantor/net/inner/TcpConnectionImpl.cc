/**
 *
 *  @file TcpConnectionImpl.cc
 *  @author An Tao
 *
 *  Public header file in trantor lib.
 *
 *  Copyright 2018, An Tao.  All rights reserved.
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the License file.
 *
 *
 */

#include "TcpConnectionImpl.h"
#include "Socket.h"
#include "Channel.h"
#include <trantor/utils/Utilities.h>
#ifdef __linux__
#include <sys/sendfile.h>
#include <poll.h>
#endif
#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <WinSock2.h>
#include <Windows.h>
#include <wincrypt.h>
#endif
#include <sys/stat.h>
#include <fcntl.h>

using namespace trantor;

#ifdef _WIN32
// Winsock does not set errno, and WSAGetLastError() has different values than
// errno socket errors
#undef EWOULDBLOCK
#define EWOULDBLOCK WSAEWOULDBLOCK
#undef EPIPE
#define EPIPE WSAENOTCONN
#undef ECONNRESET
#define ECONNRESET WSAECONNRESET
#endif

static const int kMaxSendFileBufferSize = 16 * 1024;
TcpConnectionImpl::TcpConnectionImpl(EventLoop *loop,
                                     int socketfd,
                                     const InetAddress &localAddr,
                                     const InetAddress &peerAddr,
                                     TLSPolicyPtr policy,
                                     SSLContextPtr ctx)
    : loop_(loop),
      ioChannelPtr_(new Channel(loop, socketfd)),
      socketPtr_(new Socket(socketfd)),
      localAddr_(localAddr),
      peerAddr_(peerAddr)
{
    LOG_TRACE << "new connection:" << peerAddr.toIpPort() << "->"
              << localAddr.toIpPort();
    ioChannelPtr_->setReadCallback(
        std::bind(&TcpConnectionImpl::readCallback, this));
    ioChannelPtr_->setWriteCallback(
        std::bind(&TcpConnectionImpl::writeCallback, this));
    ioChannelPtr_->setCloseCallback(
        std::bind(&TcpConnectionImpl::handleClose, this));
    ioChannelPtr_->setErrorCallback(
        std::bind(&TcpConnectionImpl::handleError, this));
    socketPtr_->setKeepAlive(true);
    name_ = localAddr.toIpPort() + "--" + peerAddr.toIpPort();

    if (policy != nullptr)
    {
        tlsProviderPtr_ = newTLSProvider(this, policy, ctx);
        tlsProviderPtr_->setWriteCallback(onSslWrite);
        tlsProviderPtr_->setErrorCallback(onSslError);
        tlsProviderPtr_->setHandshakeCallback(onHandshakeFinished);
        tlsProviderPtr_->setMessageCallback(onSslMessage);
        // This is triggered when peer sends a close alert
        tlsProviderPtr_->setCloseCallback(onSslCloseAlert);
    }
}
TcpConnectionImpl::~TcpConnectionImpl()
{
    // send a close alert to peer if we are still connected
    if (tlsProviderPtr_ && status_ == ConnStatus::Connected)
        tlsProviderPtr_->close();
}

void TcpConnectionImpl::readCallback()
{
    // LOG_TRACE<<"read Callback";
    loop_->assertInLoopThread();
    int ret = 0;

    ssize_t n = readBuffer_.readFd(socketPtr_->fd(), &ret);
    // LOG_TRACE<<"read "<<n<<" bytes from socket";
    if (n == 0)
    {
        // socket closed by peer
        handleClose();
    }
    else if (n < 0)
    {
        if (errno == EPIPE || errno == ECONNRESET)
        {
#ifdef _WIN32
            LOG_TRACE << "WSAENOTCONN or WSAECONNRESET, errno=" << errno
                      << " fd=" << socketPtr_->fd();
#else
            LOG_TRACE << "EPIPE or ECONNRESET, errno=" << errno
                      << " fd=" << socketPtr_->fd();
#endif
            return;
        }
#ifdef _WIN32
        if (errno == WSAECONNABORTED)
        {
            LOG_TRACE << "WSAECONNABORTED, errno=" << errno;
            handleClose();
            return;
        }
#else
        if (errno == EAGAIN)  // TODO: any others?
        {
            LOG_TRACE << "EAGAIN, errno=" << errno
                      << " fd=" << socketPtr_->fd();
            return;
        }
#endif
        LOG_SYSERR << "read socket error";
        handleClose();
        return;
    }
    extendLife();
    if (n > 0)
    {
        bytesReceived_ += n;
        if (tlsProviderPtr_)
        {
            tlsProviderPtr_->recvData(&readBuffer_);
        }
        else if (recvMsgCallback_)
        {
            recvMsgCallback_(shared_from_this(), &readBuffer_);
        }
    }
}
void TcpConnectionImpl::extendLife()
{
    if (idleTimeout_ > 0)
    {
        auto now = Date::date();
        if (now < lastTimingWheelUpdateTime_.after(1.0))
            return;
        lastTimingWheelUpdateTime_ = now;
        auto entry = kickoffEntry_.lock();
        if (entry)
        {
            auto timingWheelPtr = timingWheelWeakPtr_.lock();
            if (timingWheelPtr)
                timingWheelPtr->insertEntry(idleTimeout_, entry);
        }
    }
}
void TcpConnectionImpl::writeCallback()
{
    loop_->assertInLoopThread();
    extendLife();
    if (ioChannelPtr_->isWriting())
    {
        if (tlsProviderPtr_)
        {
            bool sentAll = tlsProviderPtr_->sendBufferedData();
            if (!sentAll)
            {
                ioChannelPtr_->enableWriting();
                return;
            }
        }
        assert(!writeBufferList_.empty());
        auto writeBuffer_ = writeBufferList_.front();
        if (!writeBuffer_->isFile())
        {
            // not a file
            if (writeBuffer_->msgBuffer_->readableBytes() <= 0)
            {
                // finished sending
                writeBufferList_.pop_front();
                if (writeBufferList_.empty())
                {
                    // stop writing
                    ioChannelPtr_->disableWriting();
                    if (writeCompleteCallback_)
                        writeCompleteCallback_(shared_from_this());
                    if (status_ == ConnStatus::Disconnecting)
                    {
                        socketPtr_->closeWrite();
                    }
                }
                else
                {
                    // send next
                    // what if the next is not a file???
                    auto fileNode = writeBufferList_.front();
                    assert(fileNode->isFile());
                    sendFileInLoop(fileNode);
                }
            }
            else
            {
                // continue sending
                auto n = writeInLoop(writeBuffer_->msgBuffer_->peek(),
                                     writeBuffer_->msgBuffer_->readableBytes());
                if (n >= 0)
                {
                    writeBuffer_->msgBuffer_->retrieve(n);
                }
                else
                {
#ifdef _WIN32
                    if (errno != 0 && errno != EWOULDBLOCK)
#else
                    if (errno != EWOULDBLOCK)
#endif
                    {
                        // TODO: any others?
                        if (errno == EPIPE || errno == ECONNRESET)
                        {
#ifdef _WIN32
                            LOG_TRACE << "WSAENOTCONN or WSAECONNRESET, errno="
                                      << errno;
#else
                            LOG_TRACE << "EPIPE or ECONNRESET, errno=" << errno;
#endif
                            return;
                        }
                        LOG_SYSERR << "Unexpected error(" << errno << ")";
                        return;
                    }
                }
            }
        }
        else
        {
            // is a file
            if (writeBuffer_->fileBytesToSend_ <= 0)
            {
                // finished sending
                writeBufferList_.pop_front();
                if (writeBufferList_.empty())
                {
                    // stop writing
                    ioChannelPtr_->disableWriting();
                    if (writeCompleteCallback_)
                        writeCompleteCallback_(shared_from_this());
                    if (status_ == ConnStatus::Disconnecting)
                    {
                        socketPtr_->closeWrite();
                    }
                }
                else
                {
                    // next is not a file
                    if (!writeBufferList_.front()->isFile())
                    {
                        // There is data to be sent in the buffer.
                        auto n = writeInLoop(
                            writeBufferList_.front()->msgBuffer_->peek(),
                            writeBufferList_.front()
                                ->msgBuffer_->readableBytes());
                        if (n >= 0)
                        {
                            writeBufferList_.front()->msgBuffer_->retrieve(n);
                        }
                        else
                        {
#ifdef _WIN32
                            if (errno != 0 && errno != EWOULDBLOCK)
#else
                            if (errno != EWOULDBLOCK)
#endif
                            {
                                // TODO: any others?
                                if (errno == EPIPE || errno == ECONNRESET)
                                {
#ifdef _WIN32
                                    LOG_TRACE << "WSAENOTCONN or "
                                                 "WSAECONNRESET, errno="
                                              << errno;
#else
                                    LOG_TRACE << "EPIPE or "
                                                 "ECONNRESET, erron="
                                              << errno;
#endif
                                    return;
                                }
                                LOG_SYSERR << "Unexpected error(" << errno
                                           << ")";
                                return;
                            }
                        }
                    }
                    else
                    {
                        // next is a file
                        sendFileInLoop(writeBufferList_.front());
                    }
                }
            }
            else
            {
                sendFileInLoop(writeBuffer_);
            }
        }

        if (closeOnEmpty_ &&
            (writeBufferList_.empty() ||
             (tlsProviderPtr_ == nullptr ||
              tlsProviderPtr_->getBufferedData().readableBytes() == 0)))
        {
            shutdown();
        }
    }
    else
    {
        LOG_SYSERR << "no writing but write callback called";
    }
}
void TcpConnectionImpl::connectEstablished()
{
    auto thisPtr = shared_from_this();
    loop_->runInLoop([thisPtr]() {
        LOG_TRACE << "connectEstablished";
        assert(thisPtr->status_ == ConnStatus::Connecting);
        thisPtr->ioChannelPtr_->tie(thisPtr);
        thisPtr->ioChannelPtr_->enableReading();
        thisPtr->status_ = ConnStatus::Connected;

        if (thisPtr->tlsProviderPtr_)
            thisPtr->tlsProviderPtr_->startEncryption();
        else if (thisPtr->connectionCallback_)
            thisPtr->connectionCallback_(thisPtr);
    });
}
void TcpConnectionImpl::handleClose()
{
    LOG_TRACE << "connection closed, fd=" << socketPtr_->fd();
    loop_->assertInLoopThread();
    status_ = ConnStatus::Disconnected;
    ioChannelPtr_->disableAll();
    //  ioChannelPtr_->remove();
    auto guardThis = shared_from_this();
    if (connectionCallback_)
        connectionCallback_(guardThis);
    if (closeCallback_)
    {
        LOG_TRACE << "to call close callback";
        closeCallback_(guardThis);
    }
}
void TcpConnectionImpl::handleError()
{
    int err = socketPtr_->getSocketError();
    if (err == 0)
        return;
    if (err == EPIPE ||
#ifndef _WIN32
        err == EBADMSG ||  // ??? 104=EBADMSG
#endif
        err == ECONNRESET)
    {
        LOG_TRACE << "[" << name_ << "] - SO_ERROR = " << err << " "
                  << strerror_tl(err);
    }
    else
    {
        LOG_ERROR << "[" << name_ << "] - SO_ERROR = " << err << " "
                  << strerror_tl(err);
    }
}
void TcpConnectionImpl::setTcpNoDelay(bool on)
{
    socketPtr_->setTcpNoDelay(on);
}
void TcpConnectionImpl::connectDestroyed()
{
    loop_->assertInLoopThread();
    if (status_ == ConnStatus::Connected)
    {
        status_ = ConnStatus::Disconnected;
        ioChannelPtr_->disableAll();

        connectionCallback_(shared_from_this());
    }
    ioChannelPtr_->remove();
}
void TcpConnectionImpl::shutdown()
{
    auto thisPtr = shared_from_this();
    loop_->runInLoop([thisPtr]() {
        if (thisPtr->status_ == ConnStatus::Connected)
        {
            if (thisPtr->tlsProviderPtr_)
            {
                // there's still data to be sent, so we can't close the
                // connection just yet
                if (thisPtr->tlsProviderPtr_->getBufferedData()
                            .readableBytes() != 0 ||
                    thisPtr->writeBufferList_.size() != 0)
                {
                    thisPtr->closeOnEmpty_ = true;
                    return;
                }
                thisPtr->tlsProviderPtr_->close();
            }
            if (thisPtr->tlsProviderPtr_ == nullptr &&
                thisPtr->writeBufferList_.size() != 0)
            {
                thisPtr->closeOnEmpty_ = true;
                return;
            }
            thisPtr->status_ = ConnStatus::Disconnecting;
            if (!thisPtr->ioChannelPtr_->isWriting())
            {
                thisPtr->socketPtr_->closeWrite();
            }
        }
    });
}

void TcpConnectionImpl::forceClose()
{
    auto thisPtr = shared_from_this();
    loop_->runInLoop([thisPtr]() {
        if (thisPtr->status_ == ConnStatus::Connected ||
            thisPtr->status_ == ConnStatus::Disconnecting)
        {
            thisPtr->status_ = ConnStatus::Disconnecting;
            thisPtr->handleClose();
        }
    });
}
#ifndef _WIN32
void TcpConnectionImpl::sendInLoop(const void *buffer, size_t length)
#else
void TcpConnectionImpl::sendInLoop(const char *buffer, size_t length)
#endif
{
    loop_->assertInLoopThread();
    if (status_ != ConnStatus::Connected)
    {
        LOG_WARN << "Connection is not connected,give up sending";
        return;
    }
    extendLife();
    size_t remainLen = length;
    ssize_t sendLen = 0;
    if (!ioChannelPtr_->isWriting() && writeBufferList_.empty())
    {
        // send directly
        sendLen = writeInLoop(buffer, length);
        if (sendLen < 0)
        {
            // error
#ifdef _WIN32
            if (errno != 0 && errno != EWOULDBLOCK)
#else
            if (errno != EWOULDBLOCK)
#endif
            {
                if (errno == EPIPE || errno == ECONNRESET)  // TODO: any others?
                {
#ifdef _WIN32
                    LOG_TRACE << "WSAENOTCONN or WSAECONNRESET, errno="
                              << errno;
#else
                    LOG_TRACE << "EPIPE or ECONNRESET, errno=" << errno;
#endif
                    return;
                }
                LOG_SYSERR << "Unexpected error(" << errno << ")";
                return;
            }
            sendLen = 0;
        }
        remainLen -= sendLen;
    }
    if (remainLen > 0 && status_ == ConnStatus::Connected)
    {
        if (writeBufferList_.empty())
        {
            BufferNodePtr node = std::make_shared<BufferNode>();
            node->msgBuffer_ = std::make_shared<MsgBuffer>();
            writeBufferList_.push_back(std::move(node));
        }
        else if (writeBufferList_.back()->isFile())
        {
            BufferNodePtr node = std::make_shared<BufferNode>();
            node->msgBuffer_ = std::make_shared<MsgBuffer>();
            writeBufferList_.push_back(std::move(node));
        }
        writeBufferList_.back()->msgBuffer_->append(
            static_cast<const char *>(buffer) + sendLen, remainLen);
        if (!ioChannelPtr_->isWriting())
            ioChannelPtr_->enableWriting();
        if (highWaterMarkCallback_ &&
            writeBufferList_.back()->msgBuffer_->readableBytes() >
                highWaterMarkLen_)
        {
            highWaterMarkCallback_(
                shared_from_this(),
                writeBufferList_.back()->msgBuffer_->readableBytes());
        }
        if (highWaterMarkCallback_ && tlsProviderPtr_ &&
            tlsProviderPtr_->getBufferedData().readableBytes() >
                highWaterMarkLen_)
        {
            highWaterMarkCallback_(
                shared_from_this(),
                tlsProviderPtr_->getBufferedData().readableBytes());
        }
    }
}
// The order of data sending should be same as the order of calls of send()
void TcpConnectionImpl::send(const std::shared_ptr<std::string> &msgPtr)
{
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            sendInLoop(msgPtr->data(), msgPtr->length());
        }
        else
        {
            ++sendNum_;
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, msgPtr]() {
                thisPtr->sendInLoop(msgPtr->data(), msgPtr->length());
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            });
        }
    }
    else
    {
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, msgPtr]() {
            thisPtr->sendInLoop(msgPtr->data(), msgPtr->length());
            std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
            --thisPtr->sendNum_;
        });
    }
}
// The order of data sending should be same as the order of calls of send()
void TcpConnectionImpl::send(const std::shared_ptr<MsgBuffer> &msgPtr)
{
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            sendInLoop(msgPtr->peek(), msgPtr->readableBytes());
        }
        else
        {
            ++sendNum_;
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, msgPtr]() {
                thisPtr->sendInLoop(msgPtr->peek(), msgPtr->readableBytes());
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            });
        }
    }
    else
    {
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, msgPtr]() {
            thisPtr->sendInLoop(msgPtr->peek(), msgPtr->readableBytes());
            std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
            --thisPtr->sendNum_;
        });
    }
}
void TcpConnectionImpl::send(const char *msg, size_t len)
{
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            sendInLoop(msg, len);
        }
        else
        {
            ++sendNum_;
            auto buffer = std::make_shared<std::string>(msg, len);
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, buffer]() {
                thisPtr->sendInLoop(buffer->data(), buffer->length());
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            });
        }
    }
    else
    {
        auto buffer = std::make_shared<std::string>(msg, len);
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, buffer]() {
            thisPtr->sendInLoop(buffer->data(), buffer->length());
            std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
            --thisPtr->sendNum_;
        });
    }
}
void TcpConnectionImpl::send(const void *msg, size_t len)
{
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
#ifndef _WIN32
            sendInLoop(msg, len);
#else
            sendInLoop(static_cast<const char *>(msg), len);
#endif
        }
        else
        {
            ++sendNum_;
            auto buffer =
                std::make_shared<std::string>(static_cast<const char *>(msg),
                                              len);
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, buffer]() {
                thisPtr->sendInLoop(buffer->data(), buffer->length());
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            });
        }
    }
    else
    {
        auto buffer =
            std::make_shared<std::string>(static_cast<const char *>(msg), len);
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, buffer]() {
            thisPtr->sendInLoop(buffer->data(), buffer->length());
            std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
            --thisPtr->sendNum_;
        });
    }
}
void TcpConnectionImpl::send(const std::string &msg)
{
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            sendInLoop(msg.data(), msg.length());
        }
        else
        {
            ++sendNum_;
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, msg]() {
                thisPtr->sendInLoop(msg.data(), msg.length());
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            });
        }
    }
    else
    {
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, msg]() {
            thisPtr->sendInLoop(msg.data(), msg.length());
            std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
            --thisPtr->sendNum_;
        });
    }
}
void TcpConnectionImpl::send(std::string &&msg)
{
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            sendInLoop(msg.data(), msg.length());
        }
        else
        {
            auto thisPtr = shared_from_this();
            ++sendNum_;
            loop_->queueInLoop([thisPtr, msg = std::move(msg)]() {
                thisPtr->sendInLoop(msg.data(), msg.length());
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            });
        }
    }
    else
    {
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, msg = std::move(msg)]() {
            thisPtr->sendInLoop(msg.data(), msg.length());
            std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
            --thisPtr->sendNum_;
        });
    }
}

void TcpConnectionImpl::send(const MsgBuffer &buffer)
{
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            sendInLoop(buffer.peek(), buffer.readableBytes());
        }
        else
        {
            ++sendNum_;
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, buffer]() {
                thisPtr->sendInLoop(buffer.peek(), buffer.readableBytes());
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            });
        }
    }
    else
    {
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, buffer]() {
            thisPtr->sendInLoop(buffer.peek(), buffer.readableBytes());
            std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
            --thisPtr->sendNum_;
        });
    }
}

void TcpConnectionImpl::send(MsgBuffer &&buffer)
{
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            sendInLoop(buffer.peek(), buffer.readableBytes());
        }
        else
        {
            ++sendNum_;
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, buffer = std::move(buffer)]() {
                thisPtr->sendInLoop(buffer.peek(), buffer.readableBytes());
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            });
        }
    }
    else
    {
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, buffer = std::move(buffer)]() {
            thisPtr->sendInLoop(buffer.peek(), buffer.readableBytes());
            std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
            --thisPtr->sendNum_;
        });
    }
}
void TcpConnectionImpl::sendFile(const char *fileName,
                                 size_t offset,
                                 size_t length)
{
    assert(fileName);
#ifdef _WIN32
    sendFile(utils::toNativePath(fileName).c_str(), offset, length);
#else   // _WIN32
    int fd = open(fileName, O_RDONLY);

    if (fd < 0)
    {
        LOG_SYSERR << fileName << " open error";
        return;
    }

    if (length == 0)
    {
        struct stat filestat;
        if (stat(fileName, &filestat) < 0)
        {
            LOG_SYSERR << fileName << " stat error";
            close(fd);
            return;
        }
        length = filestat.st_size;
    }

    sendFile(fd, offset, length);
#endif  // _WIN32
}

void TcpConnectionImpl::sendFile(const wchar_t *fileName,
                                 size_t offset,
                                 size_t length)
{
    assert(fileName);
#ifndef _WIN32
    sendFile(utils::toNativePath(fileName).c_str(), offset, length);
#else  // _WIN32
    FILE *fp;
#ifndef _MSC_VER
    fp = _wfopen(fileName, L"rb");
#else   // _MSC_VER
    if (_wfopen_s(&fp, fileName, L"rb") != 0)
        fp = nullptr;
#endif  // _MSC_VER
    if (fp == nullptr)
    {
        LOG_SYSERR << fileName << " open error";
        return;
    }

    if (length == 0)
    {
        struct _stati64 filestat;
        if (_wstati64(fileName, &filestat) < 0)
        {
            LOG_SYSERR << fileName << " stat error";
            fclose(fp);
            return;
        }
        length = filestat.st_size;
    }

    sendFile(fp, offset, length);
#endif  // _WIN32
}

#ifndef _WIN32
void TcpConnectionImpl::sendFile(int sfd, size_t offset, size_t length)
#else
void TcpConnectionImpl::sendFile(FILE *fp, size_t offset, size_t length)
#endif
{
    assert(length > 0);
#ifndef _WIN32
    assert(sfd >= 0);
    BufferNodePtr node = std::make_shared<BufferNode>();
    node->sendFd_ = sfd;
#else
    assert(fp);
    BufferNodePtr node = std::make_shared<BufferNode>();
    node->sendFp_ = fp;
#endif
    node->offset_ = offset;
    node->fileBytesToSend_ = length;
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            writeBufferList_.push_back(node);
            if (writeBufferList_.size() == 1)
            {
                sendFileInLoop(writeBufferList_.front());
                return;
            }
        }
        else
        {
            ++sendNum_;
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, node]() {
                thisPtr->writeBufferList_.push_back(node);
                {
                    std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                    --thisPtr->sendNum_;
                }

                if (thisPtr->writeBufferList_.size() == 1)
                {
                    thisPtr->sendFileInLoop(thisPtr->writeBufferList_.front());
                }
            });
        }
    }
    else
    {
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, node]() {
            LOG_TRACE << "Push sendfile to list";
            thisPtr->writeBufferList_.push_back(node);

            {
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            }

            if (thisPtr->writeBufferList_.size() == 1)
            {
                thisPtr->sendFileInLoop(thisPtr->writeBufferList_.front());
            }
        });
    }
}

void TcpConnectionImpl::sendStream(
    std::function<std::size_t(char *, std::size_t)> callback)
{
    BufferNodePtr node = std::make_shared<BufferNode>();
    node->offset_ =
        0;  // not used, the offset should be handled by the callback
    node->fileBytesToSend_ = 1;  // force to > 0 until stream sent
    node->streamCallback_ = std::move(callback);
    if (loop_->isInLoopThread())
    {
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        if (sendNum_ == 0)
        {
            writeBufferList_.push_back(node);
            if (writeBufferList_.size() == 1)
            {
                sendFileInLoop(writeBufferList_.front());
                return;
            }
        }
        else
        {
            ++sendNum_;
            auto thisPtr = shared_from_this();
            loop_->queueInLoop([thisPtr, node]() {
                thisPtr->writeBufferList_.push_back(node);
                {
                    std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                    --thisPtr->sendNum_;
                }

                if (thisPtr->writeBufferList_.size() == 1)
                {
                    thisPtr->sendFileInLoop(thisPtr->writeBufferList_.front());
                }
            });
        }
    }
    else
    {
        auto thisPtr = shared_from_this();
        std::lock_guard<std::mutex> guard(sendNumMutex_);
        ++sendNum_;
        loop_->queueInLoop([thisPtr, node]() {
            LOG_TRACE << "Push sendstream to list";
            thisPtr->writeBufferList_.push_back(node);

            {
                std::lock_guard<std::mutex> guard1(thisPtr->sendNumMutex_);
                --thisPtr->sendNum_;
            }

            if (thisPtr->writeBufferList_.size() == 1)
            {
                thisPtr->sendFileInLoop(thisPtr->writeBufferList_.front());
            }
        });
    }
}

void TcpConnectionImpl::sendFileInLoop(const BufferNodePtr &filePtr)
{
    loop_->assertInLoopThread();
    assert(filePtr->isFile());
#ifdef __linux__
    if (!filePtr->streamCallback_ && !tlsProviderPtr_)
    {
        LOG_TRACE << "send file in loop using linux kernel sendfile()";
        auto bytesSent = sendfile(socketPtr_->fd(),
                                  filePtr->sendFd_,
                                  &filePtr->offset_,
                                  filePtr->fileBytesToSend_);
        if (bytesSent < 0)
        {
            if (errno != EAGAIN)
            {
                LOG_SYSERR << "TcpConnectionImpl::sendFileInLoop";
                if (ioChannelPtr_->isWriting())
                    ioChannelPtr_->disableWriting();
            }
            return;
        }
        if (bytesSent < filePtr->fileBytesToSend_)
        {
            if (bytesSent == 0)
            {
                LOG_SYSERR << "TcpConnectionImpl::sendFileInLoop";
                return;
            }
        }
        LOG_TRACE << "sendfile() " << bytesSent << " bytes sent";
        filePtr->fileBytesToSend_ -= bytesSent;
        if (!ioChannelPtr_->isWriting())
        {
            ioChannelPtr_->enableWriting();
        }
        return;
    }
#endif
    // Send stream
    if (filePtr->streamCallback_)
    {
        LOG_TRACE << "send stream in loop";
        if (!fileBufferPtr_)
        {
            fileBufferPtr_ = std::make_unique<std::vector<char>>();
            fileBufferPtr_->reserve(kMaxSendFileBufferSize);
        }
        while ((filePtr->fileBytesToSend_ > 0) || !fileBufferPtr_->empty())
        {
            // get next chunk
            if (fileBufferPtr_->empty())
            {
                //                LOG_TRACE << "send stream in loop: fetch data
                //                on buffer empty";
                fileBufferPtr_->resize(kMaxSendFileBufferSize);
                std::size_t nData;
                nData = filePtr->streamCallback_(fileBufferPtr_->data(),
                                                 fileBufferPtr_->size());
                fileBufferPtr_->resize(nData);
                if (nData == 0)  // no more data!
                {
                    LOG_TRACE << "send stream in loop: no more data";
                    filePtr->fileBytesToSend_ = 0;
                }
            }
            if (fileBufferPtr_->empty())
            {
                LOG_TRACE << "send stream in loop: break on buffer empty";
                break;
            }
            auto nToWrite = fileBufferPtr_->size();
            auto nWritten = writeInLoop(fileBufferPtr_->data(), nToWrite);
            if (nWritten >= 0)
            {
#ifndef NDEBUG  // defined by CMake for release build
                filePtr->nDataWritten_ += nWritten;
                LOG_TRACE << "send stream in loop: bytes written: " << nWritten
                          << " / total bytes written: "
                          << filePtr->nDataWritten_;
#endif
                if (static_cast<std::size_t>(nWritten) < nToWrite)
                {
                    // Partial write - return and wait for next call to continue
                    fileBufferPtr_->erase(fileBufferPtr_->begin(),
                                          fileBufferPtr_->begin() + nWritten);
                    if (!ioChannelPtr_->isWriting())
                        ioChannelPtr_->enableWriting();
                    LOG_TRACE << "send stream in loop: return on partial write "
                                 "(socket buffer full?)";
                    return;
                }
                //                LOG_TRACE << "send stream in loop: continue on
                //                data written";
                fileBufferPtr_->resize(0);
                continue;
            }
            // nWritten < 0
#ifdef _WIN32
            if (errno != 0 && errno != EWOULDBLOCK)
#else
            if (errno != EWOULDBLOCK)
#endif
            {
                if (errno == EPIPE || errno == ECONNRESET)
                {
#ifdef _WIN32
                    LOG_TRACE << "WSAENOTCONN or WSAECONNRESET, errno="
                              << errno;
#else
                    LOG_TRACE << "EPIPE or ECONNRESET, errno=" << errno;
#endif
                    // abort
                    LOG_TRACE
                        << "send stream in loop: return on connection closed";
                    filePtr->fileBytesToSend_ = 0;
                    return;
                }
                // TODO: any others?
                LOG_SYSERR << "send stream in loop: return on unexpected error("
                           << errno << ")";
                filePtr->fileBytesToSend_ = 0;
                return;
            }
            // Socket buffer full - return and wait for next call
            LOG_TRACE << "send stream in loop: break on socket buffer full (?)";
            break;
        }
        if (!ioChannelPtr_->isWriting())
            ioChannelPtr_->enableWriting();
        LOG_TRACE << "send stream in loop: return on loop exit";
        return;
    }
    // Send file
    LOG_TRACE << "send file in loop";
    if (!fileBufferPtr_)
    {
        fileBufferPtr_ =
            std::make_unique<std::vector<char>>(kMaxSendFileBufferSize);
    }
    if (fileBufferPtr_->size() < kMaxSendFileBufferSize)
    {
        fileBufferPtr_->resize(kMaxSendFileBufferSize);
    }
#ifndef _WIN32
    lseek(filePtr->sendFd_, filePtr->offset_, SEEK_SET);
    while (filePtr->fileBytesToSend_ > 0)
    {
        auto n = read(filePtr->sendFd_,
                      &(*fileBufferPtr_)[0],
                      std::min(fileBufferPtr_->size(),
                               static_cast<decltype(fileBufferPtr_->size())>(
                                   filePtr->fileBytesToSend_)));
#else
    _fseeki64(filePtr->sendFp_, filePtr->offset_, SEEK_SET);
    while (filePtr->fileBytesToSend_ > 0)
    {
        //        LOG_TRACE << "send file in loop: fetch more remaining data";
        auto bytes = static_cast<decltype(fileBufferPtr_->size())>(
            filePtr->fileBytesToSend_);
        auto n = fread(&(*fileBufferPtr_)[0],
                       1,
                       (fileBufferPtr_->size() < bytes ? fileBufferPtr_->size()
                                                       : bytes),
                       filePtr->sendFp_);
#endif
        if (n > 0)
        {
            auto nSend = writeInLoop(&(*fileBufferPtr_)[0], n);
            if (nSend >= 0)
            {
                filePtr->fileBytesToSend_ -= nSend;
                filePtr->offset_ += nSend;
                if (static_cast<size_t>(nSend) < static_cast<size_t>(n))
                {
                    if (!ioChannelPtr_->isWriting())
                    {
                        ioChannelPtr_->enableWriting();
                    }
                    LOG_TRACE << "send file in loop: return on partial write "
                                 "(socket buffer full?)";
                    return;
                }
                else if (nSend == n)
                {
                    //                    LOG_TRACE << "send file in loop:
                    //                    continue on data written";
                    continue;
                }
            }
            if (nSend < 0)
            {
#ifdef _WIN32
                if (errno != 0 && errno != EWOULDBLOCK)
#else
                if (errno != EWOULDBLOCK)
#endif
                {
                    // TODO: any others?
                    if (errno == EPIPE || errno == ECONNRESET)
                    {
#ifdef _WIN32
                        LOG_TRACE << "WSAENOTCONN or WSAECONNRESET, errno="
                                  << errno;
#else
                        LOG_TRACE << "EPIPE or ECONNRESET, errno=" << errno;
#endif
                        LOG_TRACE
                            << "send file in loop: return on connection closed";
                        return;
                    }
                    LOG_SYSERR
                        << "send file in loop: return on unexpected error("
                        << errno << ")";
                    return;
                }
                LOG_TRACE
                    << "send file in loop: break on socket buffer full (?)";
                break;
            }
        }
        if (n < 0)
        {
            LOG_SYSERR << "send file in loop: return on read error";
            if (ioChannelPtr_->isWriting())
                ioChannelPtr_->disableWriting();
            return;
        }
        if (n == 0)
        {
            LOG_SYSERR
                << "send file in loop: return on read 0 (file truncated)";
            return;
        }
    }
    LOG_TRACE << "send file in loop: return on loop exit";
    if (!ioChannelPtr_->isWriting())
    {
        ioChannelPtr_->enableWriting();
    }
}
#ifndef _WIN32
ssize_t TcpConnectionImpl::writeRaw(const void *buffer, size_t length)
#else
ssize_t TcpConnectionImpl::writeRaw(const char *buffer, size_t length)
#endif
{
    // TODO: Abstract this away to support io_uring (and IOCP?)
#ifndef _WIN32
    int nWritten = write(socketPtr_->fd(), buffer, length);
#else
    int nWritten =
        ::send(socketPtr_->fd(), buffer, static_cast<int>(length), 0);
    errno = (nWritten < 0) ? ::WSAGetLastError() : 0;
#endif
    if (nWritten > 0)
        bytesSent_ += nWritten;
    return nWritten;
}

#ifndef _WIN32
ssize_t TcpConnectionImpl::writeInLoop(const void *buffer, size_t length)
#else
ssize_t TcpConnectionImpl::writeInLoop(const char *buffer, size_t length)
#endif
{
    if (tlsProviderPtr_)
        return tlsProviderPtr_->sendData((const char *)buffer, length);
    else
        return writeRaw(buffer, length);
}

#if !(defined(USE_OPENSSL) || defined(USE_BOTAN))
SSLContextPtr trantor::newSSLContext(const TLSPolicy &policy, bool isServer)
{
    (void)policy;
    (void)isServer;
    throw std::runtime_error("SSL is not supported");
}

std::shared_ptr<TLSProvider> trantor::newTLSProvider(TcpConnection *conn,
                                                     TLSPolicyPtr policy,
                                                     SSLContextPtr sslContext)
{
    (void)conn;
    (void)policy;
    (void)sslContext;
    throw std::runtime_error("SSL is not supported");
}
#endif

void TcpConnectionImpl::startEncryption(
    TLSPolicyPtr policy,
    bool isServer,
    std::function<void(const TcpConnectionPtr &)> upgradeCallback)
{
    if (tlsProviderPtr_ || upgradeCallback_)
    {
        LOG_ERROR << "TLS is already started";
        return;
    }
    auto sslContextPtr = newSSLContext(*policy, isServer);
    tlsProviderPtr_ = newTLSProvider(this, policy, sslContextPtr);
    tlsProviderPtr_->setWriteCallback(onSslWrite);
    tlsProviderPtr_->setErrorCallback(onSslError);
    tlsProviderPtr_->setHandshakeCallback(onHandshakeFinished);
    tlsProviderPtr_->setMessageCallback(onSslMessage);
    // This is triggered when peer sends a close alert
    tlsProviderPtr_->setCloseCallback(onSslCloseAlert);
    tlsProviderPtr_->startEncryption();
    upgradeCallback_ = std::move(upgradeCallback);
}

void TcpConnectionImpl::onSslError(TcpConnection *self, SSLError err)
{
    self->forceClose();
    if (self->sslErrorCallback_)
        self->sslErrorCallback_(err);
}
void TcpConnectionImpl::onHandshakeFinished(TcpConnection *self)
{
    auto connPtr = ((TcpConnectionImpl *)self)->shared_from_this();
    if (connPtr->upgradeCallback_)
    {
        connPtr->upgradeCallback_(connPtr);
        connPtr->upgradeCallback_ = nullptr;
    }
    else if (self->connectionCallback_)
        self->connectionCallback_(connPtr);
}
void TcpConnectionImpl::onSslMessage(TcpConnection *self, MsgBuffer *buffer)
{
    if (self->recvMsgCallback_)
        self->recvMsgCallback_(((TcpConnectionImpl *)self)->shared_from_this(),
                               buffer);
}
ssize_t TcpConnectionImpl::onSslWrite(TcpConnection *self,
                                      const void *data,
                                      size_t len)
{
    auto connPtr = (TcpConnectionImpl *)self;
    return connPtr->writeRaw((const char *)data, len);
}
void TcpConnectionImpl::onSslCloseAlert(TcpConnection *self)
{
    self->shutdown();
}
