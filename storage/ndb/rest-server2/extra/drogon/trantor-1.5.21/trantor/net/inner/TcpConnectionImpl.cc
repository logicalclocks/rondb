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
#endif

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
static inline bool isEAGAIN()
{
    if (errno == EWOULDBLOCK || errno == EAGAIN || errno == 0)
    {
        LOG_TRACE << "write buffer is full";
        return true;
    }
    else if (errno == EPIPE || errno == ECONNRESET)
    {
#ifdef _WIN32
        LOG_TRACE << "WSAENOTCONN or WSAECONNRESET, errno=" << errno;
#else
        LOG_TRACE << "EPIPE or ECONNRESET, errno=" << errno;
#endif
        LOG_TRACE << "send node in loop: return on connection closed";
        return false;
    }
    else
    {
        // TODO: any others?
        LOG_SYSERR << "send node in loop: return on unexpected error(" << errno
                   << ")";
        return false;
    }
}

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
    ioChannelPtr_->setReadCallback([this]() { readCallback(); });
    ioChannelPtr_->setWriteCallback([this]() { writeCallback(); });
    ioChannelPtr_->setCloseCallback([this]() { handleClose(); });
    ioChannelPtr_->setErrorCallback([this]() { handleError(); });
    socketPtr_->setKeepAlive(true);
    name_ = localAddr.toIpPort() + "--" + peerAddr.toIpPort();

    if (policy != nullptr)
    {
        tlsProviderPtr_ =
            newTLSProvider(this, std::move(policy), std::move(ctx));
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
    std::size_t readableTlsBytes = 0;
    if (tlsProviderPtr_)
    {
        readableTlsBytes = tlsProviderPtr_->getBufferedData().readableBytes();
    }
    if (!writeBufferList_.empty())
    {
        LOG_DEBUG << "write node list size: " << writeBufferList_.size()
                  << " first node is file? "
                  << writeBufferList_.front()->isFile()
                  << " first node is stream? "
                  << writeBufferList_.front()->isStream()
                  << " first node is async? "
                  << writeBufferList_.front()->isAsync() << " first node size: "
                  << writeBufferList_.front()->remainingBytes()
                  << " buffered TLS data size: " << readableTlsBytes;
    }
    else if (readableTlsBytes != 0)
    {
        LOG_DEBUG << "write node list size: 0 buffered TLS data size: "
                  << readableTlsBytes;
    }
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
    if (ioChannelPtr_->isWriting())
    {
        if (tlsProviderPtr_)
        {
            bool sentAll = tlsProviderPtr_->sendBufferedData();
            if (!sentAll)
            {
                return;
            }
        }
        while (!writeBufferList_.empty())
        {
            auto &nodePtr = writeBufferList_.front();
            if (nodePtr->remainingBytes() == 0)
            {
                if (!nodePtr->isAsync() || !nodePtr->available())
                {
                    // finished sending
                    writeBufferList_.pop_front();
                }
                else
                {
                    // the first node is an async node and is available
                    ioChannelPtr_->disableWriting();
                    return;
                }
            }
            else
            {
                // continue sending
                auto n = sendNodeInLoop(nodePtr);
                if (nodePtr->remainingBytes() > 0 || n < 0)
                    return;
            }
        }
        assert(writeBufferList_.empty());
        if (tlsProviderPtr_ == nullptr ||
            tlsProviderPtr_->getBufferedData().readableBytes() == 0)
        {
            ioChannelPtr_->disableWriting();
            if (closeOnEmpty_)
            {
                shutdown();
            }
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
    LOG_TRACE << "write buffer size: " << writeBufferList_.size();
    if (!writeBufferList_.empty())
    {
        LOG_TRACE << writeBufferList_.front()->isFile();
        LOG_TRACE << writeBufferList_.front()->isStream();
    }
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
                    !thisPtr->writeBufferList_.empty())
                {
                    thisPtr->closeOnEmpty_ = true;
                    return;
                }
                thisPtr->tlsProviderPtr_->close();
            }
            if (thisPtr->tlsProviderPtr_ == nullptr &&
                !thisPtr->writeBufferList_.empty())
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

            if (thisPtr->tlsProviderPtr_)
                thisPtr->tlsProviderPtr_->close();
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
        LOG_DEBUG << "Connection is not connected,give up sending";
        return;
    }
    ssize_t sendLen = 0;
    if (!ioChannelPtr_->isWriting() && writeBufferList_.empty())
    {
        // send directly
        sendLen = writeInLoop(buffer, length);
        if (sendLen < 0)
        {
            LOG_TRACE << "write error";
            return;
        }
        length -= sendLen;
    }
    if (length > 0 && status_ == ConnStatus::Connected)
    {
        if (writeBufferList_.empty() || writeBufferList_.back()->isFile() ||
            writeBufferList_.back()->isStream())
        {
            writeBufferList_.push_back(BufferNode::newMemBufferNode());
        }
        writeBufferList_.back()->append(static_cast<const char *>(buffer) +
                                            sendLen,
                                        length);
        if (highWaterMarkCallback_ &&
            writeBufferList_.back()->remainingBytes() >
                static_cast<long long>(highWaterMarkLen_))
        {
            highWaterMarkCallback_(shared_from_this(),
                                   writeBufferList_.back()->remainingBytes());
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
        sendInLoop(msgPtr->data(), msgPtr->length());
    }
    else
    {
        loop_->queueInLoop([thisPtr = shared_from_this(), msgPtr]() {
            thisPtr->sendInLoop(msgPtr->data(), msgPtr->length());
        });
    }
}
// The order of data sending should be same as the order of calls of send()
void TcpConnectionImpl::send(const std::shared_ptr<MsgBuffer> &msgPtr)
{
    if (loop_->isInLoopThread())
    {
        sendInLoop(msgPtr->peek(), msgPtr->readableBytes());
    }
    else
    {
        loop_->queueInLoop([thisPtr = shared_from_this(), msgPtr]() {
            thisPtr->sendInLoop(msgPtr->peek(), msgPtr->readableBytes());
        });
    }
}
void TcpConnectionImpl::send(const char *msg, size_t len)
{
    if (loop_->isInLoopThread())
    {
        sendInLoop(msg, len);
    }
    else
    {
        auto buffer = std::make_shared<std::string>(msg, len);
        loop_->queueInLoop(
            [thisPtr = shared_from_this(), buffer = std::move(buffer)]() {
                thisPtr->sendInLoop(buffer->data(), buffer->length());
            });
    }
}
void TcpConnectionImpl::send(const void *msg, size_t len)
{
    if (loop_->isInLoopThread())
    {
#ifndef _WIN32
        sendInLoop(msg, len);
#else
        sendInLoop(static_cast<const char *>(msg), len);
#endif
    }
    else
    {
        auto buffer =
            std::make_shared<std::string>(static_cast<const char *>(msg), len);
        loop_->queueInLoop(
            [thisPtr = shared_from_this(), buffer = std::move(buffer)]() {
                thisPtr->sendInLoop(buffer->data(), buffer->length());
            });
    }
}
void TcpConnectionImpl::send(const std::string &msg)
{
    if (loop_->isInLoopThread())
    {
        sendInLoop(msg.data(), msg.length());
    }
    else
    {
        loop_->queueInLoop([thisPtr = shared_from_this(), msg]() {
            thisPtr->sendInLoop(msg.data(), msg.length());
        });
    }
}
void TcpConnectionImpl::send(std::string &&msg)
{
    if (loop_->isInLoopThread())
    {
        sendInLoop(msg.data(), msg.length());
    }
    else
    {
        loop_->queueInLoop(
            [thisPtr = shared_from_this(), msg = std::move(msg)]() {
                thisPtr->sendInLoop(msg.data(), msg.length());
            });
    }
}

void TcpConnectionImpl::send(const MsgBuffer &buffer)
{
    if (loop_->isInLoopThread())
    {
        sendInLoop(buffer.peek(), buffer.readableBytes());
    }
    else
    {
        loop_->queueInLoop([thisPtr = shared_from_this(), buffer]() {
            thisPtr->sendInLoop(buffer.peek(), buffer.readableBytes());
        });
    }
}

void TcpConnectionImpl::send(MsgBuffer &&buffer)
{
    if (loop_->isInLoopThread())
    {
        sendInLoop(buffer.peek(), buffer.readableBytes());
    }
    else
    {
        loop_->queueInLoop(
            [thisPtr = shared_from_this(), buffer = std::move(buffer)]() {
                thisPtr->sendInLoop(buffer.peek(), buffer.readableBytes());
            });
    }
}
void TcpConnectionImpl::sendFile(const char *fileName,
                                 long long offset,
                                 long long length)
{
    assert(fileName);
#ifdef _WIN32
    sendFile(utils::toNativePath(fileName).c_str(), offset, length);
#else   // _WIN32
    auto fileNode = BufferNode::newFileBufferNode(fileName, offset, length);

    if (!fileNode->available())
    {
        LOG_SYSERR << fileName << " open error";
        return;
    }

    sendFile(std::move(fileNode));
#endif  // _WIN32
}

void TcpConnectionImpl::sendFile(const wchar_t *fileName,
                                 long long offset,
                                 long long length)
{
    assert(fileName);
#ifndef _WIN32
    sendFile(utils::toNativePath(fileName).c_str(), offset, length);
#else
    auto fileNode = BufferNode::newFileBufferNode(fileName, offset, length);
    if (!fileNode->available())
    {
        LOG_SYSERR << fileName << " open error";
        return;
    }
    sendFile(std::move(fileNode));
#endif  // _WIN32
}

void TcpConnectionImpl::sendFile(BufferNodePtr &&fileNode)
{
    assert(fileNode->isFile() && fileNode->remainingBytes() > 0);
    if (loop_->isInLoopThread())
    {
        if (writeBufferList_.empty())
        {
            auto n = sendNodeInLoop(fileNode);
            if (fileNode->remainingBytes() > 0 && n >= 0)
                writeBufferList_.push_back(std::move(fileNode));
            return;
        }
        else
        {
            writeBufferList_.push_back(std::move(fileNode));
        }
    }
    else
    {
        loop_->queueInLoop([thisPtr = shared_from_this(),
                            node = std::move(fileNode)]() mutable {
            if (thisPtr->writeBufferList_.empty())
            {
                auto n = thisPtr->sendNodeInLoop(node);
                if (node->remainingBytes() > 0 && n >= 0)
                    thisPtr->writeBufferList_.push_back(std::move(node));
            }
            else
            {
                thisPtr->writeBufferList_.push_back(std::move(node));
            }
        });
    }
}

void TcpConnectionImpl::sendStream(
    std::function<std::size_t(char *, std::size_t)> callback)
{
    auto node = BufferNode::newStreamBufferNode(std::move(callback));
    if (loop_->isInLoopThread())
    {
        if (writeBufferList_.empty())
        {
            auto n = sendNodeInLoop(node);
            if (node->remainingBytes() > 0 && n >= 0)
                writeBufferList_.push_back(std::move(node));
            return;
        }
    }
    else
    {
        loop_->queueInLoop(
            [thisPtr = shared_from_this(), node = std::move(node)]() mutable {
                LOG_TRACE << "Push send stream to list";
                if (thisPtr->writeBufferList_.empty())
                {
                    auto n = thisPtr->sendNodeInLoop(node);
                    if (node->remainingBytes() > 0 && n >= 0)
                        thisPtr->writeBufferList_.push_back(std::move(node));
                }
                else
                {
                    thisPtr->writeBufferList_.push_back(std::move(node));
                }
            });
    }
}

ssize_t TcpConnectionImpl::sendNodeInLoop(const BufferNodePtr &nodePtr)
{
    loop_->assertInLoopThread();
#ifdef __linux__
    if (nodePtr->isFile() && !tlsProviderPtr_)
    {
        static const long long kMaxSendBytes = 0x7ffff000;
        LOG_TRACE << "send file in loop using linux kernel sendfile()";
        auto toSend = nodePtr->remainingBytes();
        if (toSend <= 0)
        {
            LOG_ERROR << "0 or negative bytes to send";
            return -1;
        }
        auto bytesSent =
            sendfile(socketPtr_->fd(),
                     nodePtr->getFd(),
                     nullptr,
                     static_cast<size_t>(
                         toSend < kMaxSendBytes ? toSend : kMaxSendBytes));
        if (bytesSent > 0)
        {
            nodePtr->retrieve(bytesSent);
            bytesSent_ += bytesSent;
        }
        else if (!isEAGAIN())
            return -1;
        extendLife();
        if (bytesSent < toSend)
        {
            LOG_TRACE << "bytesSent = " << bytesSent << " toSend = " << toSend;
            if (!ioChannelPtr_->isWriting())
                ioChannelPtr_->enableWriting();
        }
        return bytesSent;
    }
#endif
    // Send stream

    LOG_TRACE << "send node in loop";
    const char *data;
    size_t len;
    ssize_t hasSent = 0;
    while ((nodePtr->remainingBytes() > 0))
    {
        // get next chunk
        nodePtr->getData(data, len);
        if (len == 0)
        {
            nodePtr->done();
            break;
        }
        auto nWritten = writeInLoop(data, len);
        if (nWritten >= 0)
        {
            hasSent += nWritten;
            nodePtr->retrieve(nWritten);
            if (static_cast<std::size_t>(nWritten) < len)
            {
                break;
            }
            continue;
        }
        else
        {
            LOG_TRACE << "error(" << errno << ") on send Node in loop";
            return -1;
        }
    }
    return hasSent;
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
    else if (!isEAGAIN())
        return nWritten;
    if (nWritten < 0)
    {
        nWritten = 0;
    }
    if (nWritten < static_cast<int>(length))
    {
        LOG_TRACE << "nWritten = " << nWritten << " length = " << length;
        if (!ioChannelPtr_->isWriting())
            ioChannelPtr_->enableWriting();
    }
    extendLife();
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
    tlsProviderPtr_ =
        newTLSProvider(this, std::move(policy), std::move(sslContextPtr));
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
    if (self->sslErrorCallback_)
        self->sslErrorCallback_(err);
    self->forceClose();
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
class AsyncStreamImpl : public AsyncStream
{
  public:
    explicit AsyncStreamImpl(std::function<bool(const char *, size_t)> callback)
        : callback_(std::move(callback))
    {
    }
    AsyncStreamImpl() = delete;
    bool send(const char *data, size_t len) override
    {
        return callback_(data, len);
    }
    void close() override
    {
        callback_(nullptr, 0);
        callback_ = nullptr;
    }
    ~AsyncStreamImpl() override
    {
        if (callback_)
            callback_(nullptr, 0);
    }

  private:
    std::function<bool(const char *, size_t)> callback_;
};
AsyncStreamPtr TcpConnectionImpl::sendAsyncStream(bool disableKickoff)
{
    auto asyncStreamNode = BufferNode::newAsyncStreamBufferNode();
    std::weak_ptr<TcpConnectionImpl> weakPtr = shared_from_this();
    auto asyncStream = std::make_unique<AsyncStreamImpl>(
        [asyncStreamNode, weakPtr = std::move(weakPtr)](const char *data,
                                                        size_t len) -> bool {
            auto thisPtr = weakPtr.lock();
            if (!thisPtr)
            {
                LOG_DEBUG << "Connection is closed,give up sending";
                return false;
            }
            if (thisPtr->status_ != ConnStatus::Connected)
            {
                LOG_DEBUG << "Connection is not connected,give up sending";
                return false;
            }
            if (thisPtr->loop_->isInLoopThread())
            {
                thisPtr->sendAsyncDataInLoop(asyncStreamNode, data, len);
            }
            else
            {
                if (data)
                {
                    std::string buffer(data, len);
                    thisPtr->loop_->queueInLoop([thisPtr,
                                                 asyncStreamNode,
                                                 buffer = std::move(buffer)]() {
                        thisPtr->sendAsyncDataInLoop(asyncStreamNode,
                                                     buffer.data(),
                                                     buffer.length());
                    });
                }
                else
                {
                    thisPtr->loop_->queueInLoop([thisPtr, asyncStreamNode]() {
                        thisPtr->sendAsyncDataInLoop(asyncStreamNode,
                                                     nullptr,
                                                     0);
                    });
                }
            }
            return true;
        });
    if (loop_->isInLoopThread())
    {
        if (disableKickoff)
        {
            auto entry = kickoffEntry_.lock();
            if (entry)
            {
                entry->reset();
                kickoffEntry_.reset();
            }
            idleTimeoutBackup_ = idleTimeout_;
            idleTimeout_ = 0;
        }

        writeBufferList_.push_back(asyncStreamNode);
    }
    else
    {
        loop_->queueInLoop([this,
                            thisPtr = shared_from_this(),
                            node = std::move(asyncStreamNode),
                            disableKickoff]() mutable {
            if (disableKickoff)
            {
                auto entry = kickoffEntry_.lock();
                if (entry)
                {
                    entry->reset();
                    kickoffEntry_.reset();
                }
                idleTimeoutBackup_ = idleTimeout_;
                idleTimeout_ = 0;
            }

            if (thisPtr->writeBufferList_.empty() && node->remainingBytes() > 0)
            {
                auto n = thisPtr->sendNodeInLoop(node);
                if (n >= 0 && (node->remainingBytes() > 0 || node->available()))
                    thisPtr->writeBufferList_.push_back(std::move(node));
            }
            else
            {
                thisPtr->writeBufferList_.push_back(std::move(node));
            }
        });
    }
    return asyncStream;
}
void TcpConnectionImpl::sendAsyncDataInLoop(const BufferNodePtr &node,
                                            const char *data,
                                            size_t len)
{
    loop_->assertInLoopThread();
    if (data)
    {
        if (len > 0)
        {
            if (!writeBufferList_.empty() && node == writeBufferList_.front() &&
                node->remainingBytes() == 0)
            {
                auto nWritten = writeInLoop(data, len);
                if (nWritten < 0)
                {
                    LOG_TRACE << "write error";
                    return;
                }
                if (static_cast<size_t>(nWritten) < len)
                {
                    node->append(data + nWritten, len - nWritten);
                }
            }
            else
            {
                node->append(data, len);
            }
        }
    }
    else
    {
        // stream is closed
        node->done();
        if (!writeBufferList_.empty() && node == writeBufferList_.front() &&
            !ioChannelPtr_->isWriting())
            ioChannelPtr_->enableWriting();

        if (idleTimeoutBackup_ > 0)
        {
            auto timingWheel = timingWheelWeakPtr_.lock();
            if (timingWheel)
            {
                auto entry = std::make_shared<KickoffEntry>(shared_from_this());
                kickoffEntry_ = entry;
                idleTimeout_ = idleTimeoutBackup_;
                idleTimeoutBackup_ = 0;
                timingWheel->insertEntry(idleTimeout_, std::move(entry));
            }
        }
    }
}
