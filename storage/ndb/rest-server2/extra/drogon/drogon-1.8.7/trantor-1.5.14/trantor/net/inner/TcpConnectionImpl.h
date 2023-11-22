/**
 *
 *  @file TcpConnectionImpl.h
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

#pragma once

#include <trantor/net/TcpConnection.h>
#include <trantor/utils/TimingWheel.h>
#include <trantor/net/inner/TLSProvider.h>
#include <list>
#include <mutex>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <thread>
#include <array>

namespace trantor
{
class Channel;
class Socket;
class TcpServer;
void removeConnection(EventLoop *loop, const TcpConnectionPtr &conn);
class TcpConnectionImpl : public TcpConnection,
                          public NonCopyable,
                          public std::enable_shared_from_this<TcpConnectionImpl>
{
    friend class TcpServer;
    friend class TcpClient;
    friend void trantor::removeConnection(EventLoop *loop,
                                          const TcpConnectionPtr &conn);

  public:
    class KickoffEntry
    {
      public:
        explicit KickoffEntry(const std::weak_ptr<TcpConnection> &conn)
            : conn_(conn)
        {
        }
        void reset()
        {
            conn_.reset();
        }
        ~KickoffEntry()
        {
            auto conn = conn_.lock();
            if (conn)
            {
                conn->forceClose();
            }
        }

      private:
        std::weak_ptr<TcpConnection> conn_;
    };

    TcpConnectionImpl(EventLoop *loop,
                      int socketfd,
                      const InetAddress &localAddr,
                      const InetAddress &peerAddr,
                      TLSPolicyPtr policy = nullptr,
                      SSLContextPtr ctx = nullptr);
    virtual ~TcpConnectionImpl();
    virtual void send(const char *msg, size_t len) override;
    virtual void send(const void *msg, size_t len) override;
    virtual void send(const std::string &msg) override;
    virtual void send(std::string &&msg) override;
    virtual void send(const MsgBuffer &buffer) override;
    virtual void send(MsgBuffer &&buffer) override;
    virtual void send(const std::shared_ptr<std::string> &msgPtr) override;
    virtual void send(const std::shared_ptr<MsgBuffer> &msgPtr) override;
    virtual void sendFile(const char *fileName,
                          size_t offset = 0,
                          size_t length = 0) override;
    virtual void sendFile(const wchar_t *fileName,
                          size_t offset = 0,
                          size_t length = 0) override;
    virtual void sendStream(
        std::function<std::size_t(char *, std::size_t)> callback) override;

    virtual const InetAddress &localAddr() const override
    {
        return localAddr_;
    }
    virtual const InetAddress &peerAddr() const override
    {
        return peerAddr_;
    }

    virtual bool connected() const override
    {
        return status_ == ConnStatus::Connected;
    }
    virtual bool disconnected() const override
    {
        return status_ == ConnStatus::Disconnected;
    }

    // virtual MsgBuffer* getSendBuffer() override{ return  &writeBuffer_;}
    // virtual MsgBuffer *getRecvBuffer() override
    // {
    //     return &readBuffer_;
    // }
    // set callbacks
    virtual void setHighWaterMarkCallback(const HighWaterMarkCallback &cb,
                                          size_t markLen) override
    {
        highWaterMarkCallback_ = cb;
        highWaterMarkLen_ = markLen;
    }

    virtual void keepAlive() override
    {
        idleTimeout_ = 0;
        auto entry = kickoffEntry_.lock();
        if (entry)
        {
            entry->reset();
        }
    }
    virtual bool isKeepAlive() override
    {
        return idleTimeout_ == 0;
    }
    virtual void setTcpNoDelay(bool on) override;
    virtual void shutdown() override;
    virtual void forceClose() override;
    virtual EventLoop *getLoop() override
    {
        return loop_;
    }

    virtual size_t bytesSent() const override
    {
        return bytesSent_;
    }
    virtual size_t bytesReceived() const override
    {
        return bytesReceived_;
    }

    virtual bool isSSLConnection() const override
    {
        return tlsProviderPtr_ != nullptr;
    }
    virtual void connectEstablished() override;
    virtual void connectDestroyed() override;

    virtual MsgBuffer *getRecvBuffer() override
    {
        if (tlsProviderPtr_)
            return &tlsProviderPtr_->getRecvBuffer();
        return &readBuffer_;
    }

    virtual std::string applicationProtocol() const override
    {
        if (tlsProviderPtr_)
            return tlsProviderPtr_->applicationProtocol();
        return "";
    }

    virtual CertificatePtr peerCertificate() const override
    {
        if (tlsProviderPtr_)
            return tlsProviderPtr_->peerCertificate();
        return nullptr;
    }

    virtual std::string sniName() const override
    {
        if (tlsProviderPtr_)
            return tlsProviderPtr_->sniName();
        return "";
    }

    virtual void startEncryption(TLSPolicyPtr policy,
                                 bool isServer,
                                 std::function<void(const TcpConnectionPtr &)>
                                     upgradeCallback = nullptr) override;

    void enableKickingOff(
        size_t timeout,
        const std::shared_ptr<TimingWheel> &timingWheel) override
    {
        assert(timingWheel);
        assert(timingWheel->getLoop() == loop_);
        assert(timeout > 0);
        auto entry = std::make_shared<KickoffEntry>(shared_from_this());
        kickoffEntry_ = entry;
        timingWheelWeakPtr_ = timingWheel;
        idleTimeout_ = timeout;
        timingWheel->insertEntry(timeout, entry);
    }

  private:
    /// Internal use only.

    std::weak_ptr<KickoffEntry> kickoffEntry_;
    std::weak_ptr<TimingWheel> timingWheelWeakPtr_;
    size_t idleTimeout_{0};
    Date lastTimingWheelUpdateTime_;
    void extendLife();
#ifndef _WIN32
    void sendFile(int sfd, size_t offset = 0, size_t length = 0);
#else
    void sendFile(FILE *fp, size_t offset = 0, size_t length = 0);
#endif

  protected:
    struct BufferNode
    {
        // sendFile() specific
#ifndef _WIN32
        int sendFd_{-1};
        off_t offset_{0};
#else
        FILE *sendFp_{nullptr};
        long long offset_{0};
#endif
        ssize_t fileBytesToSend_{0};
        // sendStream() specific
        std::function<std::size_t(char *, std::size_t)> streamCallback_;
#ifndef NDEBUG  // defined by CMake for release build
        std::size_t nDataWritten_{0};
#endif
        // generic
        std::shared_ptr<MsgBuffer> msgBuffer_;
        bool isFile() const
        {
            if (streamCallback_)
                return true;
#ifndef _WIN32
            if (sendFd_ >= 0)
                return true;
#else
            if (sendFp_)
                return true;
#endif
            return false;
        }
        ~BufferNode()
        {
#ifndef _WIN32
            if (sendFd_ >= 0)
                close(sendFd_);
#else
            if (sendFp_)
                fclose(sendFp_);
#endif
            if (streamCallback_)
                streamCallback_(nullptr, 0);  // cleanup callback internals
        }
        bool closeConnection_ = false;
    };
    using BufferNodePtr = std::shared_ptr<BufferNode>;
    enum class ConnStatus
    {
        Disconnected,
        Connecting,
        Connected,
        Disconnecting
    };
    EventLoop *loop_;
    std::unique_ptr<Channel> ioChannelPtr_;
    std::unique_ptr<Socket> socketPtr_;
    MsgBuffer readBuffer_;
    std::list<BufferNodePtr> writeBufferList_;
    void readCallback();
    void writeCallback();
    InetAddress localAddr_, peerAddr_;
    ConnStatus status_{ConnStatus::Connecting};
    void handleClose();
    void handleError();
    // virtual void sendInLoop(const std::string &msg);

    void sendFileInLoop(const BufferNodePtr &file);
#ifndef _WIN32
    void sendInLoop(const void *buffer, size_t length);
    ssize_t writeRaw(const void *buffer, size_t length);
    ssize_t writeInLoop(const void *buffer, size_t length);
#else
    void sendInLoop(const char *buffer, size_t length);
    ssize_t writeRaw(const char *buffer, size_t length);
    ssize_t writeInLoop(const char *buffer, size_t length);
#endif
    size_t highWaterMarkLen_;
    std::string name_;

    uint64_t sendNum_{0};
    std::mutex sendNumMutex_;

    size_t bytesSent_{0};
    size_t bytesReceived_{0};

    std::unique_ptr<std::vector<char>> fileBufferPtr_;
    std::shared_ptr<TLSProvider> tlsProviderPtr_;
    std::function<void(const TcpConnectionPtr &)> upgradeCallback_;

    bool closeOnEmpty_{false};

    static void onSslError(TcpConnection *self, SSLError err);
    static void onHandshakeFinished(TcpConnection *self);
    static void onSslMessage(TcpConnection *self, MsgBuffer *buffer);
    static ssize_t onSslWrite(TcpConnection *self,
                              const void *data,
                              size_t len);
    static void onSslCloseAlert(TcpConnection *self);
};

using TcpConnectionImplPtr = std::shared_ptr<TcpConnectionImpl>;

}  // namespace trantor
