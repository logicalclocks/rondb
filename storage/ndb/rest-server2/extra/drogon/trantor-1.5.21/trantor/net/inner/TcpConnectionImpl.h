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
#include <trantor/net/inner/BufferNode.h>
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
class TcpConnectionImpl : public TcpConnection,
                          public NonCopyable,
                          public std::enable_shared_from_this<TcpConnectionImpl>
{
    friend class TcpServer;
    friend class TcpClient;

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
    ~TcpConnectionImpl() override;
    void send(const char *msg, size_t len) override;
    void send(const void *msg, size_t len) override;
    void send(const std::string &msg) override;
    void send(std::string &&msg) override;
    void send(const MsgBuffer &buffer) override;
    void send(MsgBuffer &&buffer) override;
    void send(const std::shared_ptr<std::string> &msgPtr) override;
    void send(const std::shared_ptr<MsgBuffer> &msgPtr) override;
    void sendFile(const char *fileName,
                  long long offset,
                  long long length) override;
    void sendFile(const wchar_t *fileName,
                  long long offset,
                  long long length) override;
    void sendStream(
        std::function<std::size_t(char *, std::size_t)> callback) override;

    const InetAddress &localAddr() const override
    {
        return localAddr_;
    }
    const InetAddress &peerAddr() const override
    {
        return peerAddr_;
    }

    bool connected() const override
    {
        return status_ == ConnStatus::Connected;
    }
    bool disconnected() const override
    {
        return status_ == ConnStatus::Disconnected;
    }

    // virtual MsgBuffer* getSendBuffer() override{ return  &writeBuffer_;}
    // virtual MsgBuffer *getRecvBuffer() override
    // {
    //     return &readBuffer_;
    // }
    // set callbacks
    void setHighWaterMarkCallback(const HighWaterMarkCallback &cb,
                                  size_t markLen) override
    {
        highWaterMarkCallback_ = cb;
        highWaterMarkLen_ = markLen;
    }

    void keepAlive() override
    {
        idleTimeout_ = 0;
        auto entry = kickoffEntry_.lock();
        if (entry)
        {
            entry->reset();
        }
    }
    bool isKeepAlive() override
    {
        return idleTimeout_ == 0;
    }
    void setTcpNoDelay(bool on) override;
    void shutdown() override;
    void forceClose() override;
    EventLoop *getLoop() override
    {
        return loop_;
    }

    size_t bytesSent() const override
    {
        return bytesSent_;
    }
    size_t bytesReceived() const override
    {
        return bytesReceived_;
    }

    bool isSSLConnection() const override
    {
        return tlsProviderPtr_ != nullptr;
    }
    void connectEstablished() override;
    void connectDestroyed() override;

    MsgBuffer *getRecvBuffer() override
    {
        if (tlsProviderPtr_)
            return &tlsProviderPtr_->getRecvBuffer();
        return &readBuffer_;
    }

    std::string applicationProtocol() const override
    {
        if (tlsProviderPtr_)
            return tlsProviderPtr_->applicationProtocol();
        return "";
    }

    CertificatePtr peerCertificate() const override
    {
        if (tlsProviderPtr_)
            return tlsProviderPtr_->peerCertificate();
        return nullptr;
    }

    std::string sniName() const override
    {
        if (tlsProviderPtr_)
            return tlsProviderPtr_->sniName();
        return "";
    }

    void startEncryption(
        TLSPolicyPtr policy,
        bool isServer,
        std::function<void(const TcpConnectionPtr &)> upgradeCallback) override;
    AsyncStreamPtr sendAsyncStream(bool disableKickoff) override;

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
    size_t idleTimeoutBackup_{0};
    Date lastTimingWheelUpdateTime_;
    void extendLife();
    void sendFile(BufferNodePtr &&fileNode);

  protected:
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
    void sendAsyncDataInLoop(const BufferNodePtr &node,
                             const char *data,
                             size_t len);
    // -1: error, 0: EAGAIN, >0: bytes sent
    ssize_t sendNodeInLoop(const BufferNodePtr &node);
#ifndef _WIN32
    void sendInLoop(const void *buffer, size_t length);
    ssize_t writeRaw(const void *buffer, size_t length);
    ssize_t writeInLoop(const void *buffer, size_t length);
#else
    void sendInLoop(const char *buffer, size_t length);
    // -1: error, 0: EAGAIN, >0: bytes sent
    ssize_t writeRaw(const char *buffer, size_t length);
    // -1: error, 0: EAGAIN, >0: bytes sent
    ssize_t writeInLoop(const char *buffer, size_t length);
#endif
    size_t highWaterMarkLen_{0};
    std::string name_;

    size_t bytesSent_{0};
    size_t bytesReceived_{0};

    // std::unique_ptr<std::vector<char>> fileBufferPtr_;
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
