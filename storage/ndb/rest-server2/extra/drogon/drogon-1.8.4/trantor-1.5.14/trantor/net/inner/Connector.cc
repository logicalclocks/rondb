/**
 *
 *  @file Connector.cc
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

#include "Connector.h"
#include "Channel.h"
#include "Socket.h"

using namespace trantor;

Connector::Connector(EventLoop *loop, const InetAddress &addr, bool retry)
    : loop_(loop), serverAddr_(addr), retry_(retry)
{
}
Connector::Connector(EventLoop *loop, InetAddress &&addr, bool retry)
    : loop_(loop), serverAddr_(std::move(addr)), retry_(retry)
{
}

Connector::~Connector()
{
    if (socketHanded_ == false && fd_ != -1)
    {
#ifndef _WIN32
        ::close(fd_);
#else
        closesocket(fd_);
#endif
    }
}

void Connector::start()
{
    connect_ = true;
    loop_->runInLoop([this]() { startInLoop(); });
}
void Connector::restart()
{
}
void Connector::stop()
{
    status_ = Status::Disconnected;
    if (loop_->isInLoopThread())
    {
        removeAndResetChannel();
    }
    else
    {
        loop_->queueInLoop([thisPtr = shared_from_this()]() {
            thisPtr->removeAndResetChannel();
        });
    }
}

void Connector::startInLoop()
{
    loop_->assertInLoopThread();
    assert(status_ == Status::Disconnected);
    if (connect_)
    {
        connect();
    }
    else
    {
        LOG_TRACE << "do not connect";
    }
}
void Connector::connect()
{
    socketHanded_ = false;
    fd_ = Socket::createNonblockingSocketOrDie(serverAddr_.family());
    if (sockOptCallback_)
        sockOptCallback_(fd_);
    errno = 0;
    int ret = Socket::connect(fd_, serverAddr_);
    int savedErrno = (ret == 0) ? 0 : errno;
    switch (savedErrno)
    {
        case 0:
        case EINPROGRESS:
        case EINTR:
        case EISCONN:
            LOG_TRACE << "connecting";
            connecting(fd_);
            break;

        case EAGAIN:
        case EADDRINUSE:
        case EADDRNOTAVAIL:
        case ECONNREFUSED:
        case ENETUNREACH:
            if (retry_)
            {
                retry(fd_);
            }
            break;

        case EACCES:
        case EPERM:
        case EAFNOSUPPORT:
        case EALREADY:
        case EBADF:
        case EFAULT:
        case ENOTSOCK:
            LOG_SYSERR << "connect error in Connector::startInLoop "
                       << savedErrno;
            socketHanded_ = true;
#ifndef _WIN32
            ::close(fd_);
#else
            closesocket(fd_);
#endif
            if (errorCallback_)
                errorCallback_();
            break;

        default:
            LOG_SYSERR << "Unexpected error in Connector::startInLoop "
                       << savedErrno;
            socketHanded_ = true;
#ifndef _WIN32
            ::close(fd_);
#else
            closesocket(fd_);
#endif
            if (errorCallback_)
                errorCallback_();
            break;
    }
}

void Connector::connecting(int sockfd)
{
    status_ = Status::Connecting;
    assert(!channelPtr_);
    channelPtr_.reset(new Channel(loop_, sockfd));
    channelPtr_->setWriteCallback(
        std::bind(&Connector::handleWrite, shared_from_this()));
    channelPtr_->setErrorCallback(
        std::bind(&Connector::handleError, shared_from_this()));
    channelPtr_->setCloseCallback(
        std::bind(&Connector::handleError, shared_from_this()));
    LOG_TRACE << "connecting:" << sockfd;
    channelPtr_->enableWriting();
}

int Connector::removeAndResetChannel()
{
    if (!channelPtr_)
    {
        return -1;
    }
    channelPtr_->disableAll();
    channelPtr_->remove();
    int sockfd = channelPtr_->fd();
    // Can't reset channel_ here, because we are inside Channel::handleEvent
    loop_->queueInLoop([channelPtr = channelPtr_]() {});
    channelPtr_.reset();
    return sockfd;
}

void Connector::handleWrite()
{
    socketHanded_ = true;
    if (status_ == Status::Connecting)
    {
        int sockfd = removeAndResetChannel();
        int err = Socket::getSocketError(sockfd);
        if (err)
        {
            LOG_WARN << "Connector::handleWrite - SO_ERROR = " << err << " "
                     << strerror_tl(err);
            if (retry_)
            {
                retry(sockfd);
            }
            else
            {
                socketHanded_ = true;
#ifndef _WIN32
                ::close(sockfd);
#else
                closesocket(sockfd);
#endif
            }
            if (errorCallback_)
            {
                errorCallback_();
            }
        }
        else if (Socket::isSelfConnect(sockfd))
        {
            LOG_WARN << "Connector::handleWrite - Self connect";
            if (retry_)
            {
                retry(sockfd);
            }
            else
            {
                socketHanded_ = true;
#ifndef _WIN32
                ::close(sockfd);
#else
                closesocket(sockfd);
#endif
            }
            if (errorCallback_)
            {
                errorCallback_();
            }
        }
        else
        {
            status_ = Status::Connected;
            if (connect_)
            {
                newConnectionCallback_(sockfd);
            }
            else
            {
                socketHanded_ = true;
#ifndef _WIN32
                ::close(sockfd);
#else
                closesocket(sockfd);
#endif
            }
        }
    }
    else
    {
        // has been stopped
        assert(status_ == Status::Disconnected);
    }
}

void Connector::handleError()
{
    socketHanded_ = true;
    if (status_ == Status::Connecting)
    {
        status_ = Status::Disconnected;
        int sockfd = removeAndResetChannel();
        int err = Socket::getSocketError(sockfd);
        LOG_TRACE << "SO_ERROR = " << err << " " << strerror_tl(err);
        if (retry_)
        {
            retry(sockfd);
        }
        else
        {
#ifndef _WIN32
            ::close(sockfd);
#else
            closesocket(sockfd);
#endif
        }
        if (errorCallback_)
        {
            errorCallback_();
        }
    }
}

void Connector::retry(int sockfd)
{
    assert(retry_);
#ifndef _WIN32
    ::close(sockfd);
#else
    closesocket(sockfd);
#endif
    status_ = Status::Disconnected;
    if (connect_)
    {
        LOG_INFO << "Connector::retry - Retry connecting to "
                 << serverAddr_.toIpPort() << " in " << retryInterval_
                 << " milliseconds. ";
        loop_->runAfter(retryInterval_ / 1000.0,
                        std::bind(&Connector::startInLoop, shared_from_this()));
        retryInterval_ = retryInterval_ * 2;
        if (retryInterval_ > maxRetryInterval_)
            retryInterval_ = maxRetryInterval_;
    }
    else
    {
        LOG_TRACE << "do not connect";
    }
}
