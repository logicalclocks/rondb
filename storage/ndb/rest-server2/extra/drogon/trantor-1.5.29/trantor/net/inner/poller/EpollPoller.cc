/**
 *
 *  EpollPoller.cc
 *  An Tao
 *
 *  Public header file in trantor lib.
 *
 *  Copyright 2018, An Tao.  All rights reserved.
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the License file.
 *
 *
 */

#include <trantor/utils/Logger.h>
#include "Channel.h"
#include "EpollPoller.h"
#ifdef __linux__
#include <poll.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <assert.h>
#include <strings.h>
#include <iostream>
#if (defined(__sparc__) || defined(__sparc))
#define TRANTOR_RDHUP 0x2800 /* EPOLLRDHUP | POLLRDHUP */
#endif
#elif defined _WIN32
#include "Wepoll.h"
#include <assert.h>
#include <iostream>
#include <winsock2.h>
#include <fcntl.h>
#define EPOLL_CLOEXEC _O_NOINHERIT
#endif
namespace trantor
{
#if defined __linux__ || defined _WIN32

#if defined __linux__
static_assert(EPOLLIN == POLLIN, "EPOLLIN != POLLIN");
static_assert(EPOLLPRI == POLLPRI, "EPOLLPRI != POLLPRI");
static_assert(EPOLLOUT == POLLOUT, "EPOLLOUT != POLLOUT");
#if (defined(__sparc__) || defined(__sparc))
static_assert(EPOLLRDHUP & TRANTOR_RDHUP,
              "EPOLLRDHUP not included in TRANTOR_RDHUP");
#else
static_assert(EPOLLRDHUP == POLLRDHUP, "EPOLLRDHUP != POLLRDHUP");
#endif
static_assert(EPOLLERR == POLLERR, "EPOLLERR != POLLERR");
static_assert(EPOLLHUP == POLLHUP, "EPOLLHUP != POLLHUP");
#endif

namespace
{
const int kNew = -1;
const int kAdded = 1;
const int kDeleted = 2;
}  // namespace

EpollPoller::EpollPoller(EventLoop *loop)
    : Poller(loop),
#ifdef _WIN32
      // wepoll does not support flags
      epollfd_(::epoll_create1(0)),
#else
      epollfd_(::epoll_create1(EPOLL_CLOEXEC)),
#endif
      events_(kInitEventListSize)
{
}
EpollPoller::~EpollPoller()
{
#ifdef _WIN32
    epoll_close(epollfd_);
#else
    close(epollfd_);
#endif
}
#ifdef _WIN32
void EpollPoller::postEvent(uint64_t event)
{
    epoll_post_signal(epollfd_, event);
}
#endif
void EpollPoller::poll(int timeoutMs, ChannelList *activeChannels)
{
    int numEvents = ::epoll_wait(epollfd_,
                                 &*events_.begin(),
                                 static_cast<int>(events_.size()),
                                 timeoutMs);
    int savedErrno = errno;
    // Timestamp now(Timestamp::now());
    if (numEvents > 0)
    {
        // LOG_TRACE << numEvents << " events happened";
        fillActiveChannels(numEvents, activeChannels);
        if (static_cast<size_t>(numEvents) == events_.size())
        {
            events_.resize(events_.size() * 2);
        }
    }
    else if (numEvents == 0)
    {
        // std::cout << "nothing happened" << std::endl;
    }
    else
    {
        // error happens, log uncommon ones
        if (savedErrno != EINTR)
        {
            errno = savedErrno;
            LOG_SYSERR << "EPollEpollPoller::poll()";
        }
    }
    return;
}
void EpollPoller::fillActiveChannels(int numEvents,
                                     ChannelList *activeChannels) const
{
    assert(static_cast<size_t>(numEvents) <= events_.size());
    for (int i = 0; i < numEvents; ++i)
    {
#ifdef _WIN32
        if (events_[i].events == EPOLLEVENT)
        {
            eventCallback_(events_[i].data.u64);
            continue;
        }
#endif
        Channel *channel = static_cast<Channel *>(events_[i].data.ptr);
#ifndef NDEBUG
        int fd = channel->fd();
        ChannelMap::const_iterator it = channels_.find(fd);
        assert(it != channels_.end());
        assert(it->second == channel);
#endif
        int revents = events_[i].events;
#if defined(__linux__) && (defined(__sparc__) || defined(__sparc))
        if (revents & EPOLLRDHUP)
        {
            revents &= ~EPOLLRDHUP;
            revents |= POLLRDHUP;
        }
#endif
        channel->setRevents(revents);
        activeChannels->push_back(channel);
    }
    // LOG_TRACE<<"active Channels num:"<<activeChannels->size();
}
void EpollPoller::updateChannel(Channel *channel)
{
    assertInLoopThread();
    assert(channel->fd() >= 0);

    const int index = channel->index();
    // LOG_TRACE << "fd = " << channel->fd()
    //  << " events = " << channel->events() << " index = " << index;
    if (index == kNew || index == kDeleted)
    {
// a new one, add with EPOLL_CTL_ADD
#ifndef NDEBUG
        int fd = channel->fd();
        if (index == kNew)
        {
            assert(channels_.find(fd) == channels_.end());
            channels_[fd] = channel;
        }
        else
        {  // index == kDeleted
            assert(channels_.find(fd) != channels_.end());
            assert(channels_[fd] == channel);
        }
#endif
        channel->setIndex(kAdded);
        update(EPOLL_CTL_ADD, channel);
    }
    else
    {
// update existing one with EPOLL_CTL_MOD/DEL
#ifndef NDEBUG
        int fd = channel->fd();
        (void)fd;
        assert(channels_.find(fd) != channels_.end());
        assert(channels_[fd] == channel);
#endif
        assert(index == kAdded);
        if (channel->isNoneEvent())
        {
            update(EPOLL_CTL_DEL, channel);
            channel->setIndex(kDeleted);
        }
        else
        {
            update(EPOLL_CTL_MOD, channel);
        }
    }
}
void EpollPoller::removeChannel(Channel *channel)
{
    EpollPoller::assertInLoopThread();
#ifndef NDEBUG
    int fd = channel->fd();
    assert(channels_.find(fd) != channels_.end());
    assert(channels_[fd] == channel);
    size_t n = channels_.erase(fd);
    (void)n;
    assert(n == 1);
#endif
    assert(channel->isNoneEvent());
    int index = channel->index();
    assert(index == kAdded || index == kDeleted);
    if (index == kAdded)
    {
        update(EPOLL_CTL_DEL, channel);
    }
    channel->setIndex(kNew);
}
void EpollPoller::update(int operation, Channel *channel)
{
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = channel->events();
#if defined(__linux__) && (defined(__sparc__) || defined(__sparc))
    if (event.events & TRANTOR_RDHUP)
    {
        event.events &= ~TRANTOR_RDHUP;
        event.events |= EPOLLRDHUP;
    }
#endif
    event.data.ptr = channel;
    int fd = channel->fd();
    if (::epoll_ctl(epollfd_, operation, fd, &event) < 0)
    {
        if (operation == EPOLL_CTL_DEL)
        {
            // LOG_SYSERR << "epoll_ctl op =" << operationToString(operation) <<
            // " fd =" << fd;
        }
        else
        {
            //  LOG_SYSFATAL << "epoll_ctl op =" << operationToString(operation)
            //  << " fd =" << fd;
        }
    }
}
#else
EpollPoller::EpollPoller(EventLoop *loop) : Poller(loop)
{
    assert(false);
}
EpollPoller::~EpollPoller()
{
}
void EpollPoller::poll(int, ChannelList *)
{
}
void EpollPoller::updateChannel(Channel *)
{
}
void EpollPoller::removeChannel(Channel *)
{
}

#endif
}  // namespace trantor
