/**
 *
 *  PollPoller.h
 *  Martin Chang
 *
 *  Copyright 2021, An Tao.  All rights reserved.
 *  https://github.com/an-tao/trantor
 *  Use of this source code is governed by a BSD-style license
 *  that can be found in the License file.
 *
 *  Trantor
 *
 */
#include "PollPoller.h"

#include <trantor/utils/Logger.h>
#include "trantor/net/Channel.h"

#include <assert.h>

using namespace trantor;

#if defined __unix__ || defined __HAIKU__

#include <errno.h>
#include <poll.h>
static std::once_flag warning_flag;

PollPoller::PollPoller(EventLoop* loop) : Poller(loop)
{
    std::call_once(warning_flag, []() {
        LOG_WARN << "Creating a PollPoller. This poller is slow and should "
                    "only be used when no other pollers are available";
    });
}

PollPoller::~PollPoller()
{
}

void PollPoller::poll(int timeoutMs, ChannelList* activeChannels)
{
    // XXX pollfds_ shouldn't change
    int numEvents = ::poll(pollfds_.data(), pollfds_.size(), timeoutMs);
    int savedErrno = errno;
    if (numEvents > 0)
    {
        // LOG_TRACE << numEvents << " events happened";
        fillActiveChannels(numEvents, activeChannels);
    }
    else if (numEvents == 0)
    {
        // LOG_TRACE << " nothing happened";
    }
    else
    {
        if (savedErrno != EINTR)
        {
            errno = savedErrno;
            LOG_SYSERR << "PollPoller::poll()";
        }
    }
}

void PollPoller::fillActiveChannels(int numEvents,
                                    ChannelList* activeChannels) const
{
    int processedEvents = 0;
    for (auto pfd : pollfds_)
    {
        if (pfd.revents > 0)
        {
            auto ch = channels_.find(pfd.fd);
            assert(ch != channels_.end());
            Channel* channel = ch->second;
            assert(channel->fd() == pfd.fd);
            channel->setRevents(pfd.revents);
            // pfd.revents = 0;
            activeChannels->push_back(channel);

            processedEvents++;
            if (processedEvents == numEvents)
                break;
        }
    }
    assert(processedEvents == numEvents);
}

void PollPoller::updateChannel(Channel* channel)
{
    Poller::assertInLoopThread();
    assert(channel->fd() >= 0);

    // LOG_TRACE << "fd = " << channel->fd() << " events = " <<
    // channel->events();
    if (channel->index() < 0)
    {
        // a new one, add to pollfds_
        assert(channels_.find(channel->fd()) == channels_.end());
        pollfd pfd;
        pfd.fd = channel->fd();
        pfd.events = static_cast<short>(channel->events());
        pfd.revents = 0;
        pollfds_.push_back(pfd);
        int idx = static_cast<int>(pollfds_.size()) - 1;
        channel->setIndex(idx);
        channels_[pfd.fd] = channel;
    }
    else
    {
        // update existing one
        assert(channels_.find(channel->fd()) != channels_.end());
        assert(channels_[channel->fd()] == channel);
        int idx = channel->index();
        assert(0 <= idx && idx < static_cast<int>(pollfds_.size()));
        pollfd& pfd = pollfds_[idx];
        assert(pfd.fd == channel->fd() || pfd.fd == -channel->fd() - 1);
        pfd.fd = channel->fd();
        pfd.events = static_cast<short>(channel->events());
        pfd.revents = 0;
        if (channel->isNoneEvent())
        {
            // ignore this pollfd
            pfd.fd = -channel->fd() - 1;
        }
    }
}

void PollPoller::removeChannel(Channel* channel)
{
    Poller::assertInLoopThread();
    // LOG_TRACE << "fd = " << channel->fd();
    assert(channels_.find(channel->fd()) != channels_.end());
    assert(channels_[channel->fd()] == channel);
    assert(channel->isNoneEvent());
    int idx = channel->index();
    assert(0 <= idx && idx < static_cast<int>(pollfds_.size()));
    const pollfd& pfd = pollfds_[idx];
    (void)pfd;
    assert(pfd.fd == -channel->fd() - 1 && pfd.events == channel->events());
    size_t n = channels_.erase(channel->fd());
    assert(n == 1);
    (void)n;
    if (size_t(idx) == pollfds_.size() - 1)
    {
        pollfds_.pop_back();
    }
    else
    {
        int channelAtEnd = pollfds_.back().fd;
        iter_swap(pollfds_.begin() + idx, pollfds_.end() - 1);
        if (channelAtEnd < 0)
        {
            channelAtEnd = -channelAtEnd - 1;
        }
        channels_[channelAtEnd]->setIndex(idx);
        pollfds_.pop_back();
    }
}
#else
PollPoller::PollPoller(EventLoop *loop) : Poller(loop)
{
    assert(false);
}
PollPoller::~PollPoller()
{
}
void PollPoller::poll(int, ChannelList *)
{
}
void PollPoller::updateChannel(Channel *)
{
}
void PollPoller::removeChannel(Channel *)
{
}
#endif
