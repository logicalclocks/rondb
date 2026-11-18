/**
 *
 *  Poller.cc
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

#include "Poller.h"
#ifdef __linux__
#include "poller/EpollPoller.h"
#elif defined _WIN32
#include "Wepoll.h"
#include "poller/EpollPoller.h"
#elif defined __FreeBSD__ || defined __OpenBSD__ || defined __APPLE__
#include "poller/KQueue.h"
#else
#include "poller/PollPoller.h"
#endif
using namespace trantor;
Poller *Poller::newPoller(EventLoop *loop)
{
#if defined __linux__ || defined _WIN32
    return new EpollPoller(loop);
#elif defined __FreeBSD__ || defined __OpenBSD__ || defined __APPLE__
    return new KQueue(loop);
#else
    return new PollPoller(loop);
#endif
}
