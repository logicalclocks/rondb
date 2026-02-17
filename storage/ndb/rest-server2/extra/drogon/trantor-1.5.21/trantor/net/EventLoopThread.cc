/**
 *
 *  @file EventLoopThread.cc
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

#include <trantor/net/EventLoopThread.h>
#include <trantor/utils/Logger.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif

using namespace trantor;

void *EventLoopThread::threadFunc(void *arg)
{
    auto *self = static_cast<EventLoopThread *>(arg);
    self->loopFuncs();
    return nullptr;
}

EventLoopThread::EventLoopThread(const std::string &threadName,
                                 size_t stackSize)
    : loop_(nullptr),
      loopThreadName_(threadName),
      stackSize_(stackSize)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    if (stackSize_ > 0)
    {
        pthread_attr_setstacksize(&attr, stackSize_);
    }
    pthread_create(&threadId_, &attr, &EventLoopThread::threadFunc, this);
    pthread_attr_destroy(&attr);
    threadStarted_ = true;

    auto f = promiseForLoopPointer_.get_future();
    loop_ = f.get();
}

EventLoopThread::~EventLoopThread()
{
    run();
    std::shared_ptr<EventLoop> loop;
    {
        std::unique_lock<std::mutex> lk(loopMutex_);
        loop = loop_;
    }
    if (loop)
    {
        loop->quit();
    }
    if (threadStarted_ && !threadJoined_)
    {
        pthread_join(threadId_, nullptr);
        threadJoined_ = true;
    }
}

void EventLoopThread::wait()
{
    if (threadStarted_ && !threadJoined_)
    {
        pthread_join(threadId_, nullptr);
        threadJoined_ = true;
    }
}

void EventLoopThread::loopFuncs()
{
#ifdef __linux__
    ::prctl(PR_SET_NAME, loopThreadName_.c_str());
#endif
    thread_local static std::shared_ptr<EventLoop> loop =
        std::make_shared<EventLoop>();
    loop->queueInLoop([this]() { promiseForLoop_.set_value(1); });
    promiseForLoopPointer_.set_value(loop);
    auto f = promiseForRun_.get_future();
    (void)f.get();
    loop->loop();
    {
        std::unique_lock<std::mutex> lk(loopMutex_);
        loop_ = nullptr;
    }
}

void EventLoopThread::run()
{
    std::call_once(once_, [this]() {
        auto f = promiseForLoop_.get_future();
        promiseForRun_.set_value(1);
        // Make sure the event loop loops before returning.
        (void)f.get();
    });
}
