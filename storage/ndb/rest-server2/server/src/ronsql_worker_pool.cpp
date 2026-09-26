/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "ronsql_worker_pool.hpp"

#include <cstdio>
#include <utility>

RonSQLWorkerPool *g_ronsql_worker_pool = nullptr;

RonSQLWorkerPool::RonSQLWorkerPool(Uint32 num_workers,
                                   Uint32 max_queued,
                                   Uint32 first_ndb_thread_index,
                                   Uint32 stack_size)
    : m_num_workers(num_workers),
      m_max_queued(max_queued),
      m_first_ndb_thread_index(first_ndb_thread_index),
      m_stack_size(stack_size) {}

RonSQLWorkerPool::~RonSQLWorkerPool() {
  stop();
}

bool RonSQLWorkerPool::start() {
  m_args.resize(m_num_workers);
  m_threads.reserve(m_num_workers);
  for (Uint32 i = 0; i < m_num_workers; i++) {
    m_args[i].pool = this;
    m_args[i].worker_no = i + 1;
    char name[32];
    snprintf(name, sizeof(name), "RonSQLWorker%u", i + 1);
    NdbThread *thread = NdbThread_Create(RonSQLWorkerPool::worker_main,
                                         (NDB_THREAD_ARG *)&m_args[i],
                                         m_stack_size,
                                         name,
                                         NDB_THREAD_PRIO_MEAN);
    if (thread == nullptr) {
      fprintf(stderr, "Failed to start RonSQL worker thread %u of %u\n",
              i + 1, m_num_workers);
      return false;
    }
    m_threads.push_back(thread);
  }
  return true;
}

RonSQLWorkerPool::SubmitResult
RonSQLWorkerPool::submit(std::unique_ptr<Job> &job) {
  job->m_enqueued = NdbTick_getCurrentTicks();
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    if (m_stopping) {
      return SubmitResult::STOPPED;
    }
    if (m_queue.size() >= m_max_queued) {
      return SubmitResult::QUEUE_FULL;
    }
    m_queue.push_back(std::move(job));
  }
  m_cond.notify_one();
  return SubmitResult::OK;
}

void RonSQLWorkerPool::stop() {
  std::deque<std::unique_ptr<Job>> cancelled;
  {
    std::lock_guard<std::mutex> guard(m_mutex);
    if (m_stopping) {
      return;  // stopped before (stop() is never called concurrently)
    }
    m_stopping = true;
    cancelled.swap(m_queue);
  }
  m_cond.notify_all();
  /* Answer the jobs that never ran; the workers finish the running ones
   * and then find the queue empty and exit. */
  for (auto &job : cancelled) {
    job->cancel();
  }
  cancelled.clear();
  for (NdbThread *&thread : m_threads) {
    void *status;
    NdbThread_WaitFor(thread, &status);
    NdbThread_Destroy(&thread);
  }
  m_threads.clear();
}

void *RonSQLWorkerPool::worker_main(void *arg) {
  WorkerArg *worker_arg = static_cast<WorkerArg *>(arg);
  worker_arg->pool->worker_loop(worker_arg->worker_no);
  return nullptr;
}

void RonSQLWorkerPool::worker_loop(Uint32 worker_no) {
  const Uint32 ndb_thread_index = m_first_ndb_thread_index + worker_no - 1;
  for (;;) {
    std::unique_ptr<Job> job;
    {
      std::unique_lock<std::mutex> lock(m_mutex);
      m_cond.wait(lock, [this] { return m_stopping || !m_queue.empty(); });
      if (m_queue.empty()) {
        return;  // stopping, and stop() took the queued jobs
      }
      job = std::move(m_queue.front());
      m_queue.pop_front();
    }
    const Uint64 queue_us =
        NdbTick_Elapsed(job->m_enqueued, NdbTick_getCurrentTicks()).microSec();
    job->run(worker_no, ndb_thread_index, queue_us);
  }
}
