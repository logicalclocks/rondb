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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_WORKER_POOL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_WORKER_POOL_HPP_

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include <ndb_types.h>
#include <NdbTick.h>
#include "NdbThread.h"

/*
 * RonSQL worker pool (RONDB-1124, m3_run6_plan.md B2).
 *
 * A /ronsql statement used to run on the drogon IO loop that received it.
 * On Linux every IO loop has its own SO_REUSEPORT listener, so the kernel
 * decides which connections share a loop, and a statement that runs for
 * milliseconds or seconds stalled every other request of its loop -
 * pk-reads included - outside any server-side timing (census run 6: RonSQL
 * T=8 p99 +15-50 % in about 40 % of the cases).
 *
 * The /ronsql controller now parses, validates and authorizes a request on
 * its IO loop and submits it here.  A worker executes it with its own Ndb
 * object (a thread index of its own in RDRSRonDBConnectionPool) and calls
 * the drogon callback, which queues the send back onto the connection's
 * loop.  The pool is also this server's admission limit for RonSQL: at
 * most NumThreads statements run at once and at most MaxQueuedRequests
 * wait; a request beyond that fails at once with 503 (temporary).
 */
class RonSQLWorkerPool {
 public:
  class Job {
   public:
    virtual ~Job() = default;
    /*
     * Execute the job on a worker.  worker_no counts from 1 (0 is kept for
     * "ran on the IO loop"), ndb_thread_index is the worker's index in
     * RDRSRonDBConnectionPool and queue_us the time the job waited for a
     * worker.  The job is destroyed right after run() returns.
     */
    virtual void run(Uint32 worker_no,
                     Uint32 ndb_thread_index,
                     Uint64 queue_us) = 0;
    /*
     * The pool stopped before the job ran: answer the client (503).
     */
    virtual void cancel() = 0;

   private:
    friend class RonSQLWorkerPool;
    NDB_TICKS m_enqueued;
  };

  enum class SubmitResult {
    OK,          // queued; the pool owns the job
    QUEUE_FULL,  // MaxQueuedRequests jobs are waiting
    STOPPED      // the pool is stopping (server shutdown)
  };

  RonSQLWorkerPool(Uint32 num_workers,
                   Uint32 max_queued,
                   Uint32 first_ndb_thread_index,
                   Uint32 stack_size);
  ~RonSQLWorkerPool();
  RonSQLWorkerPool(const RonSQLWorkerPool &) = delete;
  RonSQLWorkerPool &operator=(const RonSQLWorkerPool &) = delete;

  /* Start the worker threads; false if a thread could not be created. */
  bool start();

  /*
   * Queue a job.  On OK the pool takes ownership and `job` is left empty;
   * otherwise `job` is untouched and the caller answers the client.
   */
  SubmitResult submit(std::unique_ptr<Job> &job);

  /*
   * Stop accepting jobs, cancel the queued ones and wait for the running
   * ones to finish.  Idempotent, but not to be called concurrently with
   * itself.  Call it while the drogon IO loops still run, so that the
   * responses of the finishing and cancelled jobs are sent.
   */
  void stop();

  Uint32 max_queued() const { return m_max_queued; }

 private:
  struct WorkerArg {
    RonSQLWorkerPool *pool;
    Uint32 worker_no;
  };
  static void *worker_main(void *arg);
  void worker_loop(Uint32 worker_no);

  const Uint32 m_num_workers;
  const Uint32 m_max_queued;
  const Uint32 m_first_ndb_thread_index;
  const Uint32 m_stack_size;

  std::mutex m_mutex;
  std::condition_variable m_cond;
  std::deque<std::unique_ptr<Job>> m_queue;  // protected by m_mutex
  bool m_stopping = false;                   // protected by m_mutex

  std::vector<WorkerArg> m_args;
  std::vector<NdbThread *> m_threads;
};

/* Created in main.cc when RonSQL.NumThreads > 0, otherwise nullptr and
 * /ronsql statements execute on the IO loop that received them. */
extern RonSQLWorkerPool *g_ronsql_worker_pool;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_WORKER_POOL_HPP_
