/*
 * Copyright (C) 2026 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROBE_SERVER_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROBE_SERVER_HPP_

#include <atomic>
#include <memory>
#include <unordered_map>
#include <chrono>

namespace trantor {
class EventLoopThread;
class TcpServer;
class TcpConnection;
class MsgBuffer;
}  // namespace trantor

/* True while Drogon's event loops are running, i.e. while the main REST port
 * accepts connections. Set from registerBeginningAdvice and cleared when
 * drogon::app().run() returns (main.cc). Both probe endpoints require it for
 * a 200: the probe listener binds long before Drogon (so a port conflict
 * fails fast and "starting" stays distinguishable from "dead"), and without
 * this guard a startup or readiness probe on the probe port would pass while
 * the main port still refuses connections. Liveness is unaffected by the
 * gate: Kubernetes runs it only after the startup probe has passed, and the
 * flag is latched - a data-path stall never clears it, so a stall can never
 * fail these endpoints. */
extern std::atomic<bool> g_drogon_up;

/**
 * The dedicated probe listener: one thread serving only GET/HEAD of
 * /0.1.0/ping and /0.1.0/health, so Kubernetes probes keep being answered
 * while every Drogon worker thread is blocked in the data path (the failure
 * mode behind the fleet-wide liveness kills this replaces). Never
 * authenticates - API-key validation itself goes to RonDB and can block.
 * Both handlers are wait-free: ping touches nothing, health reads atomics
 * and the NDB API's live node count (see get_rondb_stats /
 * get_num_ready_data_nodes).
 *
 * Built directly on trantor rather than Drogon: a second Drogon listener
 * would be accepted by ALL Drogon IO loops (ListenerManager fans every
 * listener out across the pool), inheriting exactly the starvation this
 * exists to escape.
 */
class ProbeServer {
 public:
  ProbeServer();
  ~ProbeServer();
  ProbeServer(const ProbeServer &) = delete;
  ProbeServer &operator=(const ProbeServer &) = delete;

  /**
   * Bind and start the listener thread. Call once, after AllConfigs::init().
   * Returns false when the port cannot be bound or TLS setup fails; the
   * caller must treat that as fatal (a server whose configured probe
   * listener is missing must not come up looking healthy).
   */
  bool Start();

  /**
   * Stop accepting, close connections and join the listener thread.
   * Idempotent.
   */
  void Stop();

 private:
  /* All private state below is touched only on the listener's event-loop
   * thread (connection callback, recv callback, sweep timer), so none of it
   * needs locking. */
  struct ConnState {
    /* Owning reference, kept so the sweep timer can force-close the
     * connection (TcpConnection does not expose shared_from_this). Erased
     * in the disconnect callback. */
    std::shared_ptr<trantor::TcpConnection> conn;
    /* When the first byte of a not-yet-complete request arrived. The sweep
     * timer force-closes the connection when a request stays incomplete
     * past the deadline: trantor's idle-connection kickoff resets on EVERY
     * read, so a client dribbling one byte a second would otherwise hold
     * the single loop's resources forever. */
    std::chrono::steady_clock::time_point partial_since{};
    bool has_partial = false;
  };

  void onConnection(const std::shared_ptr<trantor::TcpConnection> &conn);
  void onMessage(const std::shared_ptr<trantor::TcpConnection> &conn,
                 trantor::MsgBuffer *buffer);
  void sweepStalledRequests();

  std::unique_ptr<trantor::EventLoopThread> m_loop_thread;
  std::unique_ptr<trantor::TcpServer> m_server;
  std::unordered_map<trantor::TcpConnection *, ConnState> m_conns;
  bool m_started = false;
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_PROBE_SERVER_HPP_
