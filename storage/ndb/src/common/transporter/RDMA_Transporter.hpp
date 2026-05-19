/*
   Copyright (c) 2025, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef RDMA_Transporter_H
#define RDMA_Transporter_H

#include "ndb_config.h"

/*
 * RDMA_Transporter is only compiled when the build configures
 * NDB_RDMA_TRANSPORTER_SUPPORTED (driven by WITH_NDB_RDMA). Including this
 * header in a build without that macro is intentionally a no-op so the
 * registry can compile-gate its references with a single guard.
 */
#ifdef NDB_RDMA_TRANSPORTER_SUPPORTED
#include <atomic>

#include <cstdint>

#include "Transporter.hpp"

/*
 * Forward declarations of the libibverbs opaque types we hold as members.
 * Including <infiniband/verbs.h> only in the .cpp keeps the public header
 * small and avoids leaking libibverbs symbols into anything that merely
 * includes RDMA_Transporter.hpp (notably TransporterRegistry.cpp).
 */
struct ibv_context;
struct ibv_pd;
struct ibv_cq;
struct ibv_qp;
struct ibv_mr;
struct ibv_comp_channel;

/*
 * Routing bit OR'd into the `data.u32` field of epoll events that were
 * registered for an RDMA recv completion-channel fd (as opposed to the
 * normal control-socket fd, which carries a bare transporter index).
 *
 * Picked as the high bit of u32 because TrpId values are bounded by
 * MAX_NTRANSPORTERS (a few thousand at most), so the bit is always
 * free for tagging. TransporterRegistry::check_TCP unmasks the bit
 * to recover the underlying TrpId and routes the event accordingly:
 * comp-channel events go through RDMA_Transporter::handle_recv_comp_event()
 * and mark m_read_transporters; socket events keep the existing
 * TCP/SHM-style dispatch.
 */
static constexpr uint32_t RDMA_COMP_CHANNEL_BIT = 0x80000000u;

/*
 * --------------------------------------------------------------------------
 *  RDMA per-message wire framing (Milestone 6)
 * --------------------------------------------------------------------------
 *
 * Every SEND posted on the QP begins with a fixed-size rdma_msg_header_v1
 * followed by exactly `payload_len` bytes of RonDB Protocol6 signal data.
 * The framing is independent of the one-time endpoint exchange record
 * (rdma_endpoint_v1, defined in the .cpp), since the per-message header
 * has tighter latency requirements and a different schema.
 *
 * All multi-byte integers are in network byte order. Bumping the on-wire
 * format requires bumping RDMA_MSG_VERSION so the receiver can reject
 * mismatched senders early instead of corrupting state.
 */
static constexpr uint32_t RDMA_MSG_MAGIC = 0x52444D31u; /* 'R','D','M','1' */
static constexpr uint8_t RDMA_MSG_VERSION = 1u;
static constexpr uint16_t RDMA_MSG_HEADER_BYTES = 24u;

/*
 * Flag bitmap for rdma_msg_header_v1::flags.
 *
 *  RDMA_MSG_FLAG_CREDIT_ONLY   sender is just refilling peer's credit
 *                              accounting; payload_len is 0.
 *  RDMA_MSG_FLAG_HEARTBEAT     sender is asserting liveness; payload_len
 *                              is 0. Reserved for future use; receiver
 *                              currently treats it like CREDIT_ONLY.
 *  RDMA_MSG_FLAG_CONTROL_RESERVE
 *                              CREDIT_ONLY used the reserved control
 *                              receive slot, not a normal data credit.
 */
static constexpr uint8_t RDMA_MSG_FLAG_CREDIT_ONLY = 0x01u;
static constexpr uint8_t RDMA_MSG_FLAG_HEARTBEAT = 0x02u;
static constexpr uint8_t RDMA_MSG_FLAG_CONTROL_RESERVE = 0x04u;
static constexpr uint8_t RDMA_MSG_FLAG_ALL_KNOWN =
    RDMA_MSG_FLAG_CREDIT_ONLY | RDMA_MSG_FLAG_HEARTBEAT |
    RDMA_MSG_FLAG_CONTROL_RESERVE;

struct __attribute__((packed)) rdma_msg_header_v1 {
  uint32_t magic;        /* network order, must == RDMA_MSG_MAGIC */
  uint8_t version;       /* RDMA_MSG_VERSION */
  uint8_t flags;         /* RDMA_MSG_FLAG_* bitmap */
  uint16_t header_len;   /* network order, bytes (==RDMA_MSG_HEADER_BYTES) */
  uint32_t payload_len;  /* network order, bytes following the header */
  uint32_t send_seq;     /* network order, monotonic per direction */
  uint32_t ack_seq;      /* network order, peer's latest received seq */
  uint16_t credit_delta; /* network order, recv credits being granted */
  uint16_t reserved;     /* must be 0 */
};
static_assert(sizeof(rdma_msg_header_v1) == RDMA_MSG_HEADER_BYTES,
              "rdma_msg_header_v1 must be 24 bytes on the wire");

/**
 * @class RDMA_Transporter
 * @brief Native RonDB reliable-connected RDMA SEND/RECV transporter.
 *
 * Status:
 *   - Allocates verbs resources lazily during connect.
 *   - Exchanges endpoint metadata over the authenticated control socket.
 *   - Drives the QP to RTS and carries Protocol6 signal bytes over
 *     one-sided-local SEND/RECV work requests.
 *
 * Verbs ownership:
 *   - allocate_verbs_resources() acquires the device context, PD, two
 *     CQs, one RC QP, and two MRs/buffers.
 *   - release_verbs_resources() is idempotent and tears resources down
 *     in the reverse order. It is called from releaseAfterDisconnect(),
 *     resetBuffers(), the destructor, and on every failure path inside
 *     allocate_verbs_resources() itself.
 *
 * Threading:
 *   - Send posting and send-CQ reaping are serialized by the
 *     per-transporter send lock.
 *   - Receive-CQ reaping, ready-queue mutation, and slot consumption
 *     run on the assigned receive thread.
 *   - Cross-thread diagnostic fields are atomic; non-atomic slot state
 *     is owned by one side according to those locks.
 */
class RDMA_Transporter : public Transporter {
  /*
   * The transporter registry drives polling and unpacking over our
   * private receive helpers (reap_recv_completions, get_next_read,
   * consume_received_bytes, has_received_data). Mirrors the SHM/TCP
   * pattern of friending the registry rather than exposing the
   * helpers in the public API.
   */
  friend class TransporterRegistry;

 public:
  /**
   * Construct an RDMA transporter from a fully-parsed
   * TransporterConfiguration. The constructor copies the RDMA-specific
   * tunables out of the union and forwards generic fields to the base.
   *
   * Preconditions:
   *   - config != nullptr
   *   - config->type == tt_RDMA_TRANSPORTER
   *   - config->transporterIndex was assigned by the registry
   *
   * The constructor never touches libibverbs.
   */
  RDMA_Transporter(TransporterRegistry &reg,
                   const TransporterConfiguration *config);

  /**
   * Copy-construct an additional transporter for multi-transporter support.
   *
   * The clone receives fresh verbs resources on connect while copying
   * the immutable RDMA tuning configuration from the source transporter.
   */
  RDMA_Transporter(TransporterRegistry &reg, const RDMA_Transporter *other);

  ~RDMA_Transporter() override;

  /**
   * Initialize generic transporter state. Verbs resources are *not*
   * touched here; they are allocated lazily during connect setup so a
   * misconfigured fabric does not prevent the data node from starting.
   */
  bool initTransporter() override;

  /**
   * Drain the upper-layer send buffer into registered RDMA SEND slots
   * while peer receive credits and local send slots are available.
   */
  bool doSend(bool need_wakeup = true) override;

  /**
   * Clear any buffered RDMA state when the transporter is disconnected.
   * Currently equivalent to release_verbs_resources(); the override is
   * needed because the base default is a no-op and would otherwise miss
   * the verbs cleanup if a future caller invokes resetBuffers() without
   * a preceding disconnectImpl().
   */
  void resetBuffers() override;

 protected:
  /**
   * Verify and apply runtime reconfiguration. The skeleton accepts the
   * config unchanged via the base class default behavior.
   */
  bool configure_derived(const TransporterConfiguration *conf) override;

  /**
   * Connect callbacks allocate verbs resources, exchange endpoint records
   * over the control socket, post initial receives, and transition the QP
   * through INIT/RTR/RTS. On failure the caller tears the partial state
   * down and leaves the transporter disconnected.
   */
  bool connect_server_impl(NdbSocket &&socket) override;
  bool connect_client_impl(NdbSocket &&socket) override;

  /**
   * Disconnect callback. Releases any verbs resources still owned by
   * this transporter's control socket state, but deliberately leaves
   * verbs resources intact until releaseAfterDisconnect() when send
   * buffers are already disabled and the receive side has stopped using
   * the transporter.
   */
  void disconnectImpl() override;

  /**
   * Final cleanup hook called once the transporter is DISCONNECTED.
   * Ensures verbs resources are released only after the registry has
   * disabled send buffers and cleared receive-side state, then closes
   * the control socket via the base implementation.
   */
  void releaseAfterDisconnect() override;

 private:
  /**
   * Send-side hooks required by Transporter. send_is_possible() reports
   * whether a normal DATA SEND can currently be posted; CREDIT_ONLY may
   * still use the reserved control receive slot when data credits are
   * exhausted.
   */
  bool send_is_possible(int timeout_millisec) const override;
  bool send_limit_reached(int bufsize) override;

  /*
   * allocate_verbs_resources() opens the configured ibverbs device,
   * validates port/device capabilities against the configured tunables,
   * and allocates a Protection Domain, two Completion Queues (one for
   * send, one for recv), a reliable-connected Queue Pair, and two
   * Memory Regions covering the send/recv staging buffers.
   *
   * On success every owned member pointer below is non-NULL and the
   * negotiated attributes (max inline data, MTU, GID, etc.) have been
   * cached locally. On failure the function fully tears down whatever it
   * has allocated so the caller only has to react to the bool result.
   *
   * Caller must hold whatever lock protects the calling Transporter; the
   * function itself is not thread-safe wrt concurrent calls on the same
   * transporter.
   */
  bool allocate_verbs_resources();

  /**
   * Idempotently release all verbs resources owned by this transporter.
   * Each `ibv_destroy_x` / `ibv_dealloc_x` / `ibv_dereg_x` call is
   * conditional on the matching pointer being non-NULL, and every
   * member is reset to NULL before return. Safe to invoke from
   * disconnect paths, the destructor, and (defensively) from
   * resetBuffers().
   */
  void release_verbs_resources();

  /**
   * Emit an info-level event log line describing the negotiated
   * attributes (device name, port, MTU, GID index, QP number, queue
   * depths, max inline data). Called once after a successful
   * allocate_verbs_resources() so operators can correlate fabric state
   * with config tunables.
   */
  void log_negotiated_attributes() const;

  /**
   * run_endpoint_exchange() implements the symmetric endpoint handshake
   * over the already-authenticated control socket:
   *   1. Send our local endpoint record (QPN, PSN, GID, MTU, ...).
   *   2. Read the peer endpoint record with a finite timeout.
   *   3. Validate version, link layer, RoCE GIDs, and MTU compatibility.
   *   4. Transition the QP through INIT, RTR (using peer info), and
   *      RTS (using our own initial PSN).
   *
   * All ibv_modify_qp() failures and protocol mismatches are surfaced
   * via g_eventLogger->error() and cause this method to return false.
   * On false the caller must release verbs resources; on true the QP
   * is at IBV_QPS_RTS and ready to post WRs.
   *
   * @param socket  the authenticated control socket (borrowed; this
   *                method does not take ownership).
   * @param side    "server" or "client" for log output only.
   * @return        true if the QP reached RTS, false otherwise.
   */
  bool run_endpoint_exchange(const class NdbSocket &socket, const char *side);

  /*
   * Individual QP-modify helpers used by run_endpoint_exchange(). Each
   * returns true on success and logs the underlying errno on failure.
   *
   * qp_transition_to_init() puts the freshly created QP into the INIT
   * state with port, pkey index, and access flags for LOCAL_WRITE only.
   *
   * qp_transition_to_rtr() transitions INIT -> RTR using the peer's
   * QPN/PSN/GID/MTU/link-layer pulled out of the negotiated record.
   * The peer record is passed as an opaque pointer to keep the wire
   * struct definition private to the .cpp.
   *
   * qp_transition_to_rts() transitions RTR -> RTS using our own initial
   * PSN and the configured retry counters.
   */
  bool qp_transition_to_init();
  bool qp_transition_to_rtr(const void *peer_record);
  bool qp_transition_to_rts(Uint32 local_psn);

 public:
  /*
   * Wire-format helpers exposed publicly. Rationale:
   *   - The wire format itself (rdma_msg_header_v1, RDMA_MSG_MAGIC,
   *     RDMA_MSG_VERSION, flag constants) is already declared at file
   *     scope in this header, so the schema is part of the public API.
   *   - The TEST_RDMA_TRANSPORTER unit tests at the bottom of
   *     RDMA_Transporter.cpp exercise these helpers directly without
   *     instantiating verbs resources.
   *   - The helpers are stateless and have no security implications
   *     beyond what the struct already exposes.
   *
   * encode_msg_header() writes a fully-populated rdma_msg_header_v1
   * into `buf` in network byte order. `buf` must point to at least
   * RDMA_MSG_HEADER_BYTES bytes of writable memory; the caller is
   * responsible for that guarantee. Returns nothing because the
   * function cannot fail.
   *
   * validate_msg_header() decodes a header sitting at `buf` and
   * validates magic/version/header_len/payload_len/flags. The
   * `available` parameter is the total number of bytes currently in
   * the receive slot (header plus possible payload); the function
   * rejects records whose declared payload_len would overflow
   * `available - sizeof(header)`.
   * Returns true on success and writes the decoded fields through the
   * output pointers; returns false on validation failure (the output
   * pointers are left undefined).
   */
  static void encode_msg_header(void *buf, Uint32 payload_len,
                                Uint32 send_seq, Uint32 ack_seq,
                                Uint16 credit_delta, Uint8 flags);
  static bool validate_msg_header(const void *buf, size_t available,
                                  Uint32 *out_payload_len,
                                  Uint32 *out_send_seq, Uint32 *out_ack_seq,
                                  Uint16 *out_credit_delta, Uint8 *out_flags);

 private:
  /*
   * Post one receive WR pointing at slot `slot_idx` in the recv
   * staging buffer. The slot size is m_recv_buffer_size / m_queue_depth
   * and wr_id encodes the slot index so the receive completion
   * handler can re-post the same slot later.
   *
   * post_initial_receives() validates buffer geometry and posts
   * exactly m_queue_depth receive WRs. Must be called after the QP is
   * in the INIT state and before the QP is transitioned to RTR. On
   * any failure the partially-posted receives are reaped automatically
   * when the QP is destroyed during release_verbs_resources().
   */
  bool post_one_receive(Uint32 slot_idx);
  bool post_initial_receives();

  /*
   * Recompute slot geometry from m_recv_buffer_size / m_queue_depth
   * and validate it satisfies the minimum header+payload requirement.
   * Returns the slot size in bytes on success, 0 on failure (an error
   * is logged via g_eventLogger->error()).
   */
  Uint32 recv_slot_size_or_zero() const;

  /*
   * Same geometry validation for the send-side staging buffer. Each
   * outbound slot must hold one full header plus MAX_SEND_MESSAGE_BYTESIZE
   * payload bytes.
   */
  Uint32 send_slot_size_or_zero() const;

  /*
   * Allocate/free the per-slot bookkeeping array m_send_slots. Called
   * from allocate_verbs_resources() and release_verbs_resources()
   * respectively. release_send_slot_state() is idempotent.
   */
  bool allocate_send_slot_state();
  void release_send_slot_state();

  /*
   * Reap any send completions waiting on m_send_cq, bounded by the
   * configured m_completion_poll_budget. For each successful WC, free
   * the corresponding staging slot. Upper-layer send-buffer bytes were
   * already released immediately after a successful ibv_post_send(),
   * because the data has been copied into m_send_buf by then. For any
   * WC.status != SUCCESS, surface a transporter error
   * (TE_RDMA_CQ_ERROR/TE_RDMA_RETRY_EXHAUSTED) and start the disconnect
   * protocol from the send-side source.
   *
   * Returns the number of completions reaped (0 means "CQ was empty"),
   * or -1 on a fatal CQ error.
   */
  int reap_send_completions();

  /*
   * Find the next free send slot in round-robin order using
   * m_next_send_slot as a starting hint. Returns UINT32_MAX if all
   * slots are currently in flight.
   */
  Uint32 find_free_send_slot();

  /*
   * Build and post a single zero-payload CREDIT_ONLY WR using the
   * current pending credit grant. Callers must hold the send-side
   * lock (see Transporter::lock_send_transporter) because this method
   * mutates m_send_slots / m_send_slots_in_flight / m_local_send_seq /
   * m_peer_recv_credits / m_pending_credit_grant and calls
   * ibv_post_send, which the verbs API requires to be serialized
   * against other posts on the same QP.
   *
   * Returns true iff a WR was successfully posted. Returns false when
   * preconditions are not met (no pending grant, no free slot, or
   * geometry/verbs not live) or when ibv_post_send itself failed; in
   * the latter case the link is already in the disconnect protocol.
   * When normal DATA credits are exhausted, the helper may use the
   * peer's reserved control receive slot instead of failing.
   */
  bool post_credit_only_locked();

 public:
  /*
   * Opportunistic CREDIT_ONLY emission triggered by the receive
   * thread.
   *
   * Multi-transporter clones that mostly carry inbound traffic never
   * have doSend() called on them under benchmark load, so the only
   * credit-refill path in doSend() never fires for those clones.
   * Without this hook the peer's outbound credit pool against us
   * drains to zero and the inbound direction stalls, which is the
   * GCP_COMMIT-lag / NDB-274 / NDB-286 pattern we observed.
   *
   * The receive thread therefore calls this method at the tail of
   * each recv batch. The method does an atomic fast check on the
   * pending grant counter; if the half-queue-depth threshold has
   * been crossed it acquires the per-transporter send lock, reaps
   * outstanding send completions to free slots that may have been
   * occupied by previous CREDIT_ONLY emissions, and emits one
   * zero-payload CREDIT_ONLY WR via post_credit_only_locked().
   *
   * No-op when no grant is pending, when the threshold is not yet
   * met, or when verbs state is not live. Never blocks for more than
   * the duration of a single doSend() invocation on the same
   * transporter.
   */
  void recv_thread_emit_credit_only();

  /*
   * --------------------------------------------------------------------------
   *  Receive completion-channel integration (wakeup mechanism)
   * --------------------------------------------------------------------------
   *
   * The recv CQ is bound to m_recv_comp_channel at allocate_verbs_resources()
   * time. The channel's fd is added to the receive thread's epoll set by
   * TransporterReceiveData::epoll_add(); when the HCA delivers a completion
   * the fd becomes readable and the receive thread wakes out of epoll_wait().
   * The standard arm-vs-fire race is handled by arm_recv_cq(), called at the
   * tail of every reap_recv_completions() pass so an arm is always in place
   * before the recv thread re-enters epoll_wait().
   *
   * Without this wakeup the recv thread would only notice incoming RDMA
   * data at the next epoll_wait timeout in check_TCP(), which bounds
   * RDMA receive latency below by that timeout. See RonDB transporter
   * notes (src/common/transporter/trp.txt) for the equivalent SHM-side
   * wakeup discussion -- this is the verbs-native counterpart.
   *
   * get_recv_comp_channel_fd()
   *   Returns the (non-blocking) underlying fd so the registry can add
   *   it to its epoll set. Returns -1 when the channel has not been
   *   allocated yet (transporter is disconnected).
   *
   * handle_recv_comp_event()
   *   Drains and acknowledges all pending events on the comp channel
   *   (ibv_get_cq_event + ibv_ack_cq_events). Does NOT call
   *   ibv_poll_cq itself; the registry drives polling via the
   *   existing reap_recv_completions path. Safe to call from the
   *   receive thread when the channel fd has fired in epoll. Defends
   *   against the channel having been torn down concurrently by
   *   release_verbs_resources().
   *
   * arm_recv_cq()
   *   Calls ibv_req_notify_cq(m_recv_cq, solicited_only=0) so the
   *   next CQE generates an event on the channel fd. A failure here
   *   is logged but non-fatal -- the recv path still polls the CQ
   *   at the existing pollReceive cadence, just without the wakeup
   *   optimization.
   */
  int get_recv_comp_channel_fd() const;
  void handle_recv_comp_event();
  void arm_recv_cq();

 private:

  /*
   * --- Receive-side helpers (Milestone 8) ---
   *
   * allocate_recv_slot_state() / release_recv_slot_state() manage the
   * parallel arrays for tracking which receive slots currently hold
   * unpacked payload bytes, and a FIFO ring queue of slot indices in
   * completion order. Called from allocate_verbs_resources() /
   * release_verbs_resources(); release is idempotent.
   */
  bool allocate_recv_slot_state();
  void release_recv_slot_state();

  /*
   * Poll the recv CQ and process completed receives. For each
   * successful WC: validate the per-message header, enforce in-order
   * send_seq (otherwise disconnect with TE_RDMA_INVALID_HEADER),
   * advance m_local_recv_seq, fold the peer's credit_delta into
   * m_peer_recv_credits, update m_peer_ack_seq, enqueue the slot in
   * the ready queue, and notify the receive-handle via
   * transporter_recv_from() so heartbeat detection keeps working.
   *
   * Returns the number of completions consumed, or -1 on a fatal CQ
   * error (start_disconnecting has already been called).
   */
  int reap_recv_completions(class TransporterReceiveHandle &recvdata);

  /*
   * Public-facing accessors used by the registry receive path.
   * has_received_data() returns whether any slot in the ready queue
   * still has un-consumed bytes. get_next_read() yields the next
   * contiguous byte run for the unpacker directly from the parked HCA
   * receive slot in m_recv_buf; *out_len is 0 when no data is
   * available. consume_received_bytes() advances the read offset and,
   * once the unpacker has fully drained the slot, re-posts the
   * underlying ibv_recv_wr to the RQ and grants one credit back to
   * the peer before popping the slot off the ready queue. The data
   * slot is therefore owned exclusively by the upper layer between
   * its WC arrival and the consume completion, which closes the
   * earlier app-buffer overwrite hazard.
   */
  bool has_received_data() const;
  void get_next_read(const void **out_ptr, Uint32 *out_len) const;
  void consume_received_bytes(Uint32 n);

  /*
   * Reset all wire-state counters to their connect-time defaults.
   * Invoked from constructors, release_verbs_resources(), and
   * resetBuffers() so a reconnect always starts from a clean slate.
   */
  void reset_wire_state();

  /*
   * Emit a single g_eventLogger->info() line summarizing the per-link
   * RDMA counters. Called from release_verbs_resources() when at least
   * one counter is non-zero so operators see a final tally as the link
   * tears down. Safe to invoke at any time; never modifies state and
   * tolerates partial/uninitialized verbs handles.
   */
  void log_stats() const;

  /*
   * Periodic stats heartbeat. Called from doSend() (send thread) and
   * reap_recv_completions() (recv thread). If at least
   * RDMA_STATS_HEARTBEAT_NS have elapsed since the previous emission
   * for this transporter, calls log_stats() and bumps the timestamp.
   * Cheap: a single clock_gettime + 64-bit compare on the fast path.
   *
   * The two callers race on m_last_stats_log_ns, but the race is
   * benign: a torn 64-bit read on a misaligned value cannot happen
   * because rdma_stats keeps the field 8-byte aligned, and even if
   * both threads decide to emit in the same window we just get a
   * single duplicated line. No state correctness is affected.
   */
  void maybe_log_stats_heartbeat();

  /*
   * Cached RDMA-specific configuration, copied out of TransporterConfiguration
   * at construction time. Stored separately so the union doesn't need to
   * outlive the call to configure(). All sizes are in bytes; queue depth
   * counts work-requests.
   */
  Uint32 m_send_buffer_size;
  Uint32 m_recv_buffer_size;
  Uint32 m_queue_depth;
  Uint32 m_inline_threshold;
  Uint32 m_completion_poll_budget;
  Uint32 m_rdma_port;
  Uint32 m_gid_index;
  Uint32 m_traffic_class;
  Uint32 m_retry_count;
  Uint32 m_rnr_retry_count;
  /*
   * Owned copy of the configured device name. NULL means "first available".
   * Copied so the transporter doesn't depend on the lifetime of the
   * ConfigInfo string table.
   */
  char *m_device_name;

  /*
   * Verbs resources owned by this transporter. All NULL until a
   * successful allocate_verbs_resources() call. Destruction order in
   * release_verbs_resources() must be the reverse of construction:
   * QP -> CQs (recv first, while comp channel still alive) ->
   * comp channel -> MRs -> staging buffers -> PD -> device context.
   *
   * m_recv_comp_channel is bound to m_recv_cq so the HCA can deliver
   * a wakeup event whenever an incoming CQE lands on the recv CQ.
   * Its fd is registered with the receive thread's epoll set by the
   * registry; see the wakeup helpers above.
   *
   * m_recv_comp_events_pending tracks events received via
   * ibv_get_cq_event() that we still need to ack. We ack each event
   * individually inside handle_recv_comp_event(), so this normally
   * stays at 0; it exists as a defensive backstop so
   * release_verbs_resources() can drain residual acks before
   * destroying the CQ (ibv_destroy_cq blocks on unacked events).
   */
  struct ibv_context *m_verbs_ctx;
  struct ibv_pd *m_pd;
  struct ibv_cq *m_send_cq;
  struct ibv_cq *m_recv_cq;
  struct ibv_qp *m_qp;
  struct ibv_mr *m_send_mr;
  struct ibv_mr *m_recv_mr;
  struct ibv_comp_channel *m_recv_comp_channel;
  Uint64 m_recv_comp_events_pending;

  /*
   * Owned, page-aligned staging buffers registered with the HCA. Freed
   * with std::free() after the corresponding MR is deregistered.
   *
   * m_app_buf is a reserved second recv-side buffer of the same shape
   * as m_recv_buf, also page-aligned and NOT registered with the HCA.
   * It was originally introduced as an application-visible mirror so
   * the data path could re-post the HCA slot at WC time. That design
   * had a structural overwrite hazard: the HCA could land a second
   * SEND into the same slot before the upper layer had drained the
   * previous payload, corrupting the Protocol6 byte stream.
   *
   * The current safe slot-ownership rule keeps the HCA slot parked in
   * m_recv_buf until consume_received_bytes() finishes draining it,
   * and the unpacker reads payload bytes directly from m_recv_buf.
   * m_app_buf is therefore unused by the data path today but is left
   * allocated as scaffolding for a future decoupled app-buffer pool
   * that would restore WC-time repost throughput without the overwrite
   * hazard. CREDIT_ONLY / HEARTBEAT control messages never expose an
   * app-visible generation and continue to repost at WC time.
   */
  void *m_send_buf;
  void *m_recv_buf;
  void *m_app_buf;

  /*
   * Negotiated inline-data cap. The configured m_inline_threshold may be
   * larger than what the device actually supports; we record the
   * effective cap after QP creation so future send paths do not exceed
   * device limits.
   */
  Uint32 m_effective_inline_threshold;

  /*
   * Per-direction wire state. Reset by reset_wire_state(). All counters
   * use the 32-bit wraparound semantics from the wire header; comparison
   * logic that cares about ordering must use the standard "(a - b) <
   * 0x80000000" trick to handle wrap correctly.
   *
   *  m_local_send_seq      next seq number we will stamp on an outgoing
   *                        SEND. Only mutated under the send lock; non-
   *                        atomic.
   *  m_local_recv_seq      next seq number we expect from the peer; any
   *                        SEND with seq != this value is a protocol
   *                        violation and disconnects the link.
   *                        Incremented by the receive thread but read
   *                        on the send thread when stamping ack_seq on
   *                        outgoing messages -- declared std::atomic so
   *                        cross-thread access is not a C++ data race.
   *                        The send thread uses memory_order_relaxed
   *                        because ack_seq is an advisory hint, not a
   *                        protocol invariant.
   *  m_peer_ack_seq        most recent seq the peer has acknowledged in
   *                        a header.ack_seq field; used to free outbound
   *                        send slots in Milestone 7. Only mutated under
   *                        the receive lock; not currently read on the
   *                        send thread, so non-atomic is sufficient.
   *  m_peer_recv_credits   credits we currently hold against the peer's
   *                        receive queue. Decremented by 1 for every
   *                        SEND we post. When 0 we must stall sending
   *                        until the peer grants more via credit_delta.
   *  m_pending_credit_grant
   *                        credits we want to give the peer but have
   *                        not yet sent in a header.credit_delta. The
   *                        send path piggybacks this on the next
   *                        outbound SEND, or generates a CREDIT_ONLY
   *                        message if it grows beyond half of the
   *                        local queue depth.
   *
   * Credit-state mutations are serialized by the per-transporter send
   * lock shared with performSend()/recv_thread_emit_credit_only().
   * These two fields are atomic only so fast-path checks and stats
   * logging can read them without introducing a C++ data race.
   *  m_local_recv_posted   how many receive WRs are currently posted to
   *                        the RQ. Must equal m_queue_depth right after
   *                        post_initial_receives() succeeds. Read in
   *                        log_stats() from the opposite thread, so the
   *                        field is std::atomic to keep the diagnostic
   *                        snapshot well-defined under C++; the actual
   *                        mutation happens on the receive thread and
   *                        uses memory_order_relaxed.
   */
  Uint32 m_local_send_seq;
  std::atomic<Uint32> m_local_recv_seq;
  Uint32 m_peer_ack_seq;
  std::atomic<Uint32> m_peer_recv_credits;
  std::atomic<Uint32> m_pending_credit_grant;
  std::atomic<Uint32> m_local_recv_posted;
  /*
   * RDMA-local wire byte counters mirrored from the base Transporter
   * counters. The base fields stay plain Uint64 for TCP/SHM ABI
   * compatibility, but log_stats() runs from both send and receive
   * contexts, so it reads these atomics instead of cross-thread
   * reading m_bytes_sent / m_bytes_received directly.
   */
  std::atomic<Uint64> m_wire_bytes_sent{0};
  std::atomic<Uint64> m_wire_bytes_received{0};

  /*
   * Per send-slot bookkeeping. The verbs WR completion only tells us
   * which slot completed (via wr_id), so we keep a parallel array of
   * slot state here. `payload_len` is the count of payload bytes staged
   * into this registered slot; it is used for byte counters when the WR
   * completes. The upper-layer send buffer is released immediately after
   * a successful post, not at completion time.
   *
   * m_send_slots is heap-allocated with m_queue_depth entries during
   * allocate_verbs_resources() and freed during
   * release_verbs_resources(). It is intentionally a raw array rather
   * than std::vector to avoid pulling additional includes into the
   * header.
   */
  struct rdma_send_slot {
    Uint32 payload_len;
    bool in_flight;
  };
  rdma_send_slot *m_send_slots;
  /*
   * Count of send WRs currently posted on the QP. Mutated only on the
   * send thread under the per-transporter send lock; read on the
   * receive thread from log_stats() and from
   * recv_thread_emit_credit_only() (the latter also takes the send
   * lock before reading). std::atomic<Uint32> with relaxed ordering
   * makes the diagnostic cross-thread read well-defined under C++
   * without changing send-side semantics.
   */
  std::atomic<Uint32> m_send_slots_in_flight;
  /*
   * Round-robin allocation hint for find_free_send_slot(). Just a
   * starting index; the loop always tries all slots.
   */
  Uint32 m_next_send_slot;

  /*
   * Per recv-slot state. payload_len is the byte count of the framed
   * payload (excluding header) that landed in this slot from the
   * latest receive; read_offset is the number of payload bytes the
   * upper layer has already unpacked. payload_len == 0 means the slot
   * is currently posted to the RQ and waiting for incoming data.
   *
   * Allocated with m_queue_depth entries by allocate_recv_slot_state().
   */
  struct rdma_recv_slot {
    Uint32 payload_len;
    Uint32 read_offset;
  };
  rdma_recv_slot *m_recv_slots;

  /*
   * FIFO ring of recv-slot indices in completion order. RC ordering
   * makes the post order match the receive order for a single QP, but
   * we still need a queue because the unpacker may pause part-way
   * through processing the head slot.
   *
   * m_recv_queue_count is the number of valid entries; head/tail
   * advance modulo m_queue_depth.
   */
  Uint32 *m_recv_ready_queue;
  Uint32 m_recv_queue_head;
  Uint32 m_recv_queue_tail;
  /*
   * Mutated only on the receive thread (under the global recv lock).
   * Read on the send thread from log_stats(). std::atomic<Uint32>
   * with relaxed ordering keeps the cross-thread diagnostic read
   * well-defined; the receive-side mutations remain single-threaded.
   */
  std::atomic<Uint32> m_recv_queue_count;

  /*
   * --------------------------------------------------------------------------
   *  Observability counters (Milestone 10)
   * --------------------------------------------------------------------------
   *
   * Per-transporter counters incremented from the send/receive hot path
   * and the verbs lifecycle. All counters use Uint64 to keep wraparound
   * out of scope for a single process lifetime; they intentionally do
   * NOT reset across reconnects because operators want cumulative
   * counts to correlate with cluster-wide event logs.
   *
   * Counters are grouped into a plain-old-data struct so future code
   * can pass them around or snapshot them atomically with a single
   * structure copy. C++11 default member initializers zero every field
   * without help from the enclosing constructor.
   *
   * Names follow the categories listed in the implementation roadmap:
   *  - sends: posted, completions ok/err, inline, credit-only emitted,
   *           bytes copied into staging.
   *  - recvs: posted, completions ok/err, credit-only received,
   *           bytes consumed by the unpacker.
   *  - cq:    polls executed and budget exhausted events for send/recv.
   *  - link:  RNR retries, retry-exceeded events, QP-fatal events,
   *           and reconnect attempts.
   *  - flow:  send-side credit stalls observed by doSend().
   */
  /*
   * Counters are std::atomic<Uint64> because log_stats() may run on
   * either the send thread (from doSend()) or the receive thread (from
   * reap_recv_completions()) and reads counters owned by the other
   * thread. fetch_add(1, memory_order_relaxed) on x86_64 compiles to a
   * single LOCK XADD, which is well under 10 ns on the lab data nodes
   * and is therefore acceptable on the hot path. The relaxed ordering
   * is correct for monotonic counters: we do not use these values to
   * synchronize access to anything else.
   */
  struct rdma_stats {
    /* --- send path --- */
    std::atomic<Uint64> send_posted{0};
    std::atomic<Uint64> send_completions_ok{0};
    std::atomic<Uint64> send_completion_errors{0};
    std::atomic<Uint64> send_inline{0};
    std::atomic<Uint64> send_credit_only_out{0};
    /* Subset of send_credit_only_out attributed to the receive thread
     * emit path (recv_thread_emit_credit_only). Separately tracked so
     * operators can verify the new path is active on clones that have
     * mostly inbound traffic. */
    std::atomic<Uint64> send_credit_only_recv_path{0};
    /* Reserved for log-format compatibility with older diagnostic
     * parsers. The timer-based CREDIT_ONLY path was reverted and this
     * counter currently remains 0. */
    std::atomic<Uint64> send_credit_only_timer{0};
    std::atomic<Uint64> send_credit_stalls{0};
    std::atomic<Uint64> copied_send_bytes{0};
    /* --- recv path --- */
    std::atomic<Uint64> recv_posted{0};
    std::atomic<Uint64> recv_completions_ok{0};
    std::atomic<Uint64> recv_completion_errors{0};
    std::atomic<Uint64> recv_credit_only_in{0};
    std::atomic<Uint64> copied_recv_bytes{0};
    /* --- CQ polling --- */
    std::atomic<Uint64> cq_polls_send{0};
    std::atomic<Uint64> cq_polls_recv{0};
    std::atomic<Uint64> cq_budget_hits_send{0};
    std::atomic<Uint64> cq_budget_hits_recv{0};
    /* --- link-level error events --- */
    std::atomic<Uint64> rnr_events{0};
    std::atomic<Uint64> retry_exceeded_events{0};
    std::atomic<Uint64> qp_fatal_events{0};
    std::atomic<Uint64> reconnect_attempts{0};
  };
  rdma_stats m_stats;

  /*
   * Monotonic-clock nanoseconds of the last log_stats() heartbeat
   * emission for this transporter. 0 means "never emitted yet"; the
   * first heartbeat check after that always emits and updates the
   * stamp. Gated to RDMA_STATS_HEARTBEAT_NS so the journal sees one
   * line per transporter per 10s window at most.
   *
   * Declared std::atomic because both doSend() (send thread) and
   * reap_recv_completions() (receive thread) call
   * maybe_log_stats_heartbeat() and race on this field. memory_order_
   * relaxed is sufficient: the field is purely an interval guard, not
   * a synchronizer for any other state.
   */
  std::atomic<Uint64> m_last_stats_log_ns{0};

};

#endif /* NDB_RDMA_TRANSPORTER_SUPPORTED */

#endif /* RDMA_Transporter_H */
