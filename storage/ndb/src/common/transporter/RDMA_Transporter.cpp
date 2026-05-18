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

#include <ndb_global.h>
#include "ndb_config.h"

/*
 * Whole translation unit is empty unless WITH_NDB_RDMA was selected
 * at configure time. CMake only adds this .cpp to the build when
 * NDB_RDMA_TRANSPORTER_SUPPORTED is defined, but the guard mirrors
 * the header and keeps this file compilable in isolation.
 */
#ifdef NDB_RDMA_TRANSPORTER_SUPPORTED

#include <arpa/inet.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <EventLogger.hpp>

/*
 * libibverbs is required at link time when NDB_RDMA_TRANSPORTER_SUPPORTED is
 * defined. The header is only included in this .cpp so other translation
 * units that pull in RDMA_Transporter.hpp do not need libibverbs available.
 */
#include <infiniband/verbs.h>

#include "RDMA_Transporter.hpp"
#include "TransporterCallback.hpp"
#include "util/NdbSocket.h"
#include "util/require.h"

extern EventLogger *g_eventLogger;

/*
 * --------------------------------------------------------------------------
 *  Endpoint wire record
 * --------------------------------------------------------------------------
 *
 * A fixed-size, versioned record exchanged once over the authenticated
 * control socket. Both sides send a record, then read the peer's record.
 * Multi-byte integers are in network byte order. The GID is already
 * defined by the IB spec as a 16-byte big-endian quantity, so it is
 * transferred verbatim without byte swapping.
 *
 * Wire size is asserted via static_assert below; any change to the
 * struct must bump RDMA_WIRE_VERSION so peers can reject a mismatched
 * format with a clear error rather than silently corrupting state.
 *
 * Layout is fully explicit to keep alignment portable. We mark it
 * packed for paranoia even though all fields are naturally aligned at
 * their declared offsets.
 */
static constexpr uint32_t RDMA_WIRE_MAGIC = 0x52444D41u; /* 'R','D','M','A' */
static constexpr uint16_t RDMA_WIRE_VERSION = 1u;
static constexpr uint16_t RDMA_WIRE_HEADER_BYTES = 64u;
static constexpr int RDMA_HANDSHAKE_TIMEOUT_MS = 30 * 1000;

struct __attribute__((packed)) rdma_endpoint_v1 {
  uint32_t magic;       /* network order, must == RDMA_WIRE_MAGIC */
  uint16_t version;     /* network order, RDMA_WIRE_VERSION */
  uint16_t header_len;  /* network order, bytes in this record */
  uint32_t qp_num;      /* network order, local QPN */
  uint32_t psn;         /* network order, initial Packet Serial Number */
  uint16_t lid;         /* network order, IB LID (0 for RoCE) */
  uint16_t pad0;        /* reserved, must be 0 */
  uint8_t  gid[16];     /* IB spec big-endian, copied verbatim */
  uint8_t  gid_index;   /* GID table index */
  uint8_t  port_num;    /* HCA physical port */
  uint8_t  mtu;         /* ibv_mtu enum value (256B..4096B) */
  uint8_t  link_layer;  /* IBV_LINK_LAYER_INFINIBAND or _ETHERNET */
  uint32_t max_inline;  /* network order, effective inline cap */
  uint32_t queue_depth; /* network order, advertised send/recv WR depth */
  uint32_t recv_credits;/* network order, initial credits granted to peer */
  uint32_t reserved[3]; /* future use, must be zero */
};
static_assert(sizeof(rdma_endpoint_v1) == RDMA_WIRE_HEADER_BYTES,
              "rdma_endpoint_v1 must be 64 bytes on wire");

/*
 * Read exactly `len` bytes from the socket within a single timeout
 * budget, looping over NdbSocket::read() because read_socket() may
 * return fewer bytes than requested. Returns true on success.
 */
static bool rdma_recv_full(const NdbSocket &socket, void *buf, size_t len,
                           int timeout_ms) {
  char *p = static_cast<char *>(buf);
  size_t remaining = len;
  while (remaining > 0) {
    /* NdbSocket::read takes the remaining timeout per call. We pass the
     * whole budget each time; in the absolute-worst case the operation
     * fires once per byte but in practice the socket-level poll fast-
     * paths large reads. */
    int n = socket.read(timeout_ms, p, (int)remaining);
    if (n <= 0) {
      g_eventLogger->error(
          "RDMA: control-socket read failed (returned %d, errno=%d %s)", n,
          errno, std::strerror(errno));
      return false;
    }
    p += n;
    remaining -= (size_t)n;
  }
  return true;
}

/*
 * Send exactly `len` bytes within `timeout_ms`. NdbSocket::write()
 * already loops internally and returns 0 on full success, -1 otherwise.
 */
static bool rdma_send_full(const NdbSocket &socket, const void *buf,
                           size_t len, int timeout_ms) {
  int elapsed = 0;
  int r = socket.write(timeout_ms, &elapsed, static_cast<const char *>(buf),
                       (int)len);
  if (r != 0) {
    g_eventLogger->error(
        "RDMA: control-socket write failed (returned %d, errno=%d %s)", r,
        errno, std::strerror(errno));
    return false;
  }
  return true;
}

/*
 * Generate a non-zero initial PSN. Per IB spec the PSN is 24 bits.
 * We do not use std::rand() to avoid touching shared RNG state; a
 * monotonically increasing process-local counter, XOR'd with the
 * pointer value of the QP, is good enough for distinguishing local
 * PSN sequences across reconnects.
 */
static uint32_t rdma_pick_initial_psn(const struct ibv_qp *qp) {
  static uint32_t s_psn_counter = 0;
  /* Treat the QP pointer as an opaque mixing value. We only use the
   * low 24 bits because that is all the IB PSN field carries. */
  const uintptr_t mix = reinterpret_cast<uintptr_t>(qp);
  const uint32_t bumped = ++s_psn_counter;
  return ((uint32_t)mix ^ bumped) & 0x00FFFFFFu;
}

/*
 * Helper: duplicate a configured device-name string. Returns nullptr when
 * the input is null or empty (the empty string in config means "any"); the
 * caller takes ownership of the returned buffer. Allocation failure is
 * silently tolerated: we fall back to "any" rather than crashing.
 */
static char *rdma_clone_device_name(const char *src) {
  if (src == nullptr || src[0] == '\0') return nullptr;
  const size_t len = std::strlen(src);
  char *dup = new (std::nothrow) char[len + 1];
  if (dup == nullptr) return nullptr;
  std::memcpy(dup, src, len + 1);
  return dup;
}

/*
 * Page-aligned buffer allocation backed by posix_memalign(3). Returns NULL
 * on failure. We deliberately do not use C11 aligned_alloc to avoid
 * portability problems on older glibc targets that RonDB still supports.
 * Alignment is fixed at 4 KiB which matches the host page size on every
 * platform RonDB currently targets and satisfies the alignment
 * requirements documented for ibv_reg_mr().
 */
static void *rdma_aligned_alloc(size_t bytes) {
  void *p = nullptr;
  if (bytes == 0) return nullptr;
  if (posix_memalign(&p, 4096, bytes) != 0) return nullptr;
  std::memset(p, 0, bytes);
  return p;
}

/*
 * Convert an IBV MTU enum to its byte value for logging only. The runtime
 * never depends on this value; it is purely informational.
 */
static Uint32 rdma_mtu_to_bytes(enum ibv_mtu mtu) {
  switch (mtu) {
    case IBV_MTU_256:
      return 256;
    case IBV_MTU_512:
      return 512;
    case IBV_MTU_1024:
      return 1024;
    case IBV_MTU_2048:
      return 2048;
    case IBV_MTU_4096:
      return 4096;
    default:
      return 0;
  }
}

RDMA_Transporter::RDMA_Transporter(TransporterRegistry &reg,
                                   const TransporterConfiguration *config)
    : Transporter(reg, config->transporterIndex, tt_RDMA_TRANSPORTER,
                  config->localHostName, config->remoteHostName, config->s_port,
                  config->isMgmConnection, config->localNodeId,
                  config->remoteNodeId, config->serverNodeId,
                  /*byteorder=*/0, /*compression=*/false, config->checksum,
                  config->signalId,
                  /*max_send_buffer=*/config->rdma.sendBufferSize,
                  config->preSendChecksum, config->rdma.spintime),
      m_send_buffer_size(config->rdma.sendBufferSize),
      m_recv_buffer_size(config->rdma.recvBufferSize),
      m_queue_depth(config->rdma.queueDepth),
      m_inline_threshold(config->rdma.inlineThreshold),
      m_completion_poll_budget(config->rdma.completionPollBudget),
      m_rdma_port(config->rdma.rdmaPort),
      m_gid_index(config->rdma.gidIndex),
      m_traffic_class(config->rdma.trafficClass),
      m_retry_count(config->rdma.retryCount),
      m_rnr_retry_count(config->rdma.rnrRetryCount),
      m_device_name(rdma_clone_device_name(config->rdma.deviceName)),
      m_verbs_ctx(nullptr),
      m_pd(nullptr),
      m_send_cq(nullptr),
      m_recv_cq(nullptr),
      m_qp(nullptr),
      m_send_mr(nullptr),
      m_recv_mr(nullptr),
      m_send_buf(nullptr),
      m_recv_buf(nullptr),
      m_effective_inline_threshold(0),
      m_local_send_seq(0),
      m_local_recv_seq(0),
      m_peer_ack_seq(0),
      m_peer_recv_credits(0),
      m_pending_credit_grant(0),
      m_local_recv_posted(0),
      m_send_slots(nullptr),
      m_send_slots_in_flight(0),
      m_bytes_in_flight(0),
      m_next_send_slot(0),
      m_recv_slots(nullptr),
      m_recv_ready_queue(nullptr),
      m_recv_queue_head(0),
      m_recv_queue_tail(0),
      m_recv_queue_count(0) {}

RDMA_Transporter::RDMA_Transporter(TransporterRegistry &reg,
                                   const RDMA_Transporter *other)
    : Transporter(reg, /*transporter_index=*/0, tt_RDMA_TRANSPORTER,
                  other->localHostName, other->remoteHostName, other->m_s_port,
                  other->isMgmConnection, other->localNodeId,
                  other->remoteNodeId,
                  other->isServer ? other->localNodeId : other->remoteNodeId,
                  /*byteorder=*/0, /*compression=*/false, other->checksumUsed,
                  other->signalIdUsed, other->m_max_send_buffer,
                  other->check_send_checksum, other->m_spintime),
      m_send_buffer_size(other->m_send_buffer_size),
      m_recv_buffer_size(other->m_recv_buffer_size),
      m_queue_depth(other->m_queue_depth),
      m_inline_threshold(other->m_inline_threshold),
      m_completion_poll_budget(other->m_completion_poll_budget),
      m_rdma_port(other->m_rdma_port),
      m_gid_index(other->m_gid_index),
      m_traffic_class(other->m_traffic_class),
      m_retry_count(other->m_retry_count),
      m_rnr_retry_count(other->m_rnr_retry_count),
      m_device_name(rdma_clone_device_name(other->m_device_name)),
      m_verbs_ctx(nullptr),
      m_pd(nullptr),
      m_send_cq(nullptr),
      m_recv_cq(nullptr),
      m_qp(nullptr),
      m_send_mr(nullptr),
      m_recv_mr(nullptr),
      m_send_buf(nullptr),
      m_recv_buf(nullptr),
      m_effective_inline_threshold(0),
      m_local_send_seq(0),
      m_local_recv_seq(0),
      m_peer_ack_seq(0),
      m_peer_recv_credits(0),
      m_pending_credit_grant(0),
      m_local_recv_posted(0),
      m_send_slots(nullptr),
      m_send_slots_in_flight(0),
      m_bytes_in_flight(0),
      m_next_send_slot(0),
      m_recv_slots(nullptr),
      m_recv_ready_queue(nullptr),
      m_recv_queue_head(0),
      m_recv_queue_tail(0),
      m_recv_queue_count(0) {
  /*
   * Gate-3 multi-transporter clone:
   *   - All tuning state (queue depth, buffers, retry counters, device
   *     name, etc.) is copied from `other`.
   *   - Verbs handles (m_verbs_ctx/m_pd/m_send_cq/m_recv_cq/m_qp/MRs)
   *     and per-slot bookkeeping arrays start out NULL; the connect
   *     path will allocate fresh ones for this clone (its own QP, its
   *     own MRs) just like the base transporter.
   *
   * Each clone is fully independent at the verbs level, sharing only
   * the ibv_device identity through configuration. The registry adds
   * the clone to m_node_multi_transporters and the Multi_Transporter
   * wrapper drives parallel doSend()/poll_RDMA() across all clones.
   */
}

RDMA_Transporter::~RDMA_Transporter() {
  /*
   * Defensive cleanup: if a connect path was interrupted, verbs resources
   * may still be allocated when the destructor fires. release_verbs_resources()
   * is idempotent, so the double-call from disconnect paths is harmless.
   */
  release_verbs_resources();
  delete[] m_device_name;
  m_device_name = nullptr;
}

bool RDMA_Transporter::initTransporter() {
  /*
   * Generic init only. Verbs resource allocation is deferred to the
   * connect paths so a transient fabric problem at node startup does not
   * prevent the rest of the cluster from coming up. The verbs allocation
   * itself is exercised inside connect_*_impl().
   */
  return true;
}

bool RDMA_Transporter::configure_derived(
    const TransporterConfiguration *conf) {
  /*
   * Accept any reconfigure attempt whose RDMA tunables match what we were
   * constructed with. The skeleton has no live verbs state to mutate, so
   * we only need to ensure later code paths do not assume different sizes.
   */
  if (conf == nullptr) return false;
  if (conf->type != tt_RDMA_TRANSPORTER) return false;
  if (conf->rdma.sendBufferSize != m_send_buffer_size) return false;
  if (conf->rdma.recvBufferSize != m_recv_buffer_size) return false;
  if (conf->rdma.queueDepth != m_queue_depth) return false;
  return true;
}

bool RDMA_Transporter::connect_server_impl(NdbSocket &&socket) {
  /*
   * Gate-3 server-side connect:
   *   1. Allocate verbs resources (PD/CQ/QP/MR).
   *   2. Run the endpoint exchange and drive the QP through INIT->RTR->RTS.
   *   3. On success, KEEP the verbs resources alive and return true so
   *      that the registry promotes the transporter to CONNECTED and
   *      the existing milestone-7/8/9 send/recv plumbing (doSend(),
   *      poll_RDMA(), performReceive() RDMA arm, reap_recv_completions(),
   *      etc.) begins carrying signals over the QP.
   *
   * On any failure we release everything and return false; the registry's
   * reconnect loop will retry. The control socket is closed in either
   * direction once endpoint exchange completes -- after RTS, all further
   * traffic flows over ibverbs.
   */
  if (!allocate_verbs_resources()) {
    socket.close();
    return false;
  }
  if (!run_endpoint_exchange(socket, "server")) {
    release_verbs_resources();
    socket.close();
    return false;
  }
  log_negotiated_attributes();
  g_eventLogger->info(
      "RDMA[node %u->%u]: server-side QP reached RTS; data path live",
      (unsigned)localNodeId, (unsigned)remoteNodeId);
  /* The control socket is no longer needed for data; all signal traffic
   * now flows over the QP. But the base Transporter contract requires
   * a valid fd in theSocket so the registry's epoll_add() succeeds in
   * report_connect() and isReleased() returns false until
   * releaseAfterDisconnect() runs. The socket sits idle (non-blocking)
   * and only contributes EPOLLHUP if the peer tears down, which we want
   * as a fast-fail signal for link death. SO_KEEPALIVE lets the kernel
   * notice silent peer death on its own. */
  set_get(socket.ndb_socket(), SOL_SOCKET, SO_KEEPALIVE, "SO_KEEPALIVE", 1);
  socket.set_nonblocking(true);
  theSocket = std::move(socket);
  return true;
}

bool RDMA_Transporter::connect_client_impl(NdbSocket &&socket) {
  /* Gate-3 client-side connect -- see connect_server_impl for the full
   * lifecycle. */
  if (!allocate_verbs_resources()) {
    socket.close();
    return false;
  }
  if (!run_endpoint_exchange(socket, "client")) {
    release_verbs_resources();
    socket.close();
    return false;
  }
  log_negotiated_attributes();
  g_eventLogger->info(
      "RDMA[node %u->%u]: client-side QP reached RTS; data path live",
      (unsigned)localNodeId, (unsigned)remoteNodeId);
  set_get(socket.ndb_socket(), SOL_SOCKET, SO_KEEPALIVE, "SO_KEEPALIVE", 1);
  socket.set_nonblocking(true);
  theSocket = std::move(socket);
  return true;
}

void RDMA_Transporter::disconnectImpl() {
  /*
   * No live verbs activity yet, but release any resources that a future
   * connect path may have allocated. The call is a cheap no-op when
   * nothing has been allocated.
   */
  release_verbs_resources();
}

void RDMA_Transporter::releaseAfterDisconnect() {
  release_verbs_resources();
  Transporter::releaseAfterDisconnect();
}

void RDMA_Transporter::resetBuffers() {
  /*
   * resetBuffers() is only ever called when the transporter is in the
   * DISCONNECTED state (Transporter base contract). We treat it as a
   * safety net that releases verbs resources in case a future code path
   * skips disconnectImpl().
   */
  release_verbs_resources();
}

bool RDMA_Transporter::doSend(bool /*need_wakeup*/) {
  /*
   * Milestone-7 send path. Note that under Gate 1 the connect path
   * still refuses the link, so this function is never reached from the
   * registry's performSend(); the implementation exists so unit tests
   * (and later Gate 2) can exercise it directly.
   *
   * Sequence:
   *   1. Reap completions for previously posted sends. This frees
   *      slots and calls iovec_data_sent() for the bytes that the
   *      peer has now reliably received.
   *   2. While we have remote credits and a free slot, fetch iovec
   *      bytes from the send buffer, skip past anything we have
   *      already staged into in-flight slots, build a framed message,
   *      and post one IBV_WR_SEND.
   *
   * Return value follows the existing transporter convention: true if
   * there are still bytes to push (more work to do later), false if
   * the buffer is drained and all in-flight WRs are reaped.
   */
  if (m_qp == nullptr || m_send_slots == nullptr) {
    /* Connect path has not allocated verbs resources yet. Nothing to
     * send and nothing to fail; the registry retains the bytes in its
     * send buffer for the next attempt. */
    return false;
  }

  const int reaped = reap_send_completions();
  if (reaped < 0) {
    /* Fatal CQ error: link is being torn down. Report no further
     * pending work; the disconnect logic owns recovery from here. */
    return false;
  }

  const Uint32 slot_size = send_slot_size_or_zero();
  if (slot_size == 0) return false;
  const Uint32 max_payload = slot_size - (Uint32)RDMA_MSG_HEADER_BYTES;

  /*
   * Drain loop. Each iteration tries to fill exactly one slot with
   * whole Protocol6 messages. We bail out as soon as we run out of
   * credits, free slots, or send-buffer data beyond what we have
   * already staged. Messages are never split across two RDMA recv
   * slots, because the receiver's unpacker (TransporterRegistry::
   * unpack -> Packer::unpack_one) can only consume complete Protocol6
   * messages from a contiguous buffer, and a single recv slot is
   * what get_next_read() exposes as a contiguous buffer to the
   * unpacker. The TCP transporter side-steps this because its
   * receive buffer is a contiguous ring; RDMA cannot share that
   * shortcut.
   */
  while (m_send_slots_in_flight < m_queue_depth && m_peer_recv_credits > 0) {
    /* Pull iovec descriptors describing the send buffer's pending data. */
    constexpr Uint32 IOV_BATCH = 8;
    struct iovec iov[IOV_BATCH];
    const Uint32 n_iov = fetch_send_iovec_data(iov, IOV_BATCH);
    if (n_iov == 0) {
      /* Send buffer is empty (or our skip ate it all). */
      break;
    }

    /*
     * Skip past bytes already staged into in-flight slots. After the
     * signal-aligned packing below, m_bytes_in_flight is always a
     * sum of whole-signal byte counts, so the cursor lands cleanly
     * on a signal boundary. We have not called iovec_data_sent() for
     * those bytes yet, so fetch returns them again on every call
     * until the corresponding completions arrive.
     */
    Uint32 skip = m_bytes_in_flight;
    Uint32 cursor_iov = 0;
    while (cursor_iov < n_iov && skip >= iov[cursor_iov].iov_len) {
      skip -= iov[cursor_iov].iov_len;
      cursor_iov++;
    }
    if (cursor_iov >= n_iov) {
      /* All currently visible bytes are already in flight. Nothing to
       * post until the next completion arrives. */
      break;
    }
    /* Byte offset within iov[cursor_iov] of the next unsent signal. */
    Uint32 cursor_off = skip;

    /* Find a free slot. */
    const Uint32 slot = find_free_send_slot();
    if (slot == UINT32_MAX) {
      /* Out of slots even though m_send_slots_in_flight < queue_depth?
       * Should not happen, but bail out safely. */
      break;
    }

    char *const slot_buf =
        (char *)m_send_buf + (size_t)slot * (size_t)slot_size;
    char *payload_dst = slot_buf + RDMA_MSG_HEADER_BYTES;
    Uint32 payload_len = 0;

    /*
     * Pack only *complete* Protocol6 messages into the slot. The
     * upper-layer send buffer guarantees that each iovec entry is a
     * sequence of whole Protocol6 messages: SendBufferPage / thr_send_page
     * allocate a fresh page whenever a message will not fit in the
     * current one, and MAX_SEND_MESSAGE_BYTESIZE <= page max_bytes().
     * Therefore peeking the next message's word1 only ever needs to
     * read 4 bytes within the current iovec entry; if those 4 bytes
     * are not available, the iovec has been fully consumed for this
     * slot and we post what we have.
     *
     * If the upcoming message would exceed the remaining slot room
     * we stop and post the slot as-is, leaving the remaining bytes
     * in the upper-layer send buffer for the next slot. This is the
     * invariant that prevents a Protocol6 message from being split
     * across two RDMA receive slots, which is what wedged the scan
     * receive path on the receiver side.
     */
    while (payload_len < max_payload && cursor_iov < n_iov) {
      const Uint32 iov_remaining =
          (Uint32)iov[cursor_iov].iov_len - cursor_off;
      if (iov_remaining == 0) {
        /* Reached end of this iov entry; move to next. */
        cursor_iov++;
        cursor_off = 0;
        continue;
      }
      if (iov_remaining < (Uint32)sizeof(Uint32)) {
        /*
         * Fewer than 4 bytes left in this iov to peek word1. The
         * upper-layer invariant says signals do not straddle pages,
         * so a non-zero sub-4-byte remainder would indicate buffer
         * corruption or a signal split across iovecs. Refuse to post
         * any further bytes this round; the next doSend() will fetch
         * a fresh iovec batch after completions arrive.
         */
        g_eventLogger->error(
            "RDMA[node %u->%u]: send buffer iov entry %u has %u bytes"
            " left, expected a full Protocol6 header; bailing this"
            " round",
            (unsigned)localNodeId, (unsigned)remoteNodeId, cursor_iov,
            iov_remaining);
        break;
      }
      /*
       * Read word1 of the next Protocol6 message. Packer writes the
       * header in host byte order; both sender and receiver use
       * Protocol6::getMessageLength() on the host-endian word1 to
       * extract the length.
       */
      Uint32 word1;
      std::memcpy(&word1,
                  (const char *)iov[cursor_iov].iov_base + cursor_off,
                  sizeof(word1));
      const Uint32 msg_len_bytes =
          (Uint32)Protocol6::getMessageLength(word1) * 4u;
      if (msg_len_bytes < (Uint32)sizeof(Uint32) ||
          msg_len_bytes > (Uint32)MAX_SEND_MESSAGE_BYTESIZE) {
        /*
         * Corrupt buffer or signal larger than the protocol allows.
         * Surfacing this is fatal for the link; otherwise we would
         * silently wedge or send garbage.
         */
        g_eventLogger->error(
            "RDMA[node %u->%u]: invalid Protocol6 message length %u "
            "in send buffer (word1=0x%08x); disconnecting",
            (unsigned)localNodeId, (unsigned)remoteNodeId, msg_len_bytes,
            word1);
        m_stats.qp_fatal_events++;
        report_error(TE_RDMA_QP_ERROR,
                     "invalid Protocol6 length in send buffer");
        start_disconnecting(EBADMSG, /*send_source=*/true);
        return false;
      }
      if (msg_len_bytes > iov_remaining) {
        /*
         * The full message is not yet within the current iovec
         * batch. This can only happen if IOV_BATCH was too small
         * to expose the page that holds this message; bail and let
         * the next doSend() pull more iovecs. Because pages cannot
         * span signals, this situation also implies cursor_off == 0,
         * but we do not assert it here so a misbehaving upper layer
         * cannot escalate to abort().
         */
        break;
      }
      if (payload_len + msg_len_bytes > max_payload) {
        /*
         * Next message doesn't fit in this slot's remaining room.
         * Post what we have; the next slot will carry this message.
         * This is the key invariant for the receive side.
         */
        break;
      }
      std::memcpy(payload_dst,
                  (const char *)iov[cursor_iov].iov_base + cursor_off,
                  msg_len_bytes);
      payload_dst += msg_len_bytes;
      payload_len += msg_len_bytes;
      cursor_off += msg_len_bytes;
      if (cursor_off == iov[cursor_iov].iov_len) {
        cursor_iov++;
        cursor_off = 0;
      }
    }
    if (payload_len == 0) {
      /*
       * Either we caught up to in-flight bytes, or the next message
       * is not yet fully visible in the iovec batch, or (handled
       * earlier) the next message is larger than the slot. In every
       * case there is nothing useful to post this round.
       */
      break;
    }

    /* Build the framing header. We piggyback the pending ack and
     * credit grant on this SEND. */
    const Uint16 credit_delta = (m_pending_credit_grant > 0xFFFFu)
                                    ? (Uint16)0xFFFFu
                                    : (Uint16)m_pending_credit_grant;
    encode_msg_header(slot_buf, payload_len, m_local_send_seq,
                      m_local_recv_seq, credit_delta, /*flags=*/0u);

    /* Post the WR. */
    struct ibv_sge sge;
    std::memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)slot_buf;
    sge.length = payload_len + (Uint32)RDMA_MSG_HEADER_BYTES;
    sge.lkey = m_send_mr->lkey;

    struct ibv_send_wr wr;
    std::memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)slot;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    /* Use inline send for small messages within the device's negotiated
     * cap. This avoids the HCA touching pinned memory at the cost of a
     * copy into the WR; the verbs implementation handles the actual
     * copy at post time so SGE.addr can remain inside the registered
     * MR. */
    const bool inline_used = (sge.length <= m_effective_inline_threshold);
    if (inline_used) {
      wr.send_flags |= IBV_SEND_INLINE;
    }

    struct ibv_send_wr *bad = nullptr;
    const int rc = ibv_post_send(m_qp, &wr, &bad);
    if (rc != 0) {
      g_eventLogger->error(
          "RDMA[node %u->%u]: ibv_post_send failed (rc=%d errno=%d %s); "
          "disconnecting",
          (unsigned)localNodeId, (unsigned)remoteNodeId, rc, errno,
          std::strerror(errno));
      m_stats.qp_fatal_events++;
      report_error(TE_RDMA_QP_ERROR, "ibv_post_send failed");
      start_disconnecting(rc, /*send_source=*/true);
      return false;
    }

    /* Commit state changes only after a successful post. The
     * iovec_data_sent() call is deferred until the completion arrives;
     * see reap_send_completions(). */
    m_send_slots[slot].bytes_consumed = payload_len;
    m_send_slots[slot].in_flight = true;
    m_send_slots_in_flight++;
    m_bytes_in_flight += payload_len;
    m_local_send_seq++;
    m_peer_recv_credits--;
    /* We just transmitted whatever credits we had queued. */
    m_pending_credit_grant = 0;

    /* Observability bumps: count this WR as posted, optionally as an
     * inline send, and record the staged payload bytes. */
    m_stats.send_posted++;
    if (inline_used) m_stats.send_inline++;
    m_stats.copied_send_bytes += payload_len;
  }

  /*
   * CREDIT_ONLY refill: if the receiver has accumulated enough credit
   * grants to give back to the peer but the main loop above had no
   * outgoing data to piggy-back them on, post an explicit zero-payload
   * CREDIT_ONLY message. Without this, an asymmetric multi-transporter
   * clone (e.g. one that mostly carries inbound GCP_COMMIT to this
   * node) will accumulate pending grants forever and the peer's
   * m_peer_recv_credits will drain to zero, stalling further sends in
   * that direction. The GCP_COMMIT lag pattern we observed under
   * benchmark load (m_count=1 against the peer with no qp_fatal/RNR
   * markers) is the visible symptom.
   *
   * Threshold: emit when pending grants reach half the queue depth.
   * This amortizes the WR cost across many real grants while keeping
   * enough headroom that the peer never sees fewer than queue_depth/2
   * credits even in pathological cases. We additionally fire when
   * pending grants exceed 0 and the peer's own credit pool against
   * us looks low (m_peer_recv_credits below queue_depth/4), because
   * if both sides are running low we want to refresh aggressively
   * to avoid a mutual-stall deadlock.
   */
  const Uint32 credit_threshold =
      m_queue_depth > 1 ? (m_queue_depth / 2) : 1u;
  const Uint32 low_credit_threshold =
      m_queue_depth > 3 ? (m_queue_depth / 4) : 1u;
  const bool needs_refill =
      (m_pending_credit_grant >= credit_threshold) ||
      (m_pending_credit_grant > 0 &&
       m_peer_recv_credits < low_credit_threshold);
  if (needs_refill && m_send_slots_in_flight < m_queue_depth &&
      m_peer_recv_credits > 0) {
    const Uint32 slot = find_free_send_slot();
    if (slot != UINT32_MAX) {
      char *const slot_buf =
          (char *)m_send_buf + (size_t)slot * (size_t)slot_size;
      const Uint16 credit_delta = (m_pending_credit_grant > 0xFFFFu)
                                      ? (Uint16)0xFFFFu
                                      : (Uint16)m_pending_credit_grant;
      encode_msg_header(slot_buf, /*payload_len=*/0u, m_local_send_seq,
                        m_local_recv_seq, credit_delta,
                        RDMA_MSG_FLAG_CREDIT_ONLY);

      struct ibv_sge sge;
      std::memset(&sge, 0, sizeof(sge));
      sge.addr = (uintptr_t)slot_buf;
      sge.length = (Uint32)RDMA_MSG_HEADER_BYTES;
      sge.lkey = m_send_mr->lkey;

      struct ibv_send_wr wr;
      std::memset(&wr, 0, sizeof(wr));
      wr.wr_id = (uint64_t)slot;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.opcode = IBV_WR_SEND;
      wr.send_flags = IBV_SEND_SIGNALED;
      const bool credit_inline_used =
          (sge.length <= m_effective_inline_threshold);
      if (credit_inline_used) {
        wr.send_flags |= IBV_SEND_INLINE;
      }

      struct ibv_send_wr *bad = nullptr;
      const int rc = ibv_post_send(m_qp, &wr, &bad);
      if (rc != 0) {
        g_eventLogger->error(
            "RDMA[node %u->%u]: ibv_post_send(CREDIT_ONLY) failed (rc=%d "
            "errno=%d %s); disconnecting",
            (unsigned)localNodeId, (unsigned)remoteNodeId, rc, errno,
            std::strerror(errno));
        m_stats.qp_fatal_events++;
        report_error(TE_RDMA_QP_ERROR, "ibv_post_send(CREDIT_ONLY) failed");
        start_disconnecting(rc, /*send_source=*/true);
        return false;
      }

      /*
       * Commit state. bytes_consumed stays at 0 so reap_send_completions
       * does not call iovec_data_sent() (there are no upper-layer bytes
       * tied to this WR). m_bytes_in_flight is also unchanged.
       */
      m_send_slots[slot].bytes_consumed = 0;
      m_send_slots[slot].in_flight = true;
      m_send_slots_in_flight++;
      m_local_send_seq++;
      m_peer_recv_credits--;
      m_pending_credit_grant = 0;
      m_stats.send_credit_only_out++;
      m_stats.send_posted++;
      if (credit_inline_used) m_stats.send_inline++;
    }
  }

  /*
   * If the loop exited because the peer-credit pool is empty while we
   * still have free slots, this is a credit-bound stall. Note we do
   * not differentiate "no data to send" from "data to send but no
   * credits"; in steady state the registry only calls doSend() when
   * there are bytes pending, so the over-counting in idle periods is
   * acceptable for an operator-facing diagnostic.
   */
  if (m_peer_recv_credits == 0 && m_send_slots_in_flight < m_queue_depth) {
    m_stats.send_credit_stalls++;
  }

  /* Tell the caller whether more work is outstanding. The registry
   * loop will keep polling us if any of these conditions hold. */
  return m_send_slots_in_flight > 0;
}

bool RDMA_Transporter::send_is_possible(int /*timeout_millisec*/) const {
  /*
   * Send is "possible" if we have at least one free slot AND at least
   * one credit against the peer's recv queue. The registry uses this
   * to decide whether retrying with a brief wait is worthwhile; we
   * answer based on current state only, not a timed sleep, because the
   * caller already implements its own sleep loop.
   */
  if (m_qp == nullptr || m_send_slots == nullptr) return false;
  if (m_send_slots_in_flight >= m_queue_depth) return false;
  if (m_peer_recv_credits == 0) return false;
  return true;
}

bool RDMA_Transporter::send_limit_reached(int bufsize) {
  /*
   * Mirror the TCP/SHM convention: the buffer is considered "over
   * limit" once its used byte count crosses the configured
   * OverloadLimit. doSend() picks up the work; the registry then
   * decides whether to force-send.
   */
  if (m_overload_limit == 0) return false;
  return (Uint32)bufsize >= m_overload_limit;
}

/*
 * --------------------------------------------------------------------------
 *  Verbs resource lifecycle
 * --------------------------------------------------------------------------
 *
 * allocate_verbs_resources() walks the libibverbs API in strict order:
 *   1. Open the configured device (or first available) and query its
 *      capabilities.
 *   2. Validate the requested port is ACTIVE and pick MTU/LID/GID values
 *      we will eventually use during QP transitions.
 *   3. Allocate the Protection Domain.
 *   4. Allocate two Completion Queues sized at queue_depth * 2 so the
 *      send and receive halves cannot starve each other.
 *   5. Allocate page-aligned staging buffers and register them as MRs
 *      with LOCAL_WRITE access (no remote READ/WRITE/atomics are needed
 *      for the SEND/RECV-only design).
 *   6. Create the reliable-connected QP with the configured queue depths
 *      and inline-data threshold; clamp the threshold to whatever the
 *      device returned in qp_init_attr.cap.max_inline_data.
 *
 * On any failure we tear down everything allocated so far via
 * release_verbs_resources() and return false. Errors are recorded with
 * g_eventLogger->info()/error() so operators see what went wrong without
 * having to enable debug logging.
 */
bool RDMA_Transporter::allocate_verbs_resources() {
  /*
   * Every entry into this function corresponds to a fresh attempt at
   * setting up the link: either the first connect or a reconnect after
   * a previous teardown. Increment up front (before the require()
   * checks) so even failed allocate attempts are visible in the
   * counter. Note: an abort() inside one of the require()s is treated
   * as a programming error, not a counted event.
   */
  m_stats.reconnect_attempts++;

  /* Sanity-check that we are starting from a clean slate. */
  require(m_verbs_ctx == nullptr);
  require(m_pd == nullptr);
  require(m_send_cq == nullptr);
  require(m_recv_cq == nullptr);
  require(m_qp == nullptr);
  require(m_send_mr == nullptr);
  require(m_recv_mr == nullptr);
  require(m_send_buf == nullptr);
  require(m_recv_buf == nullptr);

  /* Step 1: device discovery & open. */
  int num_devices = 0;
  struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
  if (dev_list == nullptr || num_devices == 0) {
    g_eventLogger->error(
        "RDMA: ibv_get_device_list returned no devices (errno=%d %s)", errno,
        std::strerror(errno));
    if (dev_list != nullptr) ibv_free_device_list(dev_list);
    return false;
  }

  struct ibv_device *selected = nullptr;
  if (m_device_name == nullptr) {
    /* No name configured: pick the first device. */
    selected = dev_list[0];
  } else {
    for (int i = 0; i < num_devices && dev_list[i] != nullptr; i++) {
      const char *name = ibv_get_device_name(dev_list[i]);
      if (name != nullptr && std::strcmp(name, m_device_name) == 0) {
        selected = dev_list[i];
        break;
      }
    }
    if (selected == nullptr) {
      g_eventLogger->error(
          "RDMA: configured device '%s' not found among %d available HCAs",
          m_device_name, num_devices);
      ibv_free_device_list(dev_list);
      return false;
    }
  }

  m_verbs_ctx = ibv_open_device(selected);
  /*
   * ibv_open_device() returns a context that retains an internal
   * reference to the device; the device list itself can be freed
   * immediately after.
   */
  ibv_free_device_list(dev_list);
  if (m_verbs_ctx == nullptr) {
    g_eventLogger->error("RDMA: ibv_open_device failed (errno=%d %s)", errno,
                         std::strerror(errno));
    return false;
  }

  /* Step 2: device & port queries, capability validation. */
  struct ibv_device_attr dev_attr;
  std::memset(&dev_attr, 0, sizeof(dev_attr));
  if (ibv_query_device(m_verbs_ctx, &dev_attr) != 0) {
    g_eventLogger->error("RDMA: ibv_query_device failed (errno=%d %s)", errno,
                         std::strerror(errno));
    release_verbs_resources();
    return false;
  }
  if (m_queue_depth > (Uint32)dev_attr.max_qp_wr) {
    g_eventLogger->error(
        "RDMA: configured RdmaQueueDepth=%u exceeds device max_qp_wr=%d",
        m_queue_depth, dev_attr.max_qp_wr);
    release_verbs_resources();
    return false;
  }
  if ((int)(m_queue_depth * 2) > dev_attr.max_cqe) {
    g_eventLogger->error(
        "RDMA: 2 * RdmaQueueDepth=%u exceeds device max_cqe=%d",
        m_queue_depth, dev_attr.max_cqe);
    release_verbs_resources();
    return false;
  }

  struct ibv_port_attr port_attr;
  std::memset(&port_attr, 0, sizeof(port_attr));
  if (ibv_query_port(m_verbs_ctx, (uint8_t)m_rdma_port, &port_attr) != 0) {
    g_eventLogger->error(
        "RDMA: ibv_query_port(port=%u) failed (errno=%d %s)", m_rdma_port,
        errno, std::strerror(errno));
    release_verbs_resources();
    return false;
  }
  if (port_attr.state != IBV_PORT_ACTIVE) {
    g_eventLogger->error(
        "RDMA: HCA port %u is not active (state=%d); refusing to allocate QP",
        m_rdma_port, (int)port_attr.state);
    release_verbs_resources();
    return false;
  }

  /* Step 3: Protection Domain. */
  m_pd = ibv_alloc_pd(m_verbs_ctx);
  if (m_pd == nullptr) {
    g_eventLogger->error("RDMA: ibv_alloc_pd failed (errno=%d %s)", errno,
                         std::strerror(errno));
    release_verbs_resources();
    return false;
  }

  /* Step 4: Completion Queues. We size each CQ at 2 * queue_depth so a
   * receive WC backlog cannot starve send completions, even when the
   * future receive path lazily reaps WCs. */
  m_send_cq = ibv_create_cq(m_verbs_ctx, (int)(m_queue_depth * 2),
                            /*cq_context=*/nullptr, /*channel=*/nullptr,
                            /*comp_vector=*/0);
  if (m_send_cq == nullptr) {
    g_eventLogger->error("RDMA: ibv_create_cq(send) failed (errno=%d %s)",
                         errno, std::strerror(errno));
    release_verbs_resources();
    return false;
  }
  m_recv_cq = ibv_create_cq(m_verbs_ctx, (int)(m_queue_depth * 2),
                            /*cq_context=*/nullptr, /*channel=*/nullptr,
                            /*comp_vector=*/0);
  if (m_recv_cq == nullptr) {
    g_eventLogger->error("RDMA: ibv_create_cq(recv) failed (errno=%d %s)",
                         errno, std::strerror(errno));
    release_verbs_resources();
    return false;
  }

  /* Step 5: staging buffers + memory regions. */
  m_send_buf = rdma_aligned_alloc(m_send_buffer_size);
  if (m_send_buf == nullptr) {
    g_eventLogger->error(
        "RDMA: failed to allocate %u bytes of send staging memory",
        m_send_buffer_size);
    release_verbs_resources();
    return false;
  }
  m_recv_buf = rdma_aligned_alloc(m_recv_buffer_size);
  if (m_recv_buf == nullptr) {
    g_eventLogger->error(
        "RDMA: failed to allocate %u bytes of recv staging memory",
        m_recv_buffer_size);
    release_verbs_resources();
    return false;
  }

  /* LOCAL_WRITE is the minimum required for ibv_post_recv() to write
   * incoming bytes into the buffer; we do not enable remote access
   * because the SEND/RECV design never grants peers an rkey. */
  m_send_mr = ibv_reg_mr(m_pd, m_send_buf, (size_t)m_send_buffer_size,
                         IBV_ACCESS_LOCAL_WRITE);
  if (m_send_mr == nullptr) {
    g_eventLogger->error("RDMA: ibv_reg_mr(send) failed (errno=%d %s)", errno,
                         std::strerror(errno));
    release_verbs_resources();
    return false;
  }
  m_recv_mr = ibv_reg_mr(m_pd, m_recv_buf, (size_t)m_recv_buffer_size,
                         IBV_ACCESS_LOCAL_WRITE);
  if (m_recv_mr == nullptr) {
    g_eventLogger->error("RDMA: ibv_reg_mr(recv) failed (errno=%d %s)", errno,
                         std::strerror(errno));
    release_verbs_resources();
    return false;
  }

  /* Step 6: Queue Pair (RC). */
  struct ibv_qp_init_attr qp_init;
  std::memset(&qp_init, 0, sizeof(qp_init));
  qp_init.send_cq = m_send_cq;
  qp_init.recv_cq = m_recv_cq;
  qp_init.qp_type = IBV_QPT_RC;
  qp_init.sq_sig_all = 0;  // Caller picks per-WR signaling.
  qp_init.cap.max_send_wr = m_queue_depth;
  qp_init.cap.max_recv_wr = m_queue_depth;
  qp_init.cap.max_send_sge = 1;
  qp_init.cap.max_recv_sge = 1;
  qp_init.cap.max_inline_data = m_inline_threshold;

  m_qp = ibv_create_qp(m_pd, &qp_init);
  if (m_qp == nullptr) {
    g_eventLogger->error("RDMA: ibv_create_qp failed (errno=%d %s)", errno,
                         std::strerror(errno));
    release_verbs_resources();
    return false;
  }

  /* The verbs implementation may return a smaller inline limit than we
   * requested. Cache the effective value so future send paths never
   * exceed it. */
  m_effective_inline_threshold = qp_init.cap.max_inline_data;

  /* Cache the negotiated MTU value in bytes for logging only. The wire
   * protocol (Milestone 6) will encode MTU separately. */
  (void)rdma_mtu_to_bytes(port_attr.active_mtu);

  /* Step 7: allocate per-slot bookkeeping for the send and receive
   * paths. We do this last because release_verbs_resources() unwinds
   * in reverse order; freeing these arrays before destroying the QP is
   * harmless since they do not hold any verbs handles. */
  if (!allocate_send_slot_state()) {
    release_verbs_resources();
    return false;
  }
  if (!allocate_recv_slot_state()) {
    release_verbs_resources();
    return false;
  }

  return true;
}

void RDMA_Transporter::release_verbs_resources() {
  /*
   * Emit a final per-link counter snapshot before tearing the verbs
   * resources down so operators see the cumulative work-completion
   * tally for the link that just closed. Gated on at least one
   * counter being non-zero so the destructor on an unused transporter
   * does not produce noise. log_stats() never mutates state.
   */
  if (m_stats.send_posted != 0 || m_stats.recv_completions_ok != 0 ||
      m_stats.recv_credit_only_in != 0 || m_stats.send_completion_errors != 0 ||
      m_stats.recv_completion_errors != 0 ||
      m_stats.reconnect_attempts != 0) {
    log_stats();
  }

  /*
   * Destruction order is the reverse of construction. Each step ignores
   * its return value because there is nothing meaningful we can do in
   * the failure case beyond logging, and we already log allocation
   * failures elsewhere.
   */
  if (m_qp != nullptr) {
    ibv_destroy_qp(m_qp);
    m_qp = nullptr;
  }
  if (m_recv_cq != nullptr) {
    ibv_destroy_cq(m_recv_cq);
    m_recv_cq = nullptr;
  }
  if (m_send_cq != nullptr) {
    ibv_destroy_cq(m_send_cq);
    m_send_cq = nullptr;
  }
  if (m_recv_mr != nullptr) {
    ibv_dereg_mr(m_recv_mr);
    m_recv_mr = nullptr;
  }
  if (m_send_mr != nullptr) {
    ibv_dereg_mr(m_send_mr);
    m_send_mr = nullptr;
  }
  if (m_recv_buf != nullptr) {
    std::free(m_recv_buf);
    m_recv_buf = nullptr;
  }
  if (m_send_buf != nullptr) {
    std::free(m_send_buf);
    m_send_buf = nullptr;
  }
  if (m_pd != nullptr) {
    ibv_dealloc_pd(m_pd);
    m_pd = nullptr;
  }
  if (m_verbs_ctx != nullptr) {
    ibv_close_device(m_verbs_ctx);
    m_verbs_ctx = nullptr;
  }
  release_send_slot_state();
  release_recv_slot_state();
  m_effective_inline_threshold = 0;
  reset_wire_state();
}

void RDMA_Transporter::reset_wire_state() {
  /*
   * Connect-time defaults. The credit pool starts empty because the
   * peer advertises its initial credit grant via the endpoint exchange
   * record (see run_endpoint_exchange()), and we use Milestone 7 logic
   * to seed m_peer_recv_credits from that grant rather than from
   * configuration. Sequence numbers start at 0 so the first outbound
   * SEND on a fresh link carries seq=0 and so does the peer's first
   * received SEND.
   */
  m_local_send_seq = 0;
  m_local_recv_seq = 0;
  m_peer_ack_seq = 0;
  m_peer_recv_credits = 0;
  m_pending_credit_grant = 0;
  m_local_recv_posted = 0;
  m_send_slots_in_flight = 0;
  m_bytes_in_flight = 0;
  m_next_send_slot = 0;
  m_recv_queue_head = 0;
  m_recv_queue_tail = 0;
  m_recv_queue_count = 0;
}

Uint32 RDMA_Transporter::send_slot_size_or_zero() const {
  if (m_queue_depth == 0) {
    g_eventLogger->error(
        "RDMA: queue depth is zero; cannot compute send slot size");
    return 0;
  }
  const Uint32 slot = m_send_buffer_size / m_queue_depth;
  const Uint32 minimum =
      (Uint32)RDMA_MSG_HEADER_BYTES + (Uint32)MAX_SEND_MESSAGE_BYTESIZE;
  if (slot < minimum) {
    g_eventLogger->error(
        "RDMA: send slot size %u (RdmaSendBufferMemory=%u / "
        "RdmaQueueDepth=%u) is below required minimum %u (24-byte header + "
        "%u-byte max signal). Increase RdmaSendBufferMemory or decrease "
        "RdmaQueueDepth.",
        slot, m_send_buffer_size, m_queue_depth, minimum,
        (unsigned)MAX_SEND_MESSAGE_BYTESIZE);
    return 0;
  }
  return slot;
}

bool RDMA_Transporter::allocate_send_slot_state() {
  /* Validate geometry up front so we never emit a misleading "out of
   * memory" message when the real problem is misconfigured tunables. */
  if (send_slot_size_or_zero() == 0) return false;
  require(m_send_slots == nullptr);
  m_send_slots = new (std::nothrow) rdma_send_slot[m_queue_depth];
  if (m_send_slots == nullptr) {
    g_eventLogger->error(
        "RDMA: failed to allocate %u-entry send slot table", m_queue_depth);
    return false;
  }
  for (Uint32 i = 0; i < m_queue_depth; i++) {
    m_send_slots[i].bytes_consumed = 0;
    m_send_slots[i].in_flight = false;
  }
  m_send_slots_in_flight = 0;
  m_bytes_in_flight = 0;
  m_next_send_slot = 0;
  return true;
}

void RDMA_Transporter::release_send_slot_state() {
  delete[] m_send_slots;
  m_send_slots = nullptr;
  m_send_slots_in_flight = 0;
  m_bytes_in_flight = 0;
  m_next_send_slot = 0;
}

Uint32 RDMA_Transporter::find_free_send_slot() {
  if (m_send_slots == nullptr || m_queue_depth == 0) return UINT32_MAX;
  /* Linear scan starting from the round-robin hint. Worst case we touch
   * every slot once; for queue depths up to a few thousand this is
   * faster than maintaining a separate free-list. */
  for (Uint32 step = 0; step < m_queue_depth; step++) {
    const Uint32 idx = (m_next_send_slot + step) % m_queue_depth;
    if (!m_send_slots[idx].in_flight) {
      m_next_send_slot = (idx + 1) % m_queue_depth;
      return idx;
    }
  }
  return UINT32_MAX;
}

int RDMA_Transporter::reap_send_completions() {
  if (m_send_cq == nullptr || m_send_slots == nullptr) return 0;

  /*
   * Reap completions in batches up to the configured poll budget. We
   * cap the on-stack work-completion array at 64 to avoid blowing up
   * the stack frame on platforms with small default thread stacks;
   * larger budgets are handled with multiple iterations.
   */
  constexpr int CHUNK = 32;
  struct ibv_wc wc[CHUNK];
  Uint32 remaining = m_completion_poll_budget;
  if (remaining == 0) remaining = (Uint32)CHUNK;
  int reaped_total = 0;

  while (remaining > 0) {
    const int take = (remaining > (Uint32)CHUNK) ? CHUNK : (int)remaining;
    m_stats.cq_polls_send++;
    const int n = ibv_poll_cq(m_send_cq, take, wc);
    if (n < 0) {
      g_eventLogger->error(
          "RDMA[node %u->%u]: ibv_poll_cq(send) returned %d (errno=%d %s)",
          (unsigned)localNodeId, (unsigned)remoteNodeId, n, errno,
          std::strerror(errno));
      m_stats.qp_fatal_events++;
      report_error(TE_RDMA_CQ_ERROR, "ibv_poll_cq(send) returned error");
      start_disconnecting(n, /*send_source=*/true);
      return -1;
    }
    if (n == 0) break;

    for (int i = 0; i < n; i++) {
      const Uint32 slot = (Uint32)wc[i].wr_id;
      if (slot >= m_queue_depth || !m_send_slots[slot].in_flight) {
        g_eventLogger->error(
            "RDMA[node %u->%u]: stale send WC wr_id=%llu (queue_depth=%u, "
            "in_flight=%u)",
            (unsigned)localNodeId, (unsigned)remoteNodeId,
            (unsigned long long)wc[i].wr_id, m_queue_depth,
            slot < m_queue_depth ? m_send_slots[slot].in_flight : 0u);
        m_stats.qp_fatal_events++;
        report_error(TE_RDMA_CQ_ERROR, "stale send completion wr_id");
        start_disconnecting(EINVAL, /*send_source=*/true);
        return -1;
      }

      if (wc[i].status != IBV_WC_SUCCESS) {
        /* Map IBV_WC_RETRY_EXC_ERR and IBV_WC_RNR_RETRY_EXC_ERR to
         * their RonDB equivalents so observability tooling can
         * distinguish them from a generic CQ error. */
        TransporterError err = TE_RDMA_CQ_ERROR;
        if (wc[i].status == IBV_WC_RETRY_EXC_ERR ||
            wc[i].status == IBV_WC_RNR_RETRY_EXC_ERR) {
          err = TE_RDMA_RETRY_EXHAUSTED;
        }
        /* Counter buckets: every error contributes to
         * send_completion_errors; specific statuses also contribute to
         * the targeted RNR / retry counters. They are NOT mutually
         * exclusive on purpose. */
        m_stats.send_completion_errors++;
        if (wc[i].status == IBV_WC_RNR_RETRY_EXC_ERR) {
          m_stats.rnr_events++;
        } else if (wc[i].status == IBV_WC_RETRY_EXC_ERR) {
          m_stats.retry_exceeded_events++;
        }
        g_eventLogger->error(
            "RDMA[node %u->%u]: send WC failure wr_id=%u status=%d (%s)",
            (unsigned)localNodeId, (unsigned)remoteNodeId, slot,
            (int)wc[i].status, ibv_wc_status_str(wc[i].status));
        report_error(err, "RDMA send completion failure");
        start_disconnecting((int)wc[i].status, /*send_source=*/true);
        return -1;
      }

      /* Success: free the slot and release the bytes back to the
       * send-buffer accounting. */
      const Uint32 bytes = m_send_slots[slot].bytes_consumed;
      m_send_slots[slot].in_flight = false;
      m_send_slots[slot].bytes_consumed = 0;
      require(m_send_slots_in_flight > 0);
      m_send_slots_in_flight--;
      require(m_bytes_in_flight >= bytes);
      m_bytes_in_flight -= bytes;
      if (bytes > 0) {
        iovec_data_sent((int)bytes);
      }
      /* Bookkeeping: the wire bytes consumed by the HCA equal the
       * header plus the staged payload. Match the TCP convention of
       * tracking total bytes successfully transmitted. */
      m_bytes_sent += (Uint64)RDMA_MSG_HEADER_BYTES + (Uint64)bytes;
      m_stats.send_completions_ok++;
      reaped_total++;
    }

    remaining -= (Uint32)n;
    if (n < take) break;  /* CQ drained */
  }
  if (remaining == 0 && reaped_total > 0) {
    /*
     * The poll budget bounded our work this round but there may still
     * be completions ready in the CQ. Surface this so operators can
     * tune RdmaCompletionPollBudget against observed budget hits.
     */
    m_stats.cq_budget_hits_send++;
  }
  return reaped_total;
}

bool RDMA_Transporter::allocate_recv_slot_state() {
  require(m_recv_slots == nullptr);
  require(m_recv_ready_queue == nullptr);
  m_recv_slots = new (std::nothrow) rdma_recv_slot[m_queue_depth];
  m_recv_ready_queue = new (std::nothrow) Uint32[m_queue_depth];
  if (m_recv_slots == nullptr || m_recv_ready_queue == nullptr) {
    g_eventLogger->error(
        "RDMA: failed to allocate recv slot state (queue_depth=%u)",
        m_queue_depth);
    release_recv_slot_state();
    return false;
  }
  for (Uint32 i = 0; i < m_queue_depth; i++) {
    m_recv_slots[i].payload_len = 0;
    m_recv_slots[i].read_offset = 0;
    m_recv_ready_queue[i] = 0;
  }
  m_recv_queue_head = 0;
  m_recv_queue_tail = 0;
  m_recv_queue_count = 0;
  return true;
}

void RDMA_Transporter::release_recv_slot_state() {
  delete[] m_recv_slots;
  m_recv_slots = nullptr;
  delete[] m_recv_ready_queue;
  m_recv_ready_queue = nullptr;
  m_recv_queue_head = 0;
  m_recv_queue_tail = 0;
  m_recv_queue_count = 0;
}

int RDMA_Transporter::reap_recv_completions(
    TransporterReceiveHandle &recvdata) {
  if (m_recv_cq == nullptr || m_recv_slots == nullptr) return 0;

  /*
   * Same chunking strategy as reap_send_completions(): bounded by the
   * configured budget, capped at 32 WCs per ibv_poll_cq() call to keep
   * stack usage predictable.
   */
  constexpr int CHUNK = 32;
  struct ibv_wc wc[CHUNK];
  Uint32 remaining = m_completion_poll_budget;
  if (remaining == 0) remaining = (Uint32)CHUNK;
  int reaped_total = 0;

  const Uint32 slot_size = recv_slot_size_or_zero();
  if (slot_size == 0) return -1; /* geometry error already logged */

  while (remaining > 0) {
    const int take = (remaining > (Uint32)CHUNK) ? CHUNK : (int)remaining;
    m_stats.cq_polls_recv++;
    const int n = ibv_poll_cq(m_recv_cq, take, wc);
    if (n < 0) {
      g_eventLogger->error(
          "RDMA[node %u->%u]: ibv_poll_cq(recv) returned %d (errno=%d %s)",
          (unsigned)localNodeId, (unsigned)remoteNodeId, n, errno,
          std::strerror(errno));
      m_stats.qp_fatal_events++;
      report_error(TE_RDMA_CQ_ERROR, "ibv_poll_cq(recv) returned error");
      start_disconnecting(n, /*send_source=*/false);
      return -1;
    }
    if (n == 0) break;

    for (int i = 0; i < n; i++) {
      const Uint32 slot = (Uint32)wc[i].wr_id;
      if (slot >= m_queue_depth) {
        g_eventLogger->error(
            "RDMA[node %u->%u]: recv WC wr_id=%llu out of range (qd=%u)",
            (unsigned)localNodeId, (unsigned)remoteNodeId,
            (unsigned long long)wc[i].wr_id, m_queue_depth);
        m_stats.qp_fatal_events++;
        report_error(TE_RDMA_CQ_ERROR, "recv WC wr_id out of range");
        start_disconnecting(EINVAL, /*send_source=*/false);
        return -1;
      }

      if (wc[i].status != IBV_WC_SUCCESS) {
        TransporterError err = TE_RDMA_CQ_ERROR;
        if (wc[i].status == IBV_WC_RETRY_EXC_ERR ||
            wc[i].status == IBV_WC_RNR_RETRY_EXC_ERR) {
          err = TE_RDMA_RETRY_EXHAUSTED;
        }
        /* Symmetric with the send path: every recv WC error increments
         * recv_completion_errors; RNR / retry-exc statuses also count
         * in their dedicated buckets. */
        m_stats.recv_completion_errors++;
        if (wc[i].status == IBV_WC_RNR_RETRY_EXC_ERR) {
          m_stats.rnr_events++;
        } else if (wc[i].status == IBV_WC_RETRY_EXC_ERR) {
          m_stats.retry_exceeded_events++;
        }
        g_eventLogger->error(
            "RDMA[node %u->%u]: recv WC failure wr_id=%u status=%d (%s)",
            (unsigned)localNodeId, (unsigned)remoteNodeId, slot,
            (int)wc[i].status, ibv_wc_status_str(wc[i].status));
        report_error(err, "RDMA recv completion failure");
        start_disconnecting((int)wc[i].status, /*send_source=*/false);
        return -1;
      }

      /* Account every byte the HCA wrote into our slot, regardless of
       * whether this is a data message or a control message. This
       * matches the TCP convention of m_bytes_received tracking on-
       * the-wire bytes rather than just signal bytes. */
      m_bytes_received += (Uint64)wc[i].byte_len;

      /* The peer wrote wc[i].byte_len bytes into our slot, which is
       * header + payload. Validate the header before exposing it. */
      const char *slot_buf =
          (const char *)m_recv_buf + (size_t)slot * (size_t)slot_size;
      Uint32 payload_len = 0;
      Uint32 peer_seq = 0;
      Uint32 peer_ack = 0;
      Uint16 credit_delta = 0;
      Uint8 flags = 0;
      if (!validate_msg_header(slot_buf, wc[i].byte_len, &payload_len,
                               &peer_seq, &peer_ack, &credit_delta, &flags)) {
        report_error(TE_RDMA_INVALID_HEADER, "RDMA recv header validation");
        start_disconnecting(EBADMSG, /*send_source=*/false);
        return -1;
      }

      /* Enforce strict in-order delivery. RC SEND/RECV on a single QP
       * is in-order by spec, but mismatched seqs would indicate a
       * coding error or memory corruption that we should not paper
       * over. */
      if (peer_seq != m_local_recv_seq) {
        g_eventLogger->error(
            "RDMA[node %u->%u]: out-of-order recv seq %u (expected %u)",
            (unsigned)localNodeId, (unsigned)remoteNodeId, peer_seq,
            m_local_recv_seq);
        report_error(TE_RDMA_INVALID_HEADER, "RDMA recv seq mismatch");
        start_disconnecting(EPROTO, /*send_source=*/false);
        return -1;
      }
      m_local_recv_seq++;
      m_peer_ack_seq = peer_ack;

      /* Apply credit_delta to our pool of peer-recv credits. Cap the
       * sum at UINT32_MAX to avoid overflow on pathological streams
       * (which would also fail the protocol contract). */
      if (credit_delta > 0) {
        const Uint32 sum = m_peer_recv_credits + (Uint32)credit_delta;
        m_peer_recv_credits = (sum < m_peer_recv_credits) ? UINT32_MAX : sum;
      }

      /* Heartbeat: notify the receive handle that we saw traffic from
       * this node. This is required by the existing API contract so
       * Qmgr's HeartbeatIntervalDbDb mechanism continues to work. */
      recvdata.transporter_recv_from(remoteNodeId);

      /* CREDIT_ONLY / HEARTBEAT messages carry no payload; do not
       * enqueue them for the unpacker. The slot is now empty and we
       * can re-post it immediately. */
      if ((flags & (RDMA_MSG_FLAG_CREDIT_ONLY | RDMA_MSG_FLAG_HEARTBEAT)) !=
              0 ||
          payload_len == 0) {
        require(m_local_recv_posted > 0);
        m_local_recv_posted--;
        if (!post_one_receive(slot)) {
          m_stats.qp_fatal_events++;
          report_error(TE_RDMA_QP_ERROR, "failed to re-post recv slot");
          start_disconnecting(errno, /*send_source=*/false);
          return -1;
        }
        m_local_recv_posted++;
        /* Granting one credit back to the peer for the slot we just
         * recycled. */
        if (m_pending_credit_grant < 0xFFFFu) m_pending_credit_grant++;
        m_stats.recv_credit_only_in++;
        reaped_total++;
        continue;
      }

      /* Normal data message: park the slot in the ready queue and let
       * the unpacker consume it later. */
      m_recv_slots[slot].payload_len = payload_len;
      m_recv_slots[slot].read_offset = 0;
      require(m_recv_queue_count < m_queue_depth);
      m_recv_ready_queue[m_recv_queue_tail] = slot;
      m_recv_queue_tail = (m_recv_queue_tail + 1) % m_queue_depth;
      m_recv_queue_count++;
      m_stats.recv_completions_ok++;
      reaped_total++;
    }

    remaining -= (Uint32)n;
    if (n < take) break;  /* CQ drained */
  }
  if (remaining == 0 && reaped_total > 0) {
    /* Mirror reap_send_completions: surface budget exhaustion so
     * operators can correlate it with end-to-end latency. */
    m_stats.cq_budget_hits_recv++;
  }
  return reaped_total;
}

bool RDMA_Transporter::has_received_data() const {
  return m_recv_queue_count > 0;
}

void RDMA_Transporter::get_next_read(const void **out_ptr,
                                     Uint32 *out_len) const {
  if (m_recv_queue_count == 0 || m_recv_slots == nullptr ||
      m_recv_buf == nullptr) {
    if (out_ptr) *out_ptr = nullptr;
    if (out_len) *out_len = 0;
    return;
  }
  const Uint32 slot_size = m_queue_depth == 0
                               ? 0
                               : (m_recv_buffer_size / m_queue_depth);
  if (slot_size == 0) {
    if (out_ptr) *out_ptr = nullptr;
    if (out_len) *out_len = 0;
    return;
  }
  const Uint32 slot = m_recv_ready_queue[m_recv_queue_head];
  const rdma_recv_slot &st = m_recv_slots[slot];
  /* The slot layout is [header][payload]; the unpacker only ever sees
   * payload bytes. */
  const char *base = (const char *)m_recv_buf + (size_t)slot * slot_size +
                     RDMA_MSG_HEADER_BYTES;
  if (out_ptr) *out_ptr = base + st.read_offset;
  if (out_len) *out_len = st.payload_len - st.read_offset;
}

void RDMA_Transporter::consume_received_bytes(Uint32 n) {
  if (n == 0 || m_recv_queue_count == 0) return;
  const Uint32 slot = m_recv_ready_queue[m_recv_queue_head];
  rdma_recv_slot &st = m_recv_slots[slot];
  require(st.read_offset + n <= st.payload_len);
  st.read_offset += n;
  /* Account bytes successfully handed to the upper layer regardless of
   * whether this consume completes the slot or just advances within
   * it. */
  m_stats.copied_recv_bytes += (Uint64)n;
  if (st.read_offset < st.payload_len) {
    /* Partial unpack; head stays parked at this slot. */
    return;
  }
  /* Slot fully drained: pop it off the queue and re-post the
   * underlying ibv_recv_wr. Grant one credit back to the peer. */
  st.payload_len = 0;
  st.read_offset = 0;
  m_recv_queue_head = (m_recv_queue_head + 1) % m_queue_depth;
  m_recv_queue_count--;
  require(m_local_recv_posted > 0);
  m_local_recv_posted--;
  if (!post_one_receive(slot)) {
    /* Re-post failure is fatal for the link; the next reap on the
     * recv CQ will observe the failure too, but we surface it here
     * eagerly. */
    report_error(TE_RDMA_QP_ERROR, "failed to re-post drained recv slot");
    start_disconnecting(errno, /*send_source=*/false);
    return;
  }
  m_local_recv_posted++;
  if (m_pending_credit_grant < 0xFFFFu) m_pending_credit_grant++;
}

void RDMA_Transporter::encode_msg_header(void *buf, Uint32 payload_len,
                                         Uint32 send_seq, Uint32 ack_seq,
                                         Uint16 credit_delta, Uint8 flags) {
  /*
   * Caller must guarantee buf points to RDMA_MSG_HEADER_BYTES of writable
   * memory. We write directly into a packed struct view so the compiler
   * can optimize this to a handful of stores.
   */
  rdma_msg_header_v1 *hdr = static_cast<rdma_msg_header_v1 *>(buf);
  hdr->magic = htonl(RDMA_MSG_MAGIC);
  hdr->version = RDMA_MSG_VERSION;
  hdr->flags = flags;
  hdr->header_len = htons(RDMA_MSG_HEADER_BYTES);
  hdr->payload_len = htonl(payload_len);
  hdr->send_seq = htonl(send_seq);
  hdr->ack_seq = htonl(ack_seq);
  hdr->credit_delta = htons(credit_delta);
  hdr->reserved = 0;
}

bool RDMA_Transporter::validate_msg_header(
    const void *buf, size_t available, Uint32 *out_payload_len,
    Uint32 *out_send_seq, Uint32 *out_ack_seq, Uint16 *out_credit_delta,
    Uint8 *out_flags) {
  if (available < RDMA_MSG_HEADER_BYTES) {
    g_eventLogger->error(
        "RDMA: short receive (%zu bytes) cannot contain msg header (%u bytes)",
        available, (unsigned)RDMA_MSG_HEADER_BYTES);
    return false;
  }

  const rdma_msg_header_v1 *hdr =
      static_cast<const rdma_msg_header_v1 *>(buf);
  const uint32_t magic = ntohl(hdr->magic);
  const uint16_t header_len = ntohs(hdr->header_len);
  const uint32_t payload_len = ntohl(hdr->payload_len);

  if (magic != RDMA_MSG_MAGIC) {
    g_eventLogger->error("RDMA: bad msg magic 0x%08x (want 0x%08x)", magic,
                         RDMA_MSG_MAGIC);
    return false;
  }
  if (hdr->version != RDMA_MSG_VERSION) {
    g_eventLogger->error("RDMA: incompatible msg version %u (want %u)",
                         (unsigned)hdr->version, (unsigned)RDMA_MSG_VERSION);
    return false;
  }
  if (header_len != RDMA_MSG_HEADER_BYTES) {
    g_eventLogger->error(
        "RDMA: unexpected msg header_len %u (want %u)", (unsigned)header_len,
        (unsigned)RDMA_MSG_HEADER_BYTES);
    return false;
  }
  if ((hdr->flags & ~RDMA_MSG_FLAG_ALL_KNOWN) != 0) {
    g_eventLogger->error("RDMA: msg flags 0x%02x contain unknown bits",
                         (unsigned)hdr->flags);
    return false;
  }
  /* Credit-only / heartbeat must not carry payload bytes. */
  if ((hdr->flags & (RDMA_MSG_FLAG_CREDIT_ONLY | RDMA_MSG_FLAG_HEARTBEAT)) !=
          0 &&
      payload_len != 0) {
    g_eventLogger->error(
        "RDMA: control-flag msg has non-zero payload_len=%u", payload_len);
    return false;
  }
  /* Payload bytes must fit in (available - header) and within the upper
   * Protocol6 receive limit. */
  if (payload_len > (Uint32)MAX_RECV_MESSAGE_BYTESIZE) {
    g_eventLogger->error(
        "RDMA: msg payload_len=%u exceeds MAX_RECV_MESSAGE_BYTESIZE=%u",
        payload_len, (unsigned)MAX_RECV_MESSAGE_BYTESIZE);
    return false;
  }
  if ((size_t)payload_len + (size_t)RDMA_MSG_HEADER_BYTES > available) {
    g_eventLogger->error(
        "RDMA: msg payload_len=%u + header=%u exceeds available=%zu",
        payload_len, (unsigned)RDMA_MSG_HEADER_BYTES, available);
    return false;
  }

  if (out_payload_len) *out_payload_len = payload_len;
  if (out_send_seq) *out_send_seq = ntohl(hdr->send_seq);
  if (out_ack_seq) *out_ack_seq = ntohl(hdr->ack_seq);
  if (out_credit_delta) *out_credit_delta = ntohs(hdr->credit_delta);
  if (out_flags) *out_flags = hdr->flags;
  return true;
}

Uint32 RDMA_Transporter::recv_slot_size_or_zero() const {
  if (m_queue_depth == 0) {
    g_eventLogger->error(
        "RDMA: queue depth is zero; cannot compute slot size");
    return 0;
  }
  const Uint32 slot = m_recv_buffer_size / m_queue_depth;
  /* Each slot must hold a full max-size signal plus our per-message
   * framing header. If the configured RdmaRecvBufferMemory is too small
   * for the configured RdmaQueueDepth we refuse early with a clear
   * message; otherwise we would post a too-short SGE and lose data
   * silently on the wire. */
  const Uint32 minimum =
      (Uint32)RDMA_MSG_HEADER_BYTES + (Uint32)MAX_RECV_MESSAGE_BYTESIZE;
  if (slot < minimum) {
    g_eventLogger->error(
        "RDMA: receive slot size %u (RdmaRecvBufferMemory=%u / "
        "RdmaQueueDepth=%u) is below required minimum %u (24-byte header + "
        "%u-byte max signal). Increase RdmaRecvBufferMemory or decrease "
        "RdmaQueueDepth.",
        slot, m_recv_buffer_size, m_queue_depth, minimum,
        (unsigned)MAX_RECV_MESSAGE_BYTESIZE);
    return 0;
  }
  return slot;
}

bool RDMA_Transporter::post_one_receive(Uint32 slot_idx) {
  require(m_qp != nullptr);
  require(m_recv_mr != nullptr);
  require(m_recv_buf != nullptr);
  require(slot_idx < m_queue_depth);

  const Uint32 slot_size = recv_slot_size_or_zero();
  if (slot_size == 0) return false;

  struct ibv_sge sge;
  std::memset(&sge, 0, sizeof(sge));
  sge.addr =
      (uintptr_t)((char *)m_recv_buf + (size_t)slot_idx * (size_t)slot_size);
  sge.length = slot_size;
  sge.lkey = m_recv_mr->lkey;

  struct ibv_recv_wr wr;
  std::memset(&wr, 0, sizeof(wr));
  /* Encode slot index in wr_id so the receive completion handler in
   * Milestone 8 can route the bytes back to the right slot and re-post
   * it without consulting external state. */
  wr.wr_id = (uint64_t)slot_idx;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.next = nullptr;

  struct ibv_recv_wr *bad = nullptr;
  const int rc = ibv_post_recv(m_qp, &wr, &bad);
  if (rc != 0) {
    g_eventLogger->error(
        "RDMA: ibv_post_recv failed for slot %u (rc=%d errno=%d %s)", slot_idx,
        rc, errno, std::strerror(errno));
    return false;
  }
  m_stats.recv_posted++;
  return true;
}

bool RDMA_Transporter::post_initial_receives() {
  /*
   * Validate geometry once up front so a single failure short-circuits
   * all m_queue_depth post attempts. After this point each
   * post_one_receive() call only fails if the verbs provider rejects
   * the WR for runtime reasons (CQ overflow etc.).
   */
  if (recv_slot_size_or_zero() == 0) return false;

  require(m_local_recv_posted == 0);
  for (Uint32 i = 0; i < m_queue_depth; i++) {
    if (!post_one_receive(i)) {
      g_eventLogger->error(
          "RDMA: failed to post initial receive %u/%u; partial state will be "
          "reaped by QP destroy",
          i, m_queue_depth);
      return false;
    }
    m_local_recv_posted++;
  }
  return true;
}

void RDMA_Transporter::log_stats() const {
  /*
   * Single info line containing the per-link counter snapshot. Layout
   * is intentionally line-orientated and key=value to make grep/awk
   * post-processing trivial. The order groups related counters; do
   * not reorder without updating downstream parsers (none yet, but a
   * stable layout is cheap insurance).
   *
   * m_bytes_sent / m_bytes_received come from the base Transporter
   * and are updated by the reap paths in this class, so they are
   * surfaced here too for one-stop diagnostics.
   */
  g_eventLogger->info(
      "RDMA[node %u->%u]: stats reconnects=%llu "
      "send_posted=%llu send_ok=%llu send_err=%llu send_inline=%llu "
      "send_credit_only_out=%llu send_credit_stalls=%llu copied_send=%llu "
      "recv_posted=%llu recv_ok=%llu recv_err=%llu recv_credit_only_in=%llu "
      "copied_recv=%llu bytes_sent=%llu bytes_received=%llu "
      "cq_polls_send=%llu cq_polls_recv=%llu "
      "cq_budget_hits_send=%llu cq_budget_hits_recv=%llu "
      "rnr=%llu retry_exceeded=%llu qp_fatal=%llu",
      (unsigned)localNodeId, (unsigned)remoteNodeId,
      (unsigned long long)m_stats.reconnect_attempts,
      (unsigned long long)m_stats.send_posted,
      (unsigned long long)m_stats.send_completions_ok,
      (unsigned long long)m_stats.send_completion_errors,
      (unsigned long long)m_stats.send_inline,
      (unsigned long long)m_stats.send_credit_only_out,
      (unsigned long long)m_stats.send_credit_stalls,
      (unsigned long long)m_stats.copied_send_bytes,
      (unsigned long long)m_stats.recv_posted,
      (unsigned long long)m_stats.recv_completions_ok,
      (unsigned long long)m_stats.recv_completion_errors,
      (unsigned long long)m_stats.recv_credit_only_in,
      (unsigned long long)m_stats.copied_recv_bytes,
      (unsigned long long)m_bytes_sent, (unsigned long long)m_bytes_received,
      (unsigned long long)m_stats.cq_polls_send,
      (unsigned long long)m_stats.cq_polls_recv,
      (unsigned long long)m_stats.cq_budget_hits_send,
      (unsigned long long)m_stats.cq_budget_hits_recv,
      (unsigned long long)m_stats.rnr_events,
      (unsigned long long)m_stats.retry_exceeded_events,
      (unsigned long long)m_stats.qp_fatal_events);
}

void RDMA_Transporter::log_negotiated_attributes() const {
  /*
   * Caller guarantees that allocate_verbs_resources() succeeded, so the
   * verbs handles are valid. We deliberately log at info level: this is
   * useful for diagnosing capability mismatches but should not be
   * verbose enough to flood the event log in steady state.
   */
  if (m_verbs_ctx == nullptr || m_qp == nullptr) {
    /* Defensive: nothing to log. */
    return;
  }

  struct ibv_port_attr port_attr;
  std::memset(&port_attr, 0, sizeof(port_attr));
  /* Best-effort: ignore failure, the log line still has useful info. */
  (void)ibv_query_port(m_verbs_ctx, (uint8_t)m_rdma_port, &port_attr);

  const char *device_str =
      (m_device_name != nullptr) ? m_device_name : "<first-available>";

  g_eventLogger->info(
      "RDMA[node %u->%u]: verbs allocated dev=%s port=%u mtu=%uB "
      "qp_num=%u qd_send=%u qd_recv=%u max_inline=%u (requested=%u) "
      "send_buf=%uB recv_buf=%uB gid_idx=%u",
      (unsigned)localNodeId, (unsigned)remoteNodeId, device_str, m_rdma_port,
      rdma_mtu_to_bytes(port_attr.active_mtu), (unsigned)m_qp->qp_num,
      m_queue_depth, m_queue_depth, m_effective_inline_threshold,
      m_inline_threshold, m_send_buffer_size, m_recv_buffer_size, m_gid_index);
}

/*
 * --------------------------------------------------------------------------
 *  Endpoint exchange & QP state machine
 * --------------------------------------------------------------------------
 */
bool RDMA_Transporter::run_endpoint_exchange(const NdbSocket &socket,
                                             const char *side) {
  require(m_qp != nullptr);
  require(m_verbs_ctx != nullptr);

  /* Snapshot the local port attributes so we can advertise GID/LID to
   * the peer alongside the QPN. The MTU we send is whatever the port
   * currently reports; QP transitions later will clamp to the
   * minimum of (local, peer). */
  struct ibv_port_attr port_attr;
  std::memset(&port_attr, 0, sizeof(port_attr));
  if (ibv_query_port(m_verbs_ctx, (uint8_t)m_rdma_port, &port_attr) != 0) {
    g_eventLogger->error(
        "RDMA[%s,node %u->%u]: ibv_query_port failed during exchange "
        "(errno=%d %s)",
        side, (unsigned)localNodeId, (unsigned)remoteNodeId, errno,
        std::strerror(errno));
    return false;
  }

  /* Read our GID. This is only meaningful for RoCE links; for IB-link
   * the peer will use the LID we advertise in `lid`. We still send a
   * valid GID so the peer can pick whichever addressing it prefers. */
  union ibv_gid local_gid;
  std::memset(&local_gid, 0, sizeof(local_gid));
  if (ibv_query_gid(m_verbs_ctx, (uint8_t)m_rdma_port, (int)m_gid_index,
                    &local_gid) != 0) {
    g_eventLogger->error(
        "RDMA[%s,node %u->%u]: ibv_query_gid(idx=%u) failed (errno=%d %s)",
        side, (unsigned)localNodeId, (unsigned)remoteNodeId, m_gid_index,
        errno, std::strerror(errno));
    return false;
  }

  const uint32_t local_psn = rdma_pick_initial_psn(m_qp);

  /* Serialize the local record into the wire layout. */
  rdma_endpoint_v1 local_rec;
  std::memset(&local_rec, 0, sizeof(local_rec));
  local_rec.magic = htonl(RDMA_WIRE_MAGIC);
  local_rec.version = htons(RDMA_WIRE_VERSION);
  local_rec.header_len = htons(RDMA_WIRE_HEADER_BYTES);
  local_rec.qp_num = htonl(m_qp->qp_num);
  local_rec.psn = htonl(local_psn);
  local_rec.lid = htons(port_attr.lid);
  std::memcpy(local_rec.gid, local_gid.raw, sizeof(local_rec.gid));
  local_rec.gid_index = (uint8_t)m_gid_index;
  local_rec.port_num = (uint8_t)m_rdma_port;
  local_rec.mtu = (uint8_t)port_attr.active_mtu;
  local_rec.link_layer = (uint8_t)port_attr.link_layer;
  local_rec.max_inline = htonl(m_effective_inline_threshold);
  local_rec.queue_depth = htonl(m_queue_depth);
  /* Initial receive credits we grant the peer equals our queue depth
   * minus 1 (one slot is held back for the credit-only message that
   * the Gate-2 credit protocol will use). For Gate-1 we just advertise
   * the full queue depth so the peer has accurate sizing info. */
  local_rec.recv_credits = htonl(m_queue_depth);

  /* Step 1+2: simultaneous send/recv. We send first then read; the
   * peer does the same, so both ends progress without a deadlock. */
  if (!rdma_send_full(socket, &local_rec, sizeof(local_rec),
                      RDMA_HANDSHAKE_TIMEOUT_MS)) {
    g_eventLogger->error(
        "RDMA[%s,node %u->%u]: failed to send local endpoint record", side,
        (unsigned)localNodeId, (unsigned)remoteNodeId);
    return false;
  }

  rdma_endpoint_v1 peer_rec;
  std::memset(&peer_rec, 0, sizeof(peer_rec));
  if (!rdma_recv_full(socket, &peer_rec, sizeof(peer_rec),
                      RDMA_HANDSHAKE_TIMEOUT_MS)) {
    g_eventLogger->error(
        "RDMA[%s,node %u->%u]: failed to read peer endpoint record", side,
        (unsigned)localNodeId, (unsigned)remoteNodeId);
    return false;
  }

  /* Step 3: validation. Reject anything that does not look like a
   * compatible RDMA endpoint. */
  const uint32_t peer_magic = ntohl(peer_rec.magic);
  const uint16_t peer_version = ntohs(peer_rec.version);
  const uint16_t peer_hlen = ntohs(peer_rec.header_len);
  if (peer_magic != RDMA_WIRE_MAGIC) {
    g_eventLogger->error(
        "RDMA[%s,node %u->%u]: bad magic 0x%08x in peer record (want 0x%08x)",
        side, (unsigned)localNodeId, (unsigned)remoteNodeId, peer_magic,
        RDMA_WIRE_MAGIC);
    return false;
  }
  if (peer_version != RDMA_WIRE_VERSION) {
    g_eventLogger->error(
        "RDMA[%s,node %u->%u]: incompatible wire version %u (want %u)", side,
        (unsigned)localNodeId, (unsigned)remoteNodeId, peer_version,
        RDMA_WIRE_VERSION);
    return false;
  }
  if (peer_hlen != RDMA_WIRE_HEADER_BYTES) {
    g_eventLogger->error(
        "RDMA[%s,node %u->%u]: unexpected peer record length %u (want %u)",
        side, (unsigned)localNodeId, (unsigned)remoteNodeId, peer_hlen,
        RDMA_WIRE_HEADER_BYTES);
    return false;
  }
  if (peer_rec.link_layer != local_rec.link_layer) {
    g_eventLogger->error(
        "RDMA[%s,node %u->%u]: link layer mismatch (local=%u peer=%u)", side,
        (unsigned)localNodeId, (unsigned)remoteNodeId, local_rec.link_layer,
        peer_rec.link_layer);
    return false;
  }

  /* MTU negotiation: use the smaller of the two. This matters because
   * an asymmetric MTU configuration would otherwise cause silent drops
   * at the fabric level. We mutate peer_rec.mtu in place so
   * qp_transition_to_rtr() can rely on the negotiated value. */
  if ((unsigned)peer_rec.mtu < (unsigned)local_rec.mtu) {
    /* Peer is the bottleneck; keep peer.mtu as is. */
  } else if ((unsigned)peer_rec.mtu > (unsigned)local_rec.mtu) {
    peer_rec.mtu = local_rec.mtu;
  }

  /*
   * Step 4: QP transitions and initial-receive posting.
   *   INIT -> post_initial_receives() -> RTR -> RTS
   *
   * Posting receives between INIT and RTR is the standard IB pattern:
   * the QP must already be in INIT for ibv_post_recv() to be legal,
   * and the receives must be on the queue before RTR so that the
   * peer's first SEND has a slot to land in.
   */
  if (!qp_transition_to_init()) return false;
  if (!post_initial_receives()) return false;
  if (!qp_transition_to_rtr(&peer_rec)) return false;
  if (!qp_transition_to_rts(local_psn)) return false;

  /*
   * Seed credit accounting from the peer's record. The peer advertised
   * how many receive slots it has posted on its end, which equals the
   * number of in-flight SENDs we are allowed to issue before we must
   * stall.
   */
  m_peer_recv_credits = ntohl(peer_rec.recv_credits);
  /* We expect the peer's first SEND to carry seq 0; mirror the
   * sender-side default. */
  m_local_send_seq = 0;
  m_local_recv_seq = 0;
  m_peer_ack_seq = 0;

  g_eventLogger->info(
      "RDMA[%s,node %u->%u]: handshake OK: local_qpn=%u peer_qpn=%u "
      "local_psn=%u peer_psn=%u mtu=%uB recv_posted=%u credits_from_peer=%u",
      side, (unsigned)localNodeId, (unsigned)remoteNodeId,
      (unsigned)m_qp->qp_num, (unsigned)ntohl(peer_rec.qp_num),
      (unsigned)local_psn, (unsigned)ntohl(peer_rec.psn),
      rdma_mtu_to_bytes((enum ibv_mtu)peer_rec.mtu), m_local_recv_posted,
      m_peer_recv_credits);
  return true;
}

bool RDMA_Transporter::qp_transition_to_init() {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = (uint8_t)m_rdma_port;
  /* LOCAL_WRITE is required for ibv_post_recv() to land bytes in our
   * MR. We do not enable REMOTE_READ/WRITE/ATOMIC because the
   * SEND/RECV-only design does not grant peers an rkey. */
  attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE;

  const int mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                   IBV_QP_ACCESS_FLAGS;
  if (ibv_modify_qp(m_qp, &attr, mask) != 0) {
    g_eventLogger->error(
        "RDMA: ibv_modify_qp(INIT) failed for qpn=%u (errno=%d %s)",
        (unsigned)m_qp->qp_num, errno, std::strerror(errno));
    return false;
  }
  return true;
}

bool RDMA_Transporter::qp_transition_to_rtr(const void *peer_record) {
  const rdma_endpoint_v1 *peer =
      static_cast<const rdma_endpoint_v1 *>(peer_record);

  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = (enum ibv_mtu)peer->mtu;  /* already negotiated to min */
  attr.dest_qp_num = ntohl(peer->qp_num);
  attr.rq_psn = ntohl(peer->psn);
  attr.max_dest_rd_atomic = 0;  /* no READ/ATOMIC on this QP */
  attr.min_rnr_timer = 12;      /* ~0.64 ms; conservative IB default */

  /* Address handle: for RoCE we rely on GID-based addressing; for IB
   * we use the LID. We set both fields so the verbs provider can pick
   * whichever matches the link layer. */
  attr.ah_attr.is_global = (peer->link_layer == IBV_LINK_LAYER_ETHERNET) ? 1
                                                                        : 0;
  attr.ah_attr.dlid = ntohs(peer->lid);
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = (uint8_t)m_rdma_port;
  if (attr.ah_attr.is_global) {
    std::memcpy(attr.ah_attr.grh.dgid.raw, peer->gid,
                sizeof(attr.ah_attr.grh.dgid.raw));
    attr.ah_attr.grh.sgid_index = (uint8_t)m_gid_index;
    attr.ah_attr.grh.hop_limit = 1;
    attr.ah_attr.grh.traffic_class = (uint8_t)m_traffic_class;
  }

  const int mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                   IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                   IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  if (ibv_modify_qp(m_qp, &attr, mask) != 0) {
    g_eventLogger->error(
        "RDMA: ibv_modify_qp(RTR) failed for qpn=%u dest_qpn=%u "
        "(errno=%d %s)",
        (unsigned)m_qp->qp_num, (unsigned)attr.dest_qp_num, errno,
        std::strerror(errno));
    return false;
  }
  return true;
}

bool RDMA_Transporter::qp_transition_to_rts(Uint32 local_psn) {
  struct ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;       /* ~67ms ack timeout, IB-typical */
  attr.retry_cnt = (uint8_t)m_retry_count;
  attr.rnr_retry = (uint8_t)m_rnr_retry_count;
  attr.sq_psn = local_psn;
  attr.max_rd_atomic = 0;  /* no outgoing READ/ATOMIC */

  const int mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                   IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  if (ibv_modify_qp(m_qp, &attr, mask) != 0) {
    g_eventLogger->error(
        "RDMA: ibv_modify_qp(RTS) failed for qpn=%u (errno=%d %s)",
        (unsigned)m_qp->qp_num, errno, std::strerror(errno));
    return false;
  }
  return true;
}

/*
 * --------------------------------------------------------------------------
 *  Unit tests for the RDMA wire-format helpers (Milestone 11)
 * --------------------------------------------------------------------------
 *
 * Gated by TEST_RDMA_TRANSPORTER which is only defined for the
 * NDB_ADD_TEST() build of this translation unit. The test does NOT
 * exercise any verbs or socket APIs; it operates purely on byte
 * buffers via encode_msg_header() / validate_msg_header().
 *
 * Coverage:
 *   - Round-trip encode/decode with a variety of field values
 *     (zeros, max u32/u16, CREDIT_ONLY, HEARTBEAT, max payload).
 *   - Network byte-order layout assertions on the raw bytes.
 *   - Rejection of bad magic, bad version, bad header_len, and
 *     unknown flag bits.
 *   - Rejection of CREDIT_ONLY/HEARTBEAT messages carrying a payload.
 *   - Rejection of payload_len that exceeds MAX_RECV_MESSAGE_BYTESIZE.
 *   - Rejection of buffers shorter than the fixed header.
 *   - Rejection of payload_len + header_len exceeding `available`.
 *   - Acceptance of an exact-fit `available`.
 *
 * Each assertion uses OK() (which is require()-based and aborts on
 * failure); the test binary exits with TAP success only when every
 * assertion passes.
 */
#ifdef TEST_RDMA_TRANSPORTER
#include <NdbTap.hpp>
#include <ndb_init.h>

/*
 * RAII guard that calls ndb_init() at construction and ndb_end(0) at
 * destruction so the TAPTEST body executes inside an initialized NDB
 * runtime. This is required because validate_msg_header() reports
 * rejections via g_eventLogger->error(); without ndb_init() that
 * pointer is null and the negative test cases crash before they get a
 * chance to assert.
 */
namespace {
struct rdma_test_ndb_init_guard {
  rdma_test_ndb_init_guard() { ndb_init(); }
  ~rdma_test_ndb_init_guard() { ndb_end(0); }
};
}  // namespace

/*
 * Encode `payload_len`/headers into `buf`, then validate it back.
 * All fields must round-trip identically and validation must succeed.
 */
static void rdma_test_round_trip(Uint32 payload_len, Uint32 send_seq,
                                 Uint32 ack_seq, Uint16 credit_delta,
                                 Uint8 flags) {
  /* Scratch buffer sized to comfortably fit header + small payload;
   * validate_msg_header() only reads the header bytes plus the
   * `available` count we pass in. */
  uint8_t buf[64];
  std::memset(buf, 0xAA, sizeof(buf));
  RDMA_Transporter::encode_msg_header(buf, payload_len, send_seq, ack_seq,
                                      credit_delta, flags);

  /* Sentinel-init the out parameters so any branch that forgets to
   * write them shows up as a mismatch. */
  Uint32 got_payload_len = 0xDEADBEEFu;
  Uint32 got_send_seq = 0xDEADBEEFu;
  Uint32 got_ack_seq = 0xDEADBEEFu;
  Uint16 got_credit_delta = 0xDEADu;
  Uint8 got_flags = 0xCDu;
  const size_t available =
      (size_t)RDMA_MSG_HEADER_BYTES + (size_t)payload_len;
  OK(RDMA_Transporter::validate_msg_header(
      buf, available, &got_payload_len, &got_send_seq, &got_ack_seq,
      &got_credit_delta, &got_flags));
  OK(got_payload_len == payload_len);
  OK(got_send_seq == send_seq);
  OK(got_ack_seq == ack_seq);
  OK(got_credit_delta == credit_delta);
  OK(got_flags == flags);
}

/*
 * Encode a well-formed minimal header into `buf`, mutate one byte at
 * `mutate_off` to `mutate_val`, and expect validation to reject the
 * resulting record. The validator must NOT write through the output
 * pointers on failure, so we pass nullptrs.
 */
static void rdma_test_reject_after_mutation(size_t mutate_off,
                                            uint8_t mutate_val) {
  uint8_t buf[64];
  RDMA_Transporter::encode_msg_header(buf, /*payload=*/0u, /*send_seq=*/0u,
                                      /*ack_seq=*/0u, /*credit_delta=*/0u,
                                      /*flags=*/0u);
  buf[mutate_off] = mutate_val;
  OK(!RDMA_Transporter::validate_msg_header(buf, RDMA_MSG_HEADER_BYTES,
                                            nullptr, nullptr, nullptr,
                                            nullptr, nullptr));
}

TAPTEST(RDMA_Transporter) {
  /* ndb_init() before any code that calls into validate_msg_header,
   * which uses g_eventLogger on the rejection paths. */
  rdma_test_ndb_init_guard ndb_runtime_guard;
  uint8_t buf[64];

  /* ----- Round-trip cases -----
   * Exercise the encode/decode pair over a representative set of
   * field values: all-zero, peak u32/u16, CREDIT_ONLY, HEARTBEAT,
   * and a payload that is exactly at MAX_RECV_MESSAGE_BYTESIZE. */
  rdma_test_round_trip(/*payload=*/0u, /*send_seq=*/0u, /*ack_seq=*/0u,
                       /*credit_delta=*/0u, /*flags=*/0u);
  rdma_test_round_trip(/*payload=*/0x100u, /*send_seq=*/0x12345678u,
                       /*ack_seq=*/0x9ABCDEF0u, /*credit_delta=*/0x55AAu,
                       /*flags=*/0u);
  rdma_test_round_trip(/*payload=*/0u, /*send_seq=*/1u, /*ack_seq=*/2u,
                       /*credit_delta=*/3u,
                       /*flags=*/RDMA_MSG_FLAG_CREDIT_ONLY);
  rdma_test_round_trip(/*payload=*/0u, /*send_seq=*/0xFFFFFFFFu,
                       /*ack_seq=*/0xFFFFFFFFu,
                       /*credit_delta=*/0xFFFFu,
                       /*flags=*/RDMA_MSG_FLAG_HEARTBEAT);
  rdma_test_round_trip(/*payload=*/(Uint32)MAX_RECV_MESSAGE_BYTESIZE,
                       /*send_seq=*/0xCAFEBABEu,
                       /*ack_seq=*/0xFEEDFACEu,
                       /*credit_delta=*/0xFFFFu, /*flags=*/0u);

  /* ----- Network-byte-order layout -----
   * Encode known values and assert specific bytes appear at specific
   * offsets so we never silently regress to host byte order. */
  RDMA_Transporter::encode_msg_header(buf, /*payload=*/0x12345678u,
                                      /*send_seq=*/0xAABBCCDDu,
                                      /*ack_seq=*/0x11223344u,
                                      /*credit_delta=*/0x5566u,
                                      /*flags=*/0u);
  /* Magic at offset 0, htonl(0x52444D31) = 'R','D','M','1'. */
  OK(buf[0] == 'R');
  OK(buf[1] == 'D');
  OK(buf[2] == 'M');
  OK(buf[3] == '1');
  /* Version byte at offset 4, flags byte at offset 5. */
  OK(buf[4] == RDMA_MSG_VERSION);
  OK(buf[5] == 0u);
  /* header_len u16 at offset 6: 24 == 0x0018 in network order. */
  OK(buf[6] == 0x00u);
  OK(buf[7] == 0x18u);
  /* payload_len u32 at offset 8: 0x12345678. */
  OK(buf[8] == 0x12u);
  OK(buf[9] == 0x34u);
  OK(buf[10] == 0x56u);
  OK(buf[11] == 0x78u);
  /* send_seq u32 at offset 12: 0xAABBCCDD. */
  OK(buf[12] == 0xAAu);
  OK(buf[13] == 0xBBu);
  OK(buf[14] == 0xCCu);
  OK(buf[15] == 0xDDu);
  /* ack_seq u32 at offset 16: 0x11223344. */
  OK(buf[16] == 0x11u);
  OK(buf[17] == 0x22u);
  OK(buf[18] == 0x33u);
  OK(buf[19] == 0x44u);
  /* credit_delta u16 at offset 20: 0x5566. */
  OK(buf[20] == 0x55u);
  OK(buf[21] == 0x66u);
  /* reserved u16 at offset 22 must be zero. */
  OK(buf[22] == 0x00u);
  OK(buf[23] == 0x00u);

  /* ----- Single-byte corruption rejections -----
   * Each case starts from a well-formed header and changes exactly
   * one byte that the validator inspects. */
  /* Magic mismatch: first byte. */
  rdma_test_reject_after_mutation(/*off=*/0u, /*val=*/0xFFu);
  /* Version mismatch: offset 4 in the header. */
  rdma_test_reject_after_mutation(/*off=*/4u, /*val=*/99u);
  /* Unknown flag bit (high bit set, outside RDMA_MSG_FLAG_ALL_KNOWN). */
  rdma_test_reject_after_mutation(/*off=*/5u, /*val=*/0x80u);
  /* header_len mismatch: low byte to non-24 value. */
  rdma_test_reject_after_mutation(/*off=*/7u, /*val=*/99u);

  /* ----- Semantic rejections -----
   * encode_msg_header() does NOT enforce the CREDIT_ONLY/HEARTBEAT
   * "no payload" rule on the sender side; the receiver does, and
   * that is what we verify here by crafting an illegal record. */
  RDMA_Transporter::encode_msg_header(buf, /*payload=*/16u, /*send_seq=*/0u,
                                      /*ack_seq=*/0u, /*credit_delta=*/0u,
                                      RDMA_MSG_FLAG_CREDIT_ONLY);
  OK(!RDMA_Transporter::validate_msg_header(
      buf, (size_t)RDMA_MSG_HEADER_BYTES + 16u, nullptr, nullptr, nullptr,
      nullptr, nullptr));

  RDMA_Transporter::encode_msg_header(buf, /*payload=*/4u, /*send_seq=*/0u,
                                      /*ack_seq=*/0u, /*credit_delta=*/0u,
                                      RDMA_MSG_FLAG_HEARTBEAT);
  OK(!RDMA_Transporter::validate_msg_header(
      buf, (size_t)RDMA_MSG_HEADER_BYTES + 4u, nullptr, nullptr, nullptr,
      nullptr, nullptr));

  /* payload_len exceeding the protocol-level maximum must be rejected. */
  RDMA_Transporter::encode_msg_header(
      buf, /*payload=*/(Uint32)MAX_RECV_MESSAGE_BYTESIZE + 1u,
      /*send_seq=*/0u, /*ack_seq=*/0u, /*credit_delta=*/0u, /*flags=*/0u);
  OK(!RDMA_Transporter::validate_msg_header(
      buf,
      (size_t)RDMA_MSG_HEADER_BYTES + (size_t)MAX_RECV_MESSAGE_BYTESIZE + 1u,
      nullptr, nullptr, nullptr, nullptr, nullptr));

  /* available < header_len: short buffer must fail before the
   * validator dereferences any field. */
  RDMA_Transporter::encode_msg_header(buf, /*payload=*/0u, /*send_seq=*/0u,
                                      /*ack_seq=*/0u, /*credit_delta=*/0u,
                                      /*flags=*/0u);
  OK(!RDMA_Transporter::validate_msg_header(
      buf, (size_t)RDMA_MSG_HEADER_BYTES - 1u, nullptr, nullptr, nullptr,
      nullptr, nullptr));

  /* payload_len + header_len > available must fail; an exact-fit
   * `available` must succeed. */
  RDMA_Transporter::encode_msg_header(buf, /*payload=*/100u, /*send_seq=*/0u,
                                      /*ack_seq=*/0u, /*credit_delta=*/0u,
                                      /*flags=*/0u);
  OK(!RDMA_Transporter::validate_msg_header(
      buf, (size_t)RDMA_MSG_HEADER_BYTES + 99u, nullptr, nullptr, nullptr,
      nullptr, nullptr));
  Uint32 got_payload_len_exact = 0;
  OK(RDMA_Transporter::validate_msg_header(
      buf, (size_t)RDMA_MSG_HEADER_BYTES + 100u, &got_payload_len_exact,
      nullptr, nullptr, nullptr, nullptr));
  OK(got_payload_len_exact == 100u);

  return 1;  /* TAP success */
}
#endif /* TEST_RDMA_TRANSPORTER */

#endif /* NDB_RDMA_TRANSPORTER_SUPPORTED */
