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
#include <ctime>
#include <fcntl.h>
#include <map>
#include <mutex>
#include <new>
#include <sys/mman.h>
#include <vector>
#include <EventLogger.hpp>

/*
 * Heartbeat cadence for the periodic log_stats() emission driven by
 * maybe_log_stats_heartbeat(). 10 seconds is short enough to be
 * useful for diagnosing GCP-lag / NDB-274 cycles (which fire every
 * ~15-18s) without flooding the journal in steady state.
 */
static constexpr Uint64 RDMA_STATS_HEARTBEAT_NS =
    10ULL * 1000ULL * 1000ULL * 1000ULL;


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
  static std::atomic<uint32_t> s_psn_counter(0);
  /* Treat the QP pointer as an opaque mixing value. We only use the
   * low 24 bits because that is all the IB PSN field carries. */
  const uintptr_t mix = reinterpret_cast<uintptr_t>(qp);
  const uint32_t bumped =
      s_psn_counter.fetch_add(1, std::memory_order_relaxed) + 1;
  return ((uint32_t)mix ^ bumped) & 0x00FFFFFFu;
}

static bool rdma_device_name_equal(const char *lhs, const char *rhs) {
  const bool lhs_empty = (lhs == nullptr || lhs[0] == '\0');
  const bool rhs_empty = (rhs == nullptr || rhs[0] == '\0');
  if (lhs_empty || rhs_empty) return lhs_empty == rhs_empty;
  return std::strcmp(lhs, rhs) == 0;
}

static Uint32 rdma_overload_limit(const TransporterConfiguration *conf) {
  return (conf->rdma.overloadLimit ? conf->rdma.overloadLimit
                                   : conf->rdma.sendBufferSize * 4 / 5);
}

static bool rdma_gid_is_zero(const uint8_t gid[16]) {
  for (unsigned i = 0; i < 16; i++) {
    if (gid[i] != 0) return false;
  }
  return true;
}

static const char *rdma_link_layer_name(uint8_t link_layer) {
  switch (link_layer) {
    case IBV_LINK_LAYER_INFINIBAND:
      return "IB";
    case IBV_LINK_LAYER_ETHERNET:
      return "RoCE";
    default:
      return "unknown";
  }
}

static void rdma_format_gid(const uint8_t gid[16], char *dst, size_t dst_len) {
  std::snprintf(dst, dst_len,
                "%02x%02x:%02x%02x:%02x%02x:%02x%02x:"
                "%02x%02x:%02x%02x:%02x%02x:%02x%02x",
                gid[0], gid[1], gid[2], gid[3], gid[4], gid[5], gid[6],
                gid[7], gid[8], gid[9], gid[10], gid[11], gid[12], gid[13],
                gid[14], gid[15]);
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
 * --------------------------------------------------------------------------
 *  Phase 1: file-local buffer provider abstraction
 * --------------------------------------------------------------------------
 *
 * RDMA staging buffers are obtained through this small abstraction so we
 * can switch between the historical per-clone allocator and a process-
 * wide free-list pool without touching verbs lifecycle, signal ordering,
 * or MR registration semantics. The provider populates an
 * rdma_buffer_meta (declared publicly in RDMA_Transporter.hpp because the
 * transporter holds one per buffer); the matching release call uses the
 * meta to dispatch back to the original source.
 *
 * Two providers are available:
 *   PERCLONE : posix_memalign(4096) on acquire, std::free on release.
 *              Bit-for-bit identical to pre-Phase-1 behavior.
 *   SHARED   : process-lifetime free list keyed by request size. Each
 *              chunk is a full backing allocation (mmap or
 *              posix_memalign), never a slice of a larger region. On
 *              release the chunk returns to its size's free list and
 *              madvise(MADV_DONTNEED) is issued so the next acquirer's
 *              memset(0) demand-faults pages onto its own NUMA node
 *              and preserves the first-touch locality of the perclone
 *              path.
 *
 * Both providers can request hugepage backing via mmap MAP_HUGETLB at
 * an explicit 2 MiB page size. On failure (typically ENOMEM when
 * hugepages are not reserved) the allocation falls back to
 * posix_memalign(4096) and a single info-level line is emitted per
 * process lifetime. Pinning behavior (and therefore RLIMIT_MEMLOCK
 * exposure) is unchanged for the fallback case.
 *
 * Mode selection is by environment variable to avoid touching the
 * management config schema in this PR:
 *   NDB_RDMA_POOL_MODE   unset | perclone   -> PERCLONE provider
 *                         shared             -> SHARED provider
 *   NDB_RDMA_HUGEPAGES   unset | off        -> 4 KiB pages
 *                         best_effort        -> 2 MiB pages, fall back
 *
 * Threading: acquire/release run only on connect/disconnect paths and
 * may execute concurrently across transporters. The shared pool takes
 * a global std::mutex on every acquire/release. The cadence is at most
 * one per transporter per reconnect; the lock is never on the data
 * path.
 *
 * Why no slicing of larger regions in this PR:
 *   ibv_reg_mr() binds an iova range to a specific PD. When a chunk
 *   crosses transporters with different PDs across a connect/disconnect
 *   cycle, ibv_dereg_mr() fully removes the old mapping before the chunk
 *   returns to the free list. Each chunk is a self-contained allocation
 *   so two PDs never map overlapping ranges concurrently. PD sharing /
 *   MR sharing across clones is explicitly deferred.
 */
namespace {

constexpr size_t RDMA_HUGEPAGE_SIZE = 2u * 1024u * 1024u; /* 2 MiB */

enum class rdma_pool_mode_t { PERCLONE, SHARED };
enum class rdma_hugepage_mode_t { OFF, BEST_EFFORT };

static rdma_pool_mode_t rdma_pool_mode_cached() {
  static const rdma_pool_mode_t cached = []() {
    const char *e = std::getenv("NDB_RDMA_POOL_MODE");
    if (e == nullptr || std::strcmp(e, "perclone") == 0) {
      return rdma_pool_mode_t::PERCLONE;
    }
    if (std::strcmp(e, "shared") == 0) {
      g_eventLogger->info(
          "RDMA: NDB_RDMA_POOL_MODE=shared (pool-backed buffer provider)");
      return rdma_pool_mode_t::SHARED;
    }
    g_eventLogger->info(
        "RDMA: NDB_RDMA_POOL_MODE has unrecognised value '%s'; defaulting "
        "to perclone",
        e);
    return rdma_pool_mode_t::PERCLONE;
  }();
  return cached;
}

static rdma_hugepage_mode_t rdma_hugepage_mode_cached() {
  static const rdma_hugepage_mode_t cached = []() {
    const char *e = std::getenv("NDB_RDMA_HUGEPAGES");
    if (e == nullptr || std::strcmp(e, "off") == 0) {
      return rdma_hugepage_mode_t::OFF;
    }
    if (std::strcmp(e, "best_effort") == 0) {
      g_eventLogger->info(
          "RDMA: NDB_RDMA_HUGEPAGES=best_effort (try 2 MiB hugepages, "
          "fall back to 4 KiB on failure)");
      return rdma_hugepage_mode_t::BEST_EFFORT;
    }
    g_eventLogger->info(
        "RDMA: NDB_RDMA_HUGEPAGES has unrecognised value '%s'; defaulting "
        "to off",
        e);
    return rdma_hugepage_mode_t::OFF;
  }();
  return cached;
}

/*
 * Attempt a 2 MiB hugepage-backed mmap of `bytes` rounded up to the
 * hugepage size. Returns nullptr on failure; the caller should fall
 * back to the posix path. On success *out_mapped_bytes is the actual
 * map length, which the matching free path needs for munmap.
 */
static void *rdma_try_hugepage_alloc(size_t bytes,
                                     size_t *out_mapped_bytes) {
  if (bytes == 0) return nullptr;
#ifdef MAP_HUGETLB
  const size_t mask = RDMA_HUGEPAGE_SIZE - 1u;
  const size_t mapped = (bytes + mask) & ~mask;
  void *p = mmap(nullptr, mapped, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (p == MAP_FAILED) return nullptr;
  *out_mapped_bytes = mapped;
  return p;
#else
  (void)out_mapped_bytes;
  return nullptr;
#endif
}

/*
 * 4 KiB-page posix_memalign() backing. Preserves the historical
 * allocator behavior. The +memset(0) happens later inside
 * rdma_buffer_acquire(), so both providers share the same first-touch
 * locality property.
 */
static void *rdma_posix_alloc(size_t bytes, size_t *out_mapped_bytes) {
  if (bytes == 0) return nullptr;
  void *p = nullptr;
  if (posix_memalign(&p, 4096, bytes) != 0) return nullptr;
  *out_mapped_bytes = bytes;
  return p;
}

/*
 * Process-once latch that records the first hugepage fallback so the
 * event log emits one line, not one per allocation under heavy
 * connect/reconnect churn. A benign race between threads costs at most
 * two adjacent log lines.
 */
static std::atomic<bool> g_rdma_hugepage_fallback_logged{false};

/*
 * Top-level allocation dispatch. Picks hugepage vs posix based on the
 * cached env var, with one-shot logged fallback on hugepage failure.
 * Returns nullptr only when posix_memalign also fails (out of memory).
 */
static void *rdma_alloc_one(size_t bytes, bool *out_was_hugepage,
                            size_t *out_mapped_bytes) {
  if (rdma_hugepage_mode_cached() == rdma_hugepage_mode_t::BEST_EFFORT) {
    size_t mapped = 0;
    void *p = rdma_try_hugepage_alloc(bytes, &mapped);
    if (p != nullptr) {
      *out_was_hugepage = true;
      *out_mapped_bytes = mapped;
      return p;
    }
    bool expected = false;
    if (g_rdma_hugepage_fallback_logged.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel)) {
      g_eventLogger->info(
          "RDMA: 2 MiB hugepage allocation failed (errno=%d %s); falling "
          "back to 4 KiB pages for this and subsequent buffers",
          errno, std::strerror(errno));
    }
  }
  size_t mapped = 0;
  void *p = rdma_posix_alloc(bytes, &mapped);
  if (p == nullptr) return nullptr;
  *out_was_hugepage = false;
  *out_mapped_bytes = mapped;
  return p;
}

/*
 * Top-level free dispatch. The provenance recorded at acquire time
 * tells us whether to munmap or std::free the chunk.
 */
static void rdma_free_one(void *ptr, bool was_hugepage,
                          size_t mapped_bytes) {
  if (ptr == nullptr) return;
  if (was_hugepage) {
#ifdef MAP_HUGETLB
    munmap(ptr, mapped_bytes);
#else
    /* Should not be reachable: rdma_try_hugepage_alloc returns NULL
     * when MAP_HUGETLB is unavailable, so was_hugepage can only be
     * true if MAP_HUGETLB existed at acquire time. Guard against
     * future drift in compile flags. */
    (void)mapped_bytes;
    std::free(ptr);
#endif
  } else {
    (void)mapped_bytes;
    std::free(ptr);
  }
}

/*
 * Process-wide buffer pool. Free lists are keyed by the user-requested
 * size; each chunk is one full allocation (no slicing). Chunks are
 * never unmapped while the process is alive, so a chunk's address
 * remains stable across acquire/release cycles: the next ibv_reg_mr()
 * installs a fresh translation on the same virtual range against the
 * acquiring transporter's PD.
 */
class rdma_pool {
 public:
  struct chunk {
    void *ptr;
    bool was_hugepage;
    size_t mapped_bytes;
  };

  static rdma_pool &instance() {
    static rdma_pool s_inst;
    return s_inst;
  }

  /*
   * Pop a chunk of the requested size from the free list, or allocate
   * a fresh one on miss. Returns false only when a fresh allocation
   * fails (out of memory).
   */
  bool acquire(size_t bytes, void **out_ptr, bool *out_was_hugepage,
               size_t *out_mapped_bytes) {
    {
      std::lock_guard<std::mutex> g(m_lock);
      auto it = m_free_lists.find(bytes);
      if (it != m_free_lists.end() && !it->second.empty()) {
        const chunk c = it->second.back();
        it->second.pop_back();
        *out_ptr = c.ptr;
        *out_was_hugepage = c.was_hugepage;
        *out_mapped_bytes = c.mapped_bytes;
        return true;
      }
    }
    /* Free-list miss; allocate fresh outside the lock so concurrent
     * connects on other sizes are not blocked by a slow mmap. */
    bool was_hp = false;
    size_t mapped = 0;
    void *p = rdma_alloc_one(bytes, &was_hp, &mapped);
    if (p == nullptr) return false;
    *out_ptr = p;
    *out_was_hugepage = was_hp;
    *out_mapped_bytes = mapped;
    return true;
  }

  /*
   * Return a chunk to its size's free list. madvise(MADV_DONTNEED)
   * drops the underlying pages so the next acquirer's memset(0)
   * demand-faults them onto its own NUMA node. madvise failures are
   * logged once and non-fatal (the chunk is still reusable; only NUMA
   * placement may suffer on reuse).
   */
  void release(size_t bytes, void *ptr, bool was_hugepage,
               size_t mapped_bytes) {
    if (ptr == nullptr) return;
    if (madvise(ptr, mapped_bytes, MADV_DONTNEED) != 0) {
      static std::atomic<bool> s_madvise_warn{false};
      bool expected = false;
      if (s_madvise_warn.compare_exchange_strong(
              expected, true, std::memory_order_acq_rel)) {
        g_eventLogger->info(
            "RDMA: madvise(MADV_DONTNEED) on pool chunk %p (%zu bytes) "
            "failed (errno=%d %s); NUMA locality may degrade on reuse",
            ptr, mapped_bytes, errno, std::strerror(errno));
      }
    }
    std::lock_guard<std::mutex> g(m_lock);
    m_free_lists[bytes].push_back({ptr, was_hugepage, mapped_bytes});
  }

 private:
  std::mutex m_lock;
  std::map<size_t, std::vector<chunk>> m_free_lists;
};

/*
 * Acquire `bytes` of zero-initialised, page-aligned memory under the
 * configured provider. Populates *meta with the provenance the matching
 * release call needs. Returns nullptr on failure (zero size or out of
 * memory) and resets *meta to its "never acquired" state.
 */
static void *rdma_buffer_acquire(size_t bytes, rdma_buffer_meta *meta) {
  meta->was_pooled = false;
  meta->was_hugepage = false;
  meta->mapped_bytes = 0;
  if (bytes == 0) return nullptr;

  void *ptr = nullptr;
  if (rdma_pool_mode_cached() == rdma_pool_mode_t::SHARED) {
    bool was_hp = false;
    size_t mapped = 0;
    if (!rdma_pool::instance().acquire(bytes, &ptr, &was_hp, &mapped)) {
      return nullptr;
    }
    meta->was_pooled = true;
    meta->was_hugepage = was_hp;
    meta->mapped_bytes = mapped;
  } else {
    bool was_hp = false;
    size_t mapped = 0;
    ptr = rdma_alloc_one(bytes, &was_hp, &mapped);
    if (ptr == nullptr) return nullptr;
    meta->was_hugepage = was_hp;
    meta->mapped_bytes = mapped;
  }
  std::memset(ptr, 0, bytes);
  return ptr;
}

/*
 * Release a buffer previously returned by rdma_buffer_acquire().
 * `bytes` is the original request size (needed by the pool to pick the
 * right size bucket); `meta` is the provenance recorded at acquire
 * time. After return *meta is reset.
 */
static void rdma_buffer_release(void *ptr, size_t bytes,
                                rdma_buffer_meta *meta) {
  if (ptr == nullptr) return;
  if (meta->was_pooled) {
    rdma_pool::instance().release(bytes, ptr, meta->was_hugepage,
                                  meta->mapped_bytes);
  } else {
    rdma_free_one(ptr, meta->was_hugepage, meta->mapped_bytes);
  }
  meta->was_pooled = false;
  meta->was_hugepage = false;
  meta->mapped_bytes = 0;
}

}  // namespace

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
      m_recv_comp_channel(nullptr),
      m_recv_comp_events_pending(0),
      m_send_buf(nullptr),
      m_recv_buf(nullptr),
      m_app_buf(nullptr),
      m_effective_inline_threshold(0),
      m_local_send_seq(0),
      m_local_recv_seq(0),
      m_peer_ack_seq(0),
      m_peer_recv_credits(0),
      m_pending_credit_grant(0),
      m_local_recv_posted(0),
      m_send_slots(nullptr),
      m_send_slots_in_flight(0),
      m_next_send_slot(0),
      m_recv_slots(nullptr),
      m_recv_ready_queue(nullptr),
      m_recv_queue_head(0),
      m_recv_queue_tail(0),
      m_recv_queue_count(0) {
  /* m_last_stats_log_ns gets its 0 default from the in-class
   * initializer in RDMA_Transporter.hpp. */
  m_overload_limit = rdma_overload_limit(config);
  m_slowdown_limit = m_overload_limit * 6 / 10;
}

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
      m_recv_comp_channel(nullptr),
      m_recv_comp_events_pending(0),
      m_send_buf(nullptr),
      m_recv_buf(nullptr),
      m_app_buf(nullptr),
      m_effective_inline_threshold(0),
      m_local_send_seq(0),
      m_local_recv_seq(0),
      m_peer_ack_seq(0),
      m_peer_recv_credits(0),
      m_pending_credit_grant(0),
      m_local_recv_posted(0),
      m_send_slots(nullptr),
      m_send_slots_in_flight(0),
      m_next_send_slot(0),
      m_recv_slots(nullptr),
      m_recv_ready_queue(nullptr),
      m_recv_queue_head(0),
      m_recv_queue_tail(0),
      m_recv_queue_count(0) {
  /* m_last_stats_log_ns gets its 0 default from the in-class
   * initializer in RDMA_Transporter.hpp. */
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
  m_overload_limit = other->m_overload_limit;
  m_slowdown_limit = other->m_slowdown_limit;
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
  if (conf == nullptr) return false;
  if (conf->type != tt_RDMA_TRANSPORTER) return false;
  if (conf->rdma.sendBufferSize != m_send_buffer_size) return false;
  if (conf->rdma.recvBufferSize != m_recv_buffer_size) return false;
  if (conf->rdma.queueDepth != m_queue_depth) return false;
  if (conf->rdma.inlineThreshold != m_inline_threshold) return false;
  if (conf->rdma.completionPollBudget != m_completion_poll_budget) return false;
  if (conf->rdma.spintime != m_spintime) return false;
  if (conf->rdma.rdmaPort != m_rdma_port) return false;
  if (conf->rdma.gidIndex != m_gid_index) return false;
  if (conf->rdma.trafficClass != m_traffic_class) return false;
  if (conf->rdma.retryCount != m_retry_count) return false;
  if (conf->rdma.rnrRetryCount != m_rnr_retry_count) return false;
  if (!rdma_device_name_equal(conf->rdma.deviceName, m_device_name)) {
    return false;
  }
  if (rdma_overload_limit(conf) != m_overload_limit) return false;
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
   * Match TCP's sequencing: disconnect the control socket now so the
   * registry can observe link death, but keep verbs resources alive
   * until releaseAfterDisconnect() when send buffers are already disabled.
   */
  Transporter::disconnectImpl();
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
   * Native RDMA send path. Once endpoint exchange has driven the QP to
   * RTS, the registry reaches this through performSend() just like the
   * TCP and SHM transporters.
   *
   * Sequence:
   *   1. Reap completions for previously posted sends. This frees
   *      staging slots and updates wire byte counters.
   *   2. While we have remote credits and a free slot, fetch iovec
   *      bytes from the send buffer, copy complete Protocol6 messages
   *      into a registered staging slot, post one IBV_WR_SEND, and
   *      release those upper-layer send-buffer bytes immediately after
   *      the successful post.
   *
   * Return value follows the existing transporter convention: true if
   * there are still upper-layer bytes to push (more work to do later),
   * false if no send-buffer bytes are pending. In-flight RDMA WRs by
   * themselves are not send-scheduler work once their payload has been
   * copied into the registered staging buffer.
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
   * credits, free slots, or send-buffer data. Messages are never split
   * across two RDMA recv slots, because the receiver's unpacker (TransporterRegistry::
   * unpack -> Packer::unpack_one) can only consume complete Protocol6
   * messages from a contiguous buffer, and a single recv slot is
   * what get_next_read() exposes as a contiguous buffer to the
   * unpacker. The TCP transporter side-steps this because its
   * receive buffer is a contiguous ring; RDMA cannot share that
   * shortcut.
   */
  bool send_buffer_may_have_more = false;
  if (m_send_slots_in_flight.load(std::memory_order_relaxed) >=
          m_queue_depth ||
      m_peer_recv_credits.load(std::memory_order_acquire) == 0) {
    struct iovec iov[1];
    send_buffer_may_have_more = (fetch_send_iovec_data(iov, 1) > 0);
  }

  while (m_send_slots_in_flight.load(std::memory_order_relaxed) <
             m_queue_depth &&
         m_peer_recv_credits.load(std::memory_order_acquire) > 0) {
    /* Pull iovec descriptors describing the send buffer's pending data. */
    constexpr Uint32 IOV_BATCH = 8;
    struct iovec iov[IOV_BATCH];
    const Uint32 n_iov = fetch_send_iovec_data(iov, IOV_BATCH);
    if (n_iov == 0) {
      /* Send buffer is empty. */
      send_buffer_may_have_more = false;
      break;
    }

    /*
     * The send-buffer cursor always starts at the first unsent byte:
     * after a successful post we immediately call iovec_data_sent()
     * because the payload has already been copied into m_send_buf.
     */
    Uint32 cursor_iov = 0;
    Uint32 cursor_off = 0;

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
      send_buffer_may_have_more = true;
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
        m_stats.qp_fatal_events.fetch_add(1u, std::memory_order_relaxed);
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
       * Either the next message is not yet fully visible in the iovec
       * batch, or (handled earlier) the next message is larger than
       * the slot. In every case there is still send-buffer data, but
       * nothing useful to post this round.
       */
      break;
    }
    send_buffer_may_have_more =
        (cursor_iov < n_iov) || (n_iov == IOV_BATCH);

    /* Build the framing header. We piggyback the pending ack and
     * credit grant on this SEND. Use atomic exchange so a concurrent
     * increment from reap_recv_completions() (which may run on the
     * receive thread without the send lock) cannot be silently
     * dropped between read and clear. The claimed value is rolled
     * back via fetch_add() on any post failure below. */
    const Uint32 claimed_grant =
        m_pending_credit_grant.exchange(0, std::memory_order_acq_rel);
    const Uint16 credit_delta = (claimed_grant > 0xFFFFu)
                                    ? (Uint16)0xFFFFu
                                    : (Uint16)claimed_grant;
    /*
     * m_local_recv_seq is incremented on the receive thread; relaxed
     * load is sufficient here because the ack_seq we stamp is an
     * advisory hint to the peer rather than a synchronizer.
     */
    const Uint32 ack_seq_snapshot =
        m_local_recv_seq.load(std::memory_order_relaxed);
    encode_msg_header(slot_buf, payload_len, m_local_send_seq,
                      ack_seq_snapshot, credit_delta, /*flags=*/0u);

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
      /*
       * Roll back the claimed credit grant. The atomic fetch_add()
       * keeps any concurrent increment from the receive thread that
       * landed after our exchange().
       */
      if (claimed_grant > 0) {
        m_pending_credit_grant.fetch_add(claimed_grant,
                                         std::memory_order_acq_rel);
      }
      g_eventLogger->error(
          "RDMA[node %u->%u]: ibv_post_send failed (rc=%d errno=%d %s); "
          "disconnecting",
          (unsigned)localNodeId, (unsigned)remoteNodeId, rc, errno,
          std::strerror(errno));
      m_stats.qp_fatal_events.fetch_add(1u, std::memory_order_relaxed);
      report_error(TE_RDMA_QP_ERROR, "ibv_post_send failed");
      start_disconnecting(rc, /*send_source=*/true);
      return false;
    }

    /* Commit state changes only after a successful post. The payload
     * now lives in m_send_buf, so the upper-layer send buffer can be
     * released immediately; the completion path only frees this staging
     * slot and updates wire byte counters. */
    m_send_slots[slot].payload_len = payload_len;
    m_send_slots[slot].in_flight = true;
    m_send_slots_in_flight.fetch_add(1u, std::memory_order_relaxed);
    m_local_send_seq++;
    /* One credit consumed against the peer's RQ. fetch_sub() is
     * race-free because only the send-lock owner ever decrements;
     * the receive thread only increments via credit_delta. */
    m_peer_recv_credits.fetch_sub(1u, std::memory_order_acq_rel);
    iovec_data_sent((int)payload_len);

    /* Observability bumps: count this WR as posted, optionally as an
     * inline send, and record the staged payload bytes. */
    m_stats.send_posted.fetch_add(1u, std::memory_order_relaxed);
    if (inline_used)
      m_stats.send_inline.fetch_add(1u, std::memory_order_relaxed);
    m_stats.copied_send_bytes.fetch_add((Uint64)payload_len,
                                        std::memory_order_relaxed);
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
   *
   * The actual WR build / post lives in post_credit_only_locked() so
   * the receive-thread credit emission path (recv_thread_emit_credit_only)
   * can reuse the exact same wire encoding and bookkeeping under the
   * send lock.
   */
  const Uint32 credit_threshold =
      m_queue_depth > 1 ? (m_queue_depth / 2) : 1u;
  const Uint32 low_credit_threshold =
      m_queue_depth > 3 ? (m_queue_depth / 4) : 1u;
  const Uint32 pending_now =
      m_pending_credit_grant.load(std::memory_order_acquire);
  const Uint32 peer_credits_now =
      m_peer_recv_credits.load(std::memory_order_acquire);
  const bool needs_refill =
      (pending_now >= credit_threshold) ||
      (pending_now > 0 && peer_credits_now < low_credit_threshold);
  if (needs_refill) {
    (void)post_credit_only_locked();
  }

  /*
   * If the loop exited because the peer-credit pool is empty while we
   * still have free slots, this is a credit-bound stall. Note we do
   * not differentiate "no data to send" from "data to send but no
   * credits"; in steady state the registry only calls doSend() when
   * there are bytes pending, so the over-counting in idle periods is
   * acceptable for an operator-facing diagnostic.
   */
  if (m_peer_recv_credits.load(std::memory_order_acquire) == 0 &&
      m_send_slots_in_flight.load(std::memory_order_relaxed) <
          m_queue_depth) {
    m_stats.send_credit_stalls.fetch_add(1u, std::memory_order_relaxed);
  }

  /* Periodic stats heartbeat for diagnostic visibility. Bounded to
   * one log line per RDMA_STATS_HEARTBEAT_NS window so this never
   * floods the event log under load. */
  maybe_log_stats_heartbeat();

  /* Tell the caller whether more upper-layer send work is outstanding.
   * In-flight WR completions are reaped by the next doSend() or by the
   * receive-thread credit path before it needs a slot. */
  return send_buffer_may_have_more;
}

bool RDMA_Transporter::post_credit_only_locked() {
  /*
   * Single-source-of-truth helper for building and posting a zero-
   * payload CREDIT_ONLY WR. Callers (doSend() on the send thread,
   * recv_thread_emit_credit_only() on the receive thread) must hold
   * the per-transporter send lock, because we mutate the same shared
   * state doSend() touches and ibv_post_send() must be serialized
   * against itself on the same QP.
   *
   * Preconditions are checked here so callers can be minimal: we no-op
   * when the link is not yet live, the pending grant is zero, or no free
   * send slot exists. If the normal peer data-credit pool is empty, the
   * message is flagged with RDMA_MSG_FLAG_CONTROL_RESERVE and uses the
   * peer's unadvertised reserved control receive slot instead of a data
   * credit. This is what prevents mutual credit-return deadlock when
   * both peers have consumed all advertised DATA credits.
   *
   * Bookkeeping update mirrors the previous in-line block that used
   * to live in doSend(); see the rationale comments above the caller
   * in doSend() for the threshold/policy discussion.
   */
  if (m_qp == nullptr || m_send_slots == nullptr ||
      m_send_buf == nullptr || m_send_mr == nullptr) {
    return false;
  }
  if (m_pending_credit_grant.load(std::memory_order_acquire) == 0)
    return false;
  if (m_send_slots_in_flight.load(std::memory_order_relaxed) >=
      m_queue_depth)
    return false;
  const Uint32 peer_credits_snapshot =
      m_peer_recv_credits.load(std::memory_order_acquire);
  const bool use_control_reserve = (peer_credits_snapshot == 0);

  const Uint32 slot_size = send_slot_size_or_zero();
  if (slot_size == 0) return false;

  const Uint32 slot = find_free_send_slot();
  if (slot == UINT32_MAX) return false;

  char *const slot_buf =
      (char *)m_send_buf + (size_t)slot * (size_t)slot_size;
  /*
   * Atomic exchange to claim the pending grant. Any concurrent
   * receive-side increment that lands after this returns the value
   * into the next emit slot rather than being lost. The full value
   * is restored via fetch_add() on a post failure below.
   */
  const Uint32 claimed_grant =
      m_pending_credit_grant.exchange(0, std::memory_order_acq_rel);
  if (claimed_grant == 0) {
    /* Another emitter (doSend / parallel recv-thread call) already
     * shipped the pending grant. Nothing to do here. */
    return false;
  }
  const Uint16 credit_delta = (claimed_grant > 0xFFFFu)
                                  ? (Uint16)0xFFFFu
                                  : (Uint16)claimed_grant;
  /*
   * Snapshot m_local_recv_seq with a relaxed load -- the receive
   * thread bumps it under the receive lock; ack_seq is advisory and
   * doesn't synchronize anything in our protocol.
   */
  const Uint32 ack_seq_snapshot =
      m_local_recv_seq.load(std::memory_order_relaxed);
  Uint8 flags = RDMA_MSG_FLAG_CREDIT_ONLY;
  if (use_control_reserve) {
    flags |= RDMA_MSG_FLAG_CONTROL_RESERVE;
  }
  encode_msg_header(slot_buf, /*payload_len=*/0u, m_local_send_seq,
                    ack_seq_snapshot, credit_delta, flags);

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
    /* Restore the claimed credit grant so subsequent emits can
     * retry shipping it. */
    m_pending_credit_grant.fetch_add(claimed_grant,
                                     std::memory_order_acq_rel);
    g_eventLogger->error(
        "RDMA[node %u->%u]: ibv_post_send(CREDIT_ONLY) failed (rc=%d "
        "errno=%d %s); disconnecting",
        (unsigned)localNodeId, (unsigned)remoteNodeId, rc, errno,
        std::strerror(errno));
    m_stats.qp_fatal_events.fetch_add(1u, std::memory_order_relaxed);
    report_error(TE_RDMA_QP_ERROR, "ibv_post_send(CREDIT_ONLY) failed");
    start_disconnecting(rc, /*send_source=*/true);
    return false;
  }

  /*
   * Commit state. CREDIT_ONLY has no payload and no upper-layer
   * send-buffer bytes tied to this WR.
   */
  m_send_slots[slot].payload_len = 0;
  m_send_slots[slot].in_flight = true;
  m_send_slots_in_flight.fetch_add(1u, std::memory_order_relaxed);
  m_local_send_seq++;
  if (!use_control_reserve) {
    m_peer_recv_credits.fetch_sub(1u, std::memory_order_acq_rel);
  }
  m_stats.send_credit_only_out.fetch_add(1u, std::memory_order_relaxed);
  m_stats.send_posted.fetch_add(1u, std::memory_order_relaxed);
  if (credit_inline_used)
    m_stats.send_inline.fetch_add(1u, std::memory_order_relaxed);
  return true;
}

int RDMA_Transporter::get_recv_comp_channel_fd() const {
  /* Reflect the channel-fd accessor used by the registry to add this
   * transporter to the recv thread's epoll set. Returns -1 when the
   * channel is not currently allocated, which is the safe sentinel
   * for "do not add". */
  if (m_recv_comp_channel == nullptr) return -1;
  return m_recv_comp_channel->fd;
}

void RDMA_Transporter::handle_recv_comp_event() {
  /*
   * Drain every event currently sitting on the non-blocking comp
   * channel fd. Each ibv_get_cq_event() call returns one event; we
   * ack each one individually (cheap and simple), and break out of
   * the loop on EAGAIN.
   *
   * Defends against a torn-down channel (m_recv_comp_channel may be
   * NULL if release_verbs_resources() ran between epoll firing and
   * us being scheduled). In that case, the kernel has already
   * auto-removed the now-closed fd from epoll; we just return.
   *
   * Note: we deliberately do NOT call ibv_poll_cq() here. The
   * registry's existing performReceive() path already calls
   * reap_recv_completions() for any transporter whose
   * m_read_transporters bit was set, and that is the function that
   * also re-arms the CQ on exit. Splitting drain (here) and poll
   * (there) keeps the recv-thread structure unchanged.
   */
  if (m_recv_comp_channel == nullptr || m_recv_cq == nullptr) return;
  struct ibv_cq *ev_cq = nullptr;
  void *ev_ctx = nullptr;
  while (ibv_get_cq_event(m_recv_comp_channel, &ev_cq, &ev_ctx) == 0) {
    /* Defensive: a comp channel only ever associates with the recv
     * CQ in our setup, but the verbs API allows multiplexing in
     * principle. Skip events targeting other CQs (cannot happen
     * here, but staying robust is cheap). */
    if (ev_cq == m_recv_cq) {
      ibv_ack_cq_events(m_recv_cq, 1u);
    }
  }
  /* EAGAIN is the expected termination condition for the non-blocking
   * fd; any other errno is treated as transient because we cannot
   * recover here without tearing the link down, and the recv CQ
   * polling path will surface fatal errors on its own. */
}

void RDMA_Transporter::arm_recv_cq() {
  /*
   * Request a one-shot notification for the next CQE that lands on
   * the recv CQ. Idempotent against the verbs API in the sense that
   * calling arm twice in succession is permitted (the second call
   * collapses into the same outstanding arm).
   *
   * No-op when the CQ has not been created yet (allocation failed
   * early in connect) or has already been destroyed (release path
   * sneaked in). A failure here is logged but not fatal: if the
   * arm fails the wakeup path is degraded but the recv-thread
   * pollReceive cadence still drains CQEs at its existing rate.
   */
  if (m_recv_cq == nullptr) return;
  const int rc = ibv_req_notify_cq(m_recv_cq, /*solicited_only=*/0);
  if (rc != 0) {
    g_eventLogger->error(
        "RDMA[node %u->%u]: ibv_req_notify_cq failed (rc=%d errno=%d %s); "
        "recv wakeup degraded to pollReceive cadence",
        (unsigned)localNodeId, (unsigned)remoteNodeId, rc, errno,
        std::strerror(errno));
  }
}

void RDMA_Transporter::recv_thread_emit_credit_only() {
  /*
   * Fast-path lock-free check: the receive thread runs this for every
   * RDMA transporter on every performReceive() iteration; the
   * overwhelming common case is a zero or below-threshold pending
   * grant, and we must not pay the cost of a lock acquisition for
   * those calls.
   *
   * The lock-free read of m_pending_credit_grant races with doSend()'s
   * updates under the send lock, but the race is benign: a torn read
   * still falls in [0, UINT32_MAX], we re-validate the value under
   * the lock inside post_credit_only_locked(), and a missed emit is
   * naturally recovered on the next receive batch.
   *
   * A more aggressive "emit on any pending credit" cadence was tried in
   * this lab on 2026-05-19 and triggered fresh code=274/code=286 with
   * RDMA recv WC flush errors within ~5 minutes of rollout. A
   * subsequent 2 ms rate-limited timer variant still produced
   * IBV_WC_RETRY_EXC_ERR on multi-transporter clones under mdtest load.
   * Keep this path threshold-driven; the reserved control receive lane
   * below solves the zero-credit return deadlock without turning credit
   * emission into a high-rate timer.
   */
  if (m_pending_credit_grant.load(std::memory_order_acquire) == 0) return;
  const Uint32 threshold = m_queue_depth > 1 ? (m_queue_depth / 2) : 1u;
  if (m_pending_credit_grant.load(std::memory_order_acquire) < threshold)
    return;

  /*
   * Acquire the per-transporter send lock. This serializes us against
   * doSend() on this transporter -- and only this transporter, since
   * the lock is keyed by m_transporter_index. The receive thread
   * already holds the global receive lock (per trp.txt), which is
   * compatible with the send lock because the send thread never
   * takes the global receive lock; no deadlock cycle exists.
   */
  lock_send_transporter();
  /*
   * Reap policy on the recv-thread emit path.
   *
   * reap_send_completions() no longer calls iovec_data_sent(): data WRs
   * release upper-layer send buffers immediately after their payload is
   * copied into m_send_buf and ibv_post_send() succeeds. Therefore the
   * receive thread can safely reap both DATA and CREDIT_ONLY completions
   * while holding the per-transporter send lock; it only frees RDMA
   * staging slots and updates counters.
   *
   * Why we still need the reap here. In ndbmtd the send thread
   * iterates only m_pending_send_trps[] (mt.cpp do_send), populated
   * by register_pending_send() when a block thread enqueues outbound
   * data for a transporter. A clone that never has any block thread
   * enqueue outbound data is never visited by the send thread.
   * Without our recv-thread reap, the CREDIT_ONLY slots we post
   * here (and from the doSend-side refill path on other clones)
   * accumulate up to m_queue_depth, post_credit_only_locked() then
   * starts returning false, m_pending_credit_grant cannot drain,
   * peer's m_peer_recv_credits against us drains to zero, peer
   * stops sending, our inbound stalls. This was the GCP_COMMIT-lag
   * symptom reproduced after the Phase F rolling restart
   * (NDB-274/NDB-286 in pnfs-mds, mgmd GCP Monitor 10-152s lag,
   * SignalCounter m_count=1 stuck against the peer).
   *
   * Reaping here also frees slots for outbound-active clones whose
   * send scheduler has no remaining upper-layer data to push.
   */
  if (reap_send_completions() < 0) {
    /* Fatal CQ error: start_disconnecting() already called. */
    unlock_send_transporter();
    return;
  }
  if (post_credit_only_locked()) {
    /*
     * Separate counter so operators can verify the new path is
     * actually firing on clones that previously only had inbound
     * traffic. The cumulative count is also reflected in
     * send_credit_only_out, which post_credit_only_locked()
     * already bumped.
     */
    m_stats.send_credit_only_recv_path.fetch_add(1u,
                                                 std::memory_order_relaxed);
  }
  unlock_send_transporter();
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
  if (m_send_slots_in_flight.load(std::memory_order_relaxed) >=
      m_queue_depth)
    return false;
  if (m_peer_recv_credits.load(std::memory_order_acquire) == 0) return false;
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
  m_stats.reconnect_attempts.fetch_add(1u, std::memory_order_relaxed);

  /* Sanity-check that we are starting from a clean slate. */
  require(m_verbs_ctx == nullptr);
  require(m_pd == nullptr);
  require(m_send_cq == nullptr);
  require(m_recv_cq == nullptr);
  require(m_qp == nullptr);
  require(m_send_mr == nullptr);
  require(m_recv_mr == nullptr);
  require(m_recv_comp_channel == nullptr);
  require(m_recv_comp_events_pending == 0);
  require(m_send_buf == nullptr);
  require(m_recv_buf == nullptr);
  require(m_app_buf == nullptr);

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
    /*
     * No device name configured. On a host with a single HCA this is
     * deterministic. On a multi-HCA host (common for RoCE deployments
     * with separate management / storage / fabric NICs), "first
     * available" is ambiguous: the kernel-reported order is not
     * stable across reboots and may pick a NIC that is not on the
     * cluster's RDMA fabric, producing silent link timeouts or
     * IBV_WC_RETRY_EXC_ERR under load.
     *
     * Refuse implicit selection on multi-HCA hosts. Picking the first
     * kernel-reported device is not stable enough for production RDMA
     * fabrics and can silently bind the transporter to the wrong NIC.
     */
    if (num_devices > 1) {
      g_eventLogger->error(
          "RDMA[node %u->%u]: RdmaDevice not configured but host has "
          "%d HCAs. Set RdmaDevice in config to pin to the cluster "
          "fabric NIC; refusing ambiguous first-available selection.",
          (unsigned)localNodeId, (unsigned)remoteNodeId, num_devices);
      ibv_free_device_list(dev_list);
      return false;
    }
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
   * receive path parks completed data slots until the upper layer drains
   * them.
   *
   * The recv CQ is created with a dedicated completion channel
   * (m_recv_comp_channel) so the HCA can wake the receive thread via
   * an eventfd-style fd when a CQE lands. We set the channel fd to
   * non-blocking immediately after creation so the recv-thread can
   * drain ibv_get_cq_event() without ever blocking inside the
   * registry's check_TCP loop. The send CQ keeps its old null-channel
   * shape because doSend()'s drive path is fully poll-driven (we
   * never sleep waiting for our own send completions). */
  m_send_cq = ibv_create_cq(m_verbs_ctx, (int)(m_queue_depth * 2),
                            /*cq_context=*/nullptr, /*channel=*/nullptr,
                            /*comp_vector=*/0);
  if (m_send_cq == nullptr) {
    g_eventLogger->error("RDMA: ibv_create_cq(send) failed (errno=%d %s)",
                         errno, std::strerror(errno));
    release_verbs_resources();
    return false;
  }
  m_recv_comp_channel = ibv_create_comp_channel(m_verbs_ctx);
  if (m_recv_comp_channel == nullptr) {
    g_eventLogger->error(
        "RDMA: ibv_create_comp_channel(recv) failed (errno=%d %s)", errno,
        std::strerror(errno));
    release_verbs_resources();
    return false;
  }
  /* Set the channel fd to non-blocking so handle_recv_comp_event() can
   * drain it with a tight ibv_get_cq_event() loop and break out cleanly
   * on EAGAIN. fcntl failures here would silently leave the fd
   * blocking, which would deadlock the recv thread the first time we
   * tried to drain an empty channel, so treat the failure as fatal. */
  {
    const int fl = fcntl(m_recv_comp_channel->fd, F_GETFL);
    if (fl < 0 ||
        fcntl(m_recv_comp_channel->fd, F_SETFL, fl | O_NONBLOCK) < 0) {
      g_eventLogger->error(
          "RDMA: failed to set comp_channel fd %d to O_NONBLOCK "
          "(errno=%d %s)",
          m_recv_comp_channel->fd, errno, std::strerror(errno));
      release_verbs_resources();
      return false;
    }
  }
  m_recv_cq = ibv_create_cq(m_verbs_ctx, (int)(m_queue_depth * 2),
                            /*cq_context=*/nullptr, m_recv_comp_channel,
                            /*comp_vector=*/0);
  if (m_recv_cq == nullptr) {
    g_eventLogger->error("RDMA: ibv_create_cq(recv) failed (errno=%d %s)",
                         errno, std::strerror(errno));
    release_verbs_resources();
    return false;
  }

  /* Step 5: staging buffers + memory regions.
   *
   * Buffers come from the file-local buffer provider (Phase 1). The
   * provider populates m_*_buf_meta so release_verbs_resources() can
   * return the chunk to the same source. The buffer pointer itself is
   * still page-aligned and zero-initialized, so downstream code (slot
   * indexing, ibv_reg_mr) is unchanged.
   *
   * MR registration remains strictly per-clone; the chunk hands its
   * iova range to this transporter's PD via ibv_reg_mr() below. On
   * disconnect ibv_dereg_mr() removes the iova mapping before the
   * chunk goes back to the provider, so no two PDs ever map the same
   * range concurrently. */
  m_send_buf = rdma_buffer_acquire(m_send_buffer_size, &m_send_buf_meta);
  if (m_send_buf == nullptr) {
    g_eventLogger->error(
        "RDMA: failed to allocate %u bytes of send staging memory",
        m_send_buffer_size);
    release_verbs_resources();
    return false;
  }
  m_recv_buf = rdma_buffer_acquire(m_recv_buffer_size, &m_recv_buf_meta);
  if (m_recv_buf == nullptr) {
    g_eventLogger->error(
        "RDMA: failed to allocate %u bytes of recv staging memory",
        m_recv_buffer_size);
    release_verbs_resources();
    return false;
  }
  /*
   * Allocate the reserved application-visible recv buffer. The current
   * safe slot-ownership rule keeps the HCA slot parked in m_recv_buf
   * until consume_received_bytes() finishes draining it, so the data
   * path no longer copies payload into m_app_buf. The allocation is
   * preserved as scaffolding for a future decoupled app-buffer pool;
   * see the m_app_buf comment in RDMA_Transporter.hpp for context.
   */
  m_app_buf = rdma_buffer_acquire(m_recv_buffer_size, &m_app_buf_meta);
  if (m_app_buf == nullptr) {
    g_eventLogger->error(
        "RDMA: failed to allocate %u bytes of app-readable recv mirror",
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
  /* Snapshot the cross-thread counters with relaxed loads to decide
   * whether to emit a final summary line. The values do not need to be
   * mutually consistent, only "non-zero somewhere". */
  if (m_stats.send_posted.load(std::memory_order_relaxed) != 0 ||
      m_stats.recv_completions_ok.load(std::memory_order_relaxed) != 0 ||
      m_stats.recv_credit_only_in.load(std::memory_order_relaxed) != 0 ||
      m_stats.send_completion_errors.load(std::memory_order_relaxed) != 0 ||
      m_stats.recv_completion_errors.load(std::memory_order_relaxed) != 0 ||
      m_stats.reconnect_attempts.load(std::memory_order_relaxed) != 0) {
    log_stats();
  }

  /*
   * Destruction order is the reverse of construction. Each step ignores
   * its return value because there is nothing meaningful we can do in
   * the failure case beyond logging, and we already log allocation
   * failures elsewhere.
   *
   * Important: ibv_destroy_cq blocks while there are unacked CQ events
   * outstanding. handle_recv_comp_event() normally acks each event as
   * it drains, but if we tear down mid-flight (e.g. release after a
   * fatal CQ poll failure) some events may still be unacked. Drain
   * any residual events and ack them in one batched call before we
   * destroy the recv CQ -- the call is a no-op when nothing is
   * pending. Skip the drain if the comp channel was never set up
   * (allocate failed early). */
  if (m_qp != nullptr) {
    ibv_destroy_qp(m_qp);
    m_qp = nullptr;
  }
  if (m_recv_cq != nullptr) {
    if (m_recv_comp_channel != nullptr) {
      struct ibv_cq *ev_cq = nullptr;
      void *ev_ctx = nullptr;
      while (ibv_get_cq_event(m_recv_comp_channel, &ev_cq, &ev_ctx) == 0) {
        m_recv_comp_events_pending++;
      }
      if (m_recv_comp_events_pending > 0) {
        ibv_ack_cq_events(m_recv_cq,
                          (unsigned)m_recv_comp_events_pending);
        m_recv_comp_events_pending = 0;
      }
    }
    ibv_destroy_cq(m_recv_cq);
    m_recv_cq = nullptr;
  }
  if (m_recv_comp_channel != nullptr) {
    ibv_destroy_comp_channel(m_recv_comp_channel);
    m_recv_comp_channel = nullptr;
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
  /*
   * Buffer release runs AFTER ibv_dereg_mr() above, so the HCA can no
   * longer touch the memory by the time the chunk goes back to its
   * provider. The provider may return the chunk to the shared pool (in
   * NDB_RDMA_POOL_MODE=shared) or free it outright (perclone). Either
   * way the m_*_buf_meta provenance is consumed and reset. */
  if (m_recv_buf != nullptr) {
    rdma_buffer_release(m_recv_buf, m_recv_buffer_size, &m_recv_buf_meta);
    m_recv_buf = nullptr;
  }
  if (m_app_buf != nullptr) {
    rdma_buffer_release(m_app_buf, m_recv_buffer_size, &m_app_buf_meta);
    m_app_buf = nullptr;
  }
  if (m_send_buf != nullptr) {
    rdma_buffer_release(m_send_buf, m_send_buffer_size, &m_send_buf_meta);
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
  m_peer_ack_seq = 0;
  /*
   * Atomic stores: reset_wire_state() runs in the post-disconnect path
   * where the per-transporter send lock is held and no other thread is
   * touching these fields, but using .store() keeps the intent explicit
   * and avoids the operator=() overload that would otherwise emit a
   * full memory barrier for what is logically a single-threaded reset.
   */
  m_local_recv_seq.store(0, std::memory_order_relaxed);
  m_peer_recv_credits.store(0, std::memory_order_relaxed);
  m_pending_credit_grant.store(0, std::memory_order_relaxed);
  m_local_recv_posted.store(0, std::memory_order_relaxed);
  m_wire_bytes_sent.store(0, std::memory_order_relaxed);
  m_wire_bytes_received.store(0, std::memory_order_relaxed);
  m_send_slots_in_flight.store(0u, std::memory_order_relaxed);
  m_next_send_slot = 0;
  m_recv_queue_head = 0;
  m_recv_queue_tail = 0;
  m_recv_queue_count.store(0u, std::memory_order_relaxed);
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
    m_send_slots[i].payload_len = 0;
    m_send_slots[i].in_flight = false;
  }
  m_send_slots_in_flight.store(0u, std::memory_order_relaxed);
  m_next_send_slot = 0;
  return true;
}

void RDMA_Transporter::release_send_slot_state() {
  delete[] m_send_slots;
  m_send_slots = nullptr;
  m_send_slots_in_flight.store(0u, std::memory_order_relaxed);
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
    m_stats.cq_polls_send.fetch_add(1u, std::memory_order_relaxed);
    const int n = ibv_poll_cq(m_send_cq, take, wc);
    if (n < 0) {
      g_eventLogger->error(
          "RDMA[node %u->%u]: ibv_poll_cq(send) returned %d (errno=%d %s)",
          (unsigned)localNodeId, (unsigned)remoteNodeId, n, errno,
          std::strerror(errno));
      m_stats.qp_fatal_events.fetch_add(1u, std::memory_order_relaxed);
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
        m_stats.qp_fatal_events.fetch_add(1u, std::memory_order_relaxed);
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
        m_stats.send_completion_errors.fetch_add(
            1u, std::memory_order_relaxed);
        if (wc[i].status == IBV_WC_RNR_RETRY_EXC_ERR) {
          m_stats.rnr_events.fetch_add(1u, std::memory_order_relaxed);
        } else if (wc[i].status == IBV_WC_RETRY_EXC_ERR) {
          m_stats.retry_exceeded_events.fetch_add(
              1u, std::memory_order_relaxed);
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
      const Uint32 payload_len = m_send_slots[slot].payload_len;
      m_send_slots[slot].in_flight = false;
      m_send_slots[slot].payload_len = 0;
      require(m_send_slots_in_flight.load(std::memory_order_relaxed) > 0);
      m_send_slots_in_flight.fetch_sub(1u, std::memory_order_relaxed);
      /* Bookkeeping: the wire bytes consumed by the HCA equal the
       * header plus the staged payload. Match the TCP convention of
       * tracking total bytes successfully transmitted. */
      const Uint64 wire_bytes =
          (Uint64)RDMA_MSG_HEADER_BYTES + (Uint64)payload_len;
      m_bytes_sent += wire_bytes;
      m_wire_bytes_sent.fetch_add(wire_bytes, std::memory_order_relaxed);
      m_stats.send_completions_ok.fetch_add(1u,
                                            std::memory_order_relaxed);
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
    m_stats.cq_budget_hits_send.fetch_add(1u, std::memory_order_relaxed);
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
  m_recv_queue_count.store(0u, std::memory_order_relaxed);
  return true;
}

void RDMA_Transporter::release_recv_slot_state() {
  delete[] m_recv_slots;
  m_recv_slots = nullptr;
  delete[] m_recv_ready_queue;
  m_recv_ready_queue = nullptr;
  m_recv_queue_head = 0;
  m_recv_queue_tail = 0;
  m_recv_queue_count.store(0u, std::memory_order_relaxed);
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
    /*
     * Receive ready-queue backpressure under the safe slot-ownership
     * rule. Each data-message WC pops a WR off the RQ and parks the
     * slot in m_recv_ready_queue until consume_received_bytes() drains
     * it and re-posts the same WR. The ready queue therefore caps the
     * total number of unposted data slots, so when it is full we must
     * not poll the CQ further -- doing so could surface another data
     * WC for a slot we have no room to enqueue. The QP's RNR retry
     * (rnr_retry=7 + min_rnr_timer=16) keeps the sender retrying
     * harmlessly until the unpacker drains a slot here, at which
     * point consume_received_bytes() reposts the slot and credit.
     */
    if (m_recv_queue_count.load(std::memory_order_relaxed) >= m_queue_depth) {
      /*
       * Ready queue is fully parked. Skip this poll round; the next
       * pollReceive() pass (after the unpacker has consumed at least
       * one slot via consume_received_bytes) will resume reaping.
       */
      break;
    }
    const Uint32 ready_capacity =
        m_queue_depth -
        m_recv_queue_count.load(std::memory_order_relaxed);
    const Uint32 budget_limit =
        (remaining > (Uint32)CHUNK) ? (Uint32)CHUNK : remaining;
    const Uint32 take_u = (ready_capacity < budget_limit) ? ready_capacity
                                                          : budget_limit;
    const int take = (int)take_u;
    m_stats.cq_polls_recv.fetch_add(1u, std::memory_order_relaxed);
    const int n = ibv_poll_cq(m_recv_cq, take, wc);
    if (n < 0) {
      g_eventLogger->error(
          "RDMA[node %u->%u]: ibv_poll_cq(recv) returned %d (errno=%d %s)",
          (unsigned)localNodeId, (unsigned)remoteNodeId, n, errno,
          std::strerror(errno));
      m_stats.qp_fatal_events.fetch_add(1u, std::memory_order_relaxed);
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
        m_stats.qp_fatal_events.fetch_add(1u, std::memory_order_relaxed);
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
        m_stats.recv_completion_errors.fetch_add(
            1u, std::memory_order_relaxed);
        if (wc[i].status == IBV_WC_RNR_RETRY_EXC_ERR) {
          m_stats.rnr_events.fetch_add(1u, std::memory_order_relaxed);
        } else if (wc[i].status == IBV_WC_RETRY_EXC_ERR) {
          m_stats.retry_exceeded_events.fetch_add(
              1u, std::memory_order_relaxed);
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
      m_wire_bytes_received.fetch_add((Uint64)wc[i].byte_len,
                                      std::memory_order_relaxed);

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
       * over. Relaxed load is fine: the receive thread is the only
       * writer, so the value we see here is what we wrote last.
       */
      const Uint32 expected_seq =
          m_local_recv_seq.load(std::memory_order_relaxed);
      if (peer_seq != expected_seq) {
        g_eventLogger->error(
            "RDMA[node %u->%u]: out-of-order recv seq %u (expected %u)",
            (unsigned)localNodeId, (unsigned)remoteNodeId, peer_seq,
            expected_seq);
        report_error(TE_RDMA_INVALID_HEADER, "RDMA recv seq mismatch");
        start_disconnecting(EPROTO, /*send_source=*/false);
        return -1;
      }
      /* Release ordering on the store pairs with the relaxed load on
       * the send thread, ensuring any data the receive thread published
       * is visible to a send thread that observes the new seq. */
      m_local_recv_seq.store(expected_seq + 1u,
                             std::memory_order_release);
      m_peer_ack_seq = peer_ack;

      /*
       * Apply credit_delta to our pool of peer-recv credits. The send
       * lock is NOT held here (we run on the receive thread), but the
       * counter is std::atomic and only ever incremented by us; the
       * send path only decrements. A torn read against a concurrent
       * decrement is impossible because std::atomic guarantees
       * single-instruction visibility, so the fetch_add() is safe.
       *
       * Saturate at UINT32_MAX to defend against pathological
       * streams. The unsigned-wrap check uses the value we observed
       * BEFORE the add; if the result would wrap, we issue a
       * compare_exchange to clamp to UINT32_MAX.
       */
      if (credit_delta > 0) {
        const Uint32 add = (Uint32)credit_delta;
        Uint32 prev =
            m_peer_recv_credits.load(std::memory_order_acquire);
        while (true) {
          const Uint32 next = (prev > UINT32_MAX - add) ? UINT32_MAX
                                                       : prev + add;
          if (m_peer_recv_credits.compare_exchange_weak(
                  prev, next, std::memory_order_acq_rel,
                  std::memory_order_acquire)) {
            break;
          }
          /* prev was updated by compare_exchange_weak with the actual
           * current value; retry. */
        }
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
        require(m_local_recv_posted.load(std::memory_order_relaxed) > 0);
        m_local_recv_posted.fetch_sub(1u, std::memory_order_relaxed);
        if (!post_one_receive(slot)) {
          m_stats.qp_fatal_events.fetch_add(1u,
                                            std::memory_order_relaxed);
          report_error(TE_RDMA_QP_ERROR, "failed to re-post recv slot");
          start_disconnecting(errno, /*send_source=*/false);
          return -1;
        }
        m_local_recv_posted.fetch_add(1u, std::memory_order_relaxed);
        /*
         * Grant one DATA credit back to the peer for a recycled normal
         * control slot. Reserved-control CREDIT_ONLY messages used the
         * unadvertised receive slot, so reposting that slot only restores
         * the reserve and must not inflate the peer's data-credit pool.
         */
        const bool used_control_reserve =
            (flags & RDMA_MSG_FLAG_CONTROL_RESERVE) != 0;
        if (!used_control_reserve &&
            m_pending_credit_grant.load(std::memory_order_acquire) <
                0xFFFFu) {
          m_pending_credit_grant.fetch_add(1u,
                                           std::memory_order_acq_rel);
        }
        m_stats.recv_credit_only_in.fetch_add(1u,
                                              std::memory_order_relaxed);
        reaped_total++;
        continue;
      }

      /* Normal data message: park the receive slot for the upper-
       * layer unpacker without copying the payload elsewhere and
       * without re-posting the underlying WR. The WC just dequeued
       * the WR from the RQ, so m_local_recv_posted decreases by one;
       * consume_received_bytes() re-posts the slot and grants the
       * peer one credit after the unpacker has finished reading
       * every byte. This safe slot-ownership rule prevents the HCA
       * from landing a new SEND in the same m_recv_buf slot before
       * the upper layer has finished consuming the previous one,
       * which was the source of the earlier app-buffer overwrite
       * hazard. The trade-off is that a slow upper layer can keep
       * data slots parked longer; the QP's infinite RNR retry plus
       * the ready-queue backpressure above turn that into clean
       * sender stalls rather than data corruption.
       */
      require(m_local_recv_posted.load(std::memory_order_relaxed) > 0);
      m_local_recv_posted.fetch_sub(1u, std::memory_order_relaxed);

      /* Park the slot in the ready queue: the unpacker drains it
       * via get_next_read() / consume_received_bytes() directly from
       * the parked m_recv_buf slot. The capacity check above
       * guarantees space; this assertion documents the invariant
       * for code readers. */
      m_recv_slots[slot].payload_len = payload_len;
      m_recv_slots[slot].read_offset = 0;
      require(m_recv_queue_count.load(std::memory_order_relaxed) <
              m_queue_depth);
      m_recv_ready_queue[m_recv_queue_tail] = slot;
      m_recv_queue_tail = (m_recv_queue_tail + 1) % m_queue_depth;
      m_recv_queue_count.fetch_add(1u, std::memory_order_relaxed);
      m_stats.recv_completions_ok.fetch_add(1u,
                                            std::memory_order_relaxed);
      reaped_total++;
    }

    remaining -= (Uint32)n;
    if (n < take) break;  /* CQ drained */
  }
  if (remaining == 0 && reaped_total > 0) {
    /* Mirror reap_send_completions: surface budget exhaustion so
     * operators can correlate it with end-to-end latency. */
    m_stats.cq_budget_hits_recv.fetch_add(1u, std::memory_order_relaxed);
  }
  /*
   * Re-arm the recv CQ now that we have drained it. The arm is a
   * one-shot: the next CQE arriving after this call will fire an
   * event on the comp channel fd and wake the recv thread out of
   * epoll_wait(). There is a small race window between the last
   * ibv_poll_cq() that returned 0 above and this arm -- a CQE
   * arriving in that window will not fire a notification because
   * the arm came after it. We accept that as a benign source of
   * extra latency: the registry's existing poll_RDMA() cadence will
   * pick the CQE up on the next pollReceive() pass.
   *
   * Skip arm on the fatal-error return paths above (which `return -1`
   * directly without reaching this point); those have already torn
   * the link down via start_disconnecting().
   */
  arm_recv_cq();
  /* Periodic stats heartbeat from the recv side too, so transporters
   * that carry only inbound traffic still emit periodic snapshots. */
  maybe_log_stats_heartbeat();
  return reaped_total;
}

bool RDMA_Transporter::has_received_data() const {
  return m_recv_queue_count.load(std::memory_order_relaxed) > 0;
}

void RDMA_Transporter::get_next_read(const void **out_ptr,
                                     Uint32 *out_len) const {
  if (m_recv_queue_count.load(std::memory_order_relaxed) == 0 ||
      m_recv_slots == nullptr || m_recv_buf == nullptr) {
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
   * payload bytes. The payload still lives in the parked m_recv_buf
   * slot: under the safe slot-ownership rule the slot is NOT re-posted
   * to the HCA until consume_received_bytes() finishes draining it,
   * so the HCA cannot land a new SEND into the same buffer behind our
   * back. */
  const char *base = (const char *)m_recv_buf + (size_t)slot * slot_size +
                     RDMA_MSG_HEADER_BYTES;
  if (out_ptr) *out_ptr = base + st.read_offset;
  if (out_len) *out_len = st.payload_len - st.read_offset;
}

void RDMA_Transporter::consume_received_bytes(Uint32 n) {
  if (n == 0 || m_recv_queue_count.load(std::memory_order_relaxed) == 0)
    return;
  const Uint32 slot = m_recv_ready_queue[m_recv_queue_head];
  rdma_recv_slot &st = m_recv_slots[slot];
  require(st.read_offset + n <= st.payload_len);
  st.read_offset += n;
  /* Account bytes successfully handed to the upper layer regardless of
   * whether this consume completes the slot or just advances within
   * it. */
  m_stats.copied_recv_bytes.fetch_add((Uint64)n,
                                      std::memory_order_relaxed);
  if (st.read_offset < st.payload_len) {
    /* Partial unpack; head stays parked at this slot. */
    return;
  }
  /* Slot fully drained from the application's point of view. The
   * underlying recv WR has NOT been re-posted yet: under the safe
   * slot-ownership rule the slot is parked in m_recv_buf for the
   * duration of the consume so the HCA cannot overwrite the payload
   * we just handed to the upper layer. Re-post the same slot now and
   * grant the corresponding credit back to the peer; on repost
   * failure surface a fatal QP error and start the disconnect path
   * (the slot will be recycled by QP destroy). */
  st.payload_len = 0;
  st.read_offset = 0;
  if (!post_one_receive(slot)) {
    m_stats.qp_fatal_events.fetch_add(1u, std::memory_order_relaxed);
    report_error(TE_RDMA_QP_ERROR,
                 "failed to re-post recv slot at consume time");
    start_disconnecting(errno, /*send_source=*/false);
    return;
  }
  m_local_recv_posted.fetch_add(1u, std::memory_order_relaxed);
  /* Grant one credit back to the peer for the recycled slot. The
   * 0xFFFE clamp prevents the counter from saturating in the
   * pathological case of a peer that never acks our grants; the
   * credit_delta field on the wire is Uint16. fetch_add() is safe
   * against the send path's exchange() because credit grants only
   * ever flow up from here and only ever flow down from the send
   * path. */
  if (m_pending_credit_grant.load(std::memory_order_acquire) < 0xFFFFu) {
    m_pending_credit_grant.fetch_add(1u, std::memory_order_acq_rel);
  }
  m_recv_queue_head = (m_recv_queue_head + 1) % m_queue_depth;
  m_recv_queue_count.fetch_sub(1u, std::memory_order_relaxed);
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
  if ((hdr->flags & RDMA_MSG_FLAG_CONTROL_RESERVE) != 0 &&
      (hdr->flags & RDMA_MSG_FLAG_CREDIT_ONLY) == 0) {
    g_eventLogger->error(
        "RDMA: CONTROL_RESERVE flag without CREDIT_ONLY (flags=0x%02x)",
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
  /*
   * Payload bytes must fit in (available - header), where `available` is
   * the WC byte_len for the slot the HCA filled. The HCA can never write
   * more than one full receive slot, so this implicitly caps payload_len
   * to slot_size - header.
   *
   * Historically this function also rejected payload_len greater than
   * MAX_RECV_MESSAGE_BYTESIZE (the per-Protocol6-message limit). That
   * was a leftover from before the Gate-3 sender-side packing: doSend()
   * now legitimately packs MULTIPLE complete Protocol6 messages into a
   * single RDMA payload, up to slot_size - header, so a single payload
   * can be larger than one Protocol6 message. The per-message size
   * limit still applies, but it is enforced by Packer::unpack_one()
   * inside the upper layer where it has the actual signal boundary;
   * checking it here would falsely reject legitimate bulk-copy bursts
   * (the symptom: "payload_len=64740 exceeds MAX_RECV_MESSAGE_BYTESIZE"
   * during "Copying of dictionary information" in start phase 5).
   */
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
  m_stats.recv_posted.fetch_add(1u, std::memory_order_relaxed);
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

  require(m_local_recv_posted.load(std::memory_order_relaxed) == 0);
  for (Uint32 i = 0; i < m_queue_depth; i++) {
    if (!post_one_receive(i)) {
      g_eventLogger->error(
          "RDMA: failed to post initial receive %u/%u; partial state will be "
          "reaped by QP destroy",
          i, m_queue_depth);
      return false;
    }
    m_local_recv_posted.fetch_add(1u, std::memory_order_relaxed);
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
   * RDMA-local atomic byte counters mirror the base Transporter byte
   * counters. We log the atomic mirrors so this function can run from
   * either send or receive context without cross-thread reading the
   * base class's plain Uint64 fields.
   */
  g_eventLogger->info(
      "RDMA[node %u->%u trp_id=%u inst=%u active=%u recv_thread=%u]: "
      "stats reconnects=%llu peer_credits=%u pending_grant=%u "
      "send_in_flight=%u recv_posted_now=%u recv_ready=%u "
      "send_posted=%llu send_ok=%llu send_err=%llu send_inline=%llu "
      "send_credit_only_out=%llu send_credit_only_recv_path=%llu "
      "send_credit_only_timer=%llu send_credit_stalls=%llu copied_send=%llu "
      "recv_posted=%llu recv_ok=%llu recv_err=%llu recv_credit_only_in=%llu "
      "copied_recv=%llu bytes_sent=%llu bytes_received=%llu "
      "cq_polls_send=%llu cq_polls_recv=%llu "
      "cq_budget_hits_send=%llu cq_budget_hits_recv=%llu "
      "rnr=%llu retry_exceeded=%llu qp_fatal=%llu",
      (unsigned)localNodeId, (unsigned)remoteNodeId,
      (unsigned)getTransporterIndex(), (unsigned)m_multi_transporter_instance,
      (unsigned)m_is_active, (unsigned)m_recv_thread_idx,
      (unsigned long long)m_stats.reconnect_attempts.load(
          std::memory_order_relaxed),
      (unsigned)m_peer_recv_credits.load(std::memory_order_acquire),
      (unsigned)m_pending_credit_grant.load(std::memory_order_acquire),
      (unsigned)m_send_slots_in_flight.load(std::memory_order_relaxed),
      (unsigned)m_local_recv_posted.load(std::memory_order_relaxed),
      (unsigned)m_recv_queue_count.load(std::memory_order_relaxed),
      (unsigned long long)m_stats.send_posted.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.send_completions_ok.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.send_completion_errors.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.send_inline.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.send_credit_only_out.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.send_credit_only_recv_path.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.send_credit_only_timer.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.send_credit_stalls.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.copied_send_bytes.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.recv_posted.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.recv_completions_ok.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.recv_completion_errors.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.recv_credit_only_in.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.copied_recv_bytes.load(
          std::memory_order_relaxed),
      (unsigned long long)m_wire_bytes_sent.load(std::memory_order_relaxed),
      (unsigned long long)m_wire_bytes_received.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.cq_polls_send.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.cq_polls_recv.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.cq_budget_hits_send.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.cq_budget_hits_recv.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.rnr_events.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.retry_exceeded_events.load(
          std::memory_order_relaxed),
      (unsigned long long)m_stats.qp_fatal_events.load(
          std::memory_order_relaxed));
}

void RDMA_Transporter::maybe_log_stats_heartbeat() {
  /*
   * Read the monotonic clock. CLOCK_MONOTONIC is unaffected by wall-
   * clock jumps (NTP step, DST), which is what we want for an interval
   * timer. clock_gettime() is a vDSO call on Linux and costs ~10 ns,
   * so it is cheap enough to call on every doSend() / reap_recv_
   * completions() invocation without measurable overhead.
   */
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    /* clock_gettime should not fail with CLOCK_MONOTONIC on Linux,
     * but if it does we simply skip the heartbeat this round. The
     * next call will retry. */
    return;
  }
  const Uint64 now_ns =
      (Uint64)ts.tv_sec * 1000ULL * 1000ULL * 1000ULL + (Uint64)ts.tv_nsec;

  /*
   * Relaxed load is sufficient: this field is purely an interval
   * guard, not a synchronizer for other state. A torn read between
   * the send and receive threads could at worst cause an extra log
   * line; correctness is not affected.
   */
  const Uint64 last_ns =
      m_last_stats_log_ns.load(std::memory_order_relaxed);
  if (last_ns != 0 && (now_ns - last_ns) < RDMA_STATS_HEARTBEAT_NS) {
    return;
  }
  log_stats();
  m_last_stats_log_ns.store(now_ns, std::memory_order_relaxed);
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
  /* Advertise only DATA credits. One posted receive is intentionally
   * left unadvertised as the reserved control lane, so CREDIT_ONLY can
   * still return credits when both peers have consumed all DATA credits. */
  const Uint32 advertised_data_credits =
      (m_queue_depth > 0) ? (m_queue_depth - 1u) : 0u;
  local_rec.recv_credits = htonl(advertised_data_credits);

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

  /*
   * Fail fast when the link is RoCE and either side advertised
   * an all-zero GID. A zero GID indicates the gid_index points at an
   * unconfigured slot (typical when the operator picked a default
   * RdmaGidIndex that does not match the routable IPv4/IPv6 GID for
   * the cluster's RoCE VLAN). Packets cannot be routed reliably, which
   * surfaces under load as IBV_WC_RETRY_EXC_ERR / silent stalls.
   */
  if (peer_rec.link_layer == IBV_LINK_LAYER_ETHERNET) {
    char local_gid_str[64];
    char peer_gid_str[64];
    rdma_format_gid(local_rec.gid, local_gid_str, sizeof(local_gid_str));
    rdma_format_gid(peer_rec.gid, peer_gid_str, sizeof(peer_gid_str));
    if (rdma_gid_is_zero(local_rec.gid) || rdma_gid_is_zero(peer_rec.gid)) {
      g_eventLogger->error(
          "RDMA[%s,node %u->%u]: RoCE endpoint exchange advertised zero "
          "GID (local=%s idx=%u, peer=%s idx=%u). Verify RdmaGidIndex "
          "points at a routable GID for the cluster fabric.",
          side, (unsigned)localNodeId, (unsigned)remoteNodeId,
          local_gid_str, m_gid_index, peer_gid_str,
          (unsigned)peer_rec.gid_index);
      return false;
    } else {
      g_eventLogger->info(
          "RDMA[%s,node %u->%u]: RoCE GIDs local=%s idx=%u peer=%s idx=%u",
          side, (unsigned)localNodeId, (unsigned)remoteNodeId,
          local_gid_str, m_gid_index, peer_gid_str,
          (unsigned)peer_rec.gid_index);
    }
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
   * Initial arm on the recv CQ: now that the QP is at RTS and the
   * receive queue is fully posted, request a notification for the
   * first inbound CQE so the recv thread wakes promptly when the
   * peer starts sending. Every subsequent reap_recv_completions()
   * pass re-arms at its tail.
   */
  arm_recv_cq();

  /*
   * Seed credit accounting from the peer's record. The peer advertised
   * how many receive slots it has posted on its end, which equals the
   * number of in-flight SENDs we are allowed to issue before we must
   * stall. Atomic store under release ordering pairs with the acquire
   * loads in doSend() / post_credit_only_locked() so the seed is
   * visible before the first send attempt.
   */
  m_peer_recv_credits.store(ntohl(peer_rec.recv_credits),
                            std::memory_order_release);
  /* We expect the peer's first SEND to carry seq 0; mirror the
   * sender-side default. Relaxed store is fine because the QP is
   * not at RTS yet from the peer's perspective and no send thread
   * is touching these fields. */
  m_local_send_seq = 0;
  m_local_recv_seq.store(0, std::memory_order_relaxed);
  m_peer_ack_seq = 0;


  g_eventLogger->info(
      "RDMA[%s,node %u->%u]: handshake OK: link=%s local_qpn=%u peer_qpn=%u "
      "local_psn=%u peer_psn=%u mtu=%uB recv_posted=%u credits_from_peer=%u",
      side, (unsigned)localNodeId, (unsigned)remoteNodeId,
      rdma_link_layer_name(peer_rec.link_layer),
      (unsigned)m_qp->qp_num, (unsigned)ntohl(peer_rec.qp_num),
      (unsigned)local_psn, (unsigned)ntohl(peer_rec.psn),
      rdma_mtu_to_bytes((enum ibv_mtu)peer_rec.mtu),
      (unsigned)m_local_recv_posted.load(std::memory_order_relaxed),
      (unsigned)m_peer_recv_credits.load(std::memory_order_acquire));
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
  /*
   * 16 == ~1.28 ms minimum RNR NAK timer. Doubled from the previous
   * default of 12 (~0.64 ms) to give the sender's HCA more time to
   * wait for a transient RQ-empty window on this side before
   * generating an RNR NAK. Combined with qp_transition_to_rts()
   * forcing rnr_retry=7 (infinite retries), this means a sender
   * never escalates a transient slow-drain into IBV_WC_RETRY_EXC_ERR;
   * it will keep retrying until our RQ has a posted slot again. Receive
   * slots are deliberately parked until the upper layer consumes them,
   * so this provides the backstop for legitimate slow-drain windows.
   */
  attr.min_rnr_timer = 16;      /* ~1.28 ms */

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
  /*
   * Force rnr_retry to 7, the IB-spec sentinel for "infinite RNR
   * retries". This guarantees that a transient peer RQ-empty window
   * never escalates to IBV_WC_RETRY_EXC_ERR -- the HCA will keep
   * retrying every min_rnr_timer until the peer's RQ accepts the
   * SEND. Receive slots are re-posted only after the upper layer drains
   * the parked payload, so the infinite retry is a safety net for
   * legitimate application-side backpressure.
   *
   * The configured m_rnr_retry_count is ignored on purpose: lower
   * values were shown on 2026-05-19 to produce status=12
   * (transport retry counter exceeded) on multi-transporter clones
   * during mdtest load even with the credit-return-timer mechanism
   * active.
   */
  attr.rnr_retry = 7;
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
 *     (zeros, max u32/u16, CREDIT_ONLY, HEARTBEAT, control-reserve,
 *     max payload).
 *   - Network byte-order layout assertions on the raw bytes.
 *   - Rejection of bad magic, bad version, bad header_len, and
 *     unknown flag bits.
 *   - Rejection of CREDIT_ONLY/HEARTBEAT messages carrying a payload.
 *   - Rejection of CONTROL_RESERVE without CREDIT_ONLY.
 *   - Acceptance of payload_len greater than MAX_RECV_MESSAGE_BYTESIZE
 *     when `available` is large enough (multi-Protocol6 packed slot).
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
   * CONTROL_RESERVE, and a payload exactly at MAX_RECV_MESSAGE_BYTESIZE. */
  rdma_test_round_trip(/*payload=*/0u, /*send_seq=*/0u, /*ack_seq=*/0u,
                       /*credit_delta=*/0u, /*flags=*/0u);
  rdma_test_round_trip(/*payload=*/0x100u, /*send_seq=*/0x12345678u,
                       /*ack_seq=*/0x9ABCDEF0u, /*credit_delta=*/0x55AAu,
                       /*flags=*/0u);
  rdma_test_round_trip(/*payload=*/0u, /*send_seq=*/1u, /*ack_seq=*/2u,
                       /*credit_delta=*/3u,
                       /*flags=*/RDMA_MSG_FLAG_CREDIT_ONLY);
  rdma_test_round_trip(/*payload=*/0u, /*send_seq=*/3u, /*ack_seq=*/4u,
                       /*credit_delta=*/5u,
                       /*flags=*/RDMA_MSG_FLAG_CREDIT_ONLY |
                           RDMA_MSG_FLAG_CONTROL_RESERVE);
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

  RDMA_Transporter::encode_msg_header(buf, /*payload=*/0u, /*send_seq=*/0u,
                                      /*ack_seq=*/0u, /*credit_delta=*/0u,
                                      RDMA_MSG_FLAG_CONTROL_RESERVE);
  OK(!RDMA_Transporter::validate_msg_header(
      buf, (size_t)RDMA_MSG_HEADER_BYTES, nullptr, nullptr, nullptr,
      nullptr, nullptr));

  RDMA_Transporter::encode_msg_header(
      buf, /*payload=*/4u, /*send_seq=*/0u, /*ack_seq=*/0u,
      /*credit_delta=*/0u,
      RDMA_MSG_FLAG_CREDIT_ONLY | RDMA_MSG_FLAG_CONTROL_RESERVE);
  OK(!RDMA_Transporter::validate_msg_header(
      buf, (size_t)RDMA_MSG_HEADER_BYTES + 4u, nullptr, nullptr, nullptr,
      nullptr, nullptr));

  /* A payload_len greater than the single-Protocol6-message limit is
   * now LEGAL at the RDMA layer because doSend() packs multiple
   * complete Protocol6 messages per slot. The per-message limit is
   * enforced inside Packer::unpack_one(), not here. Verify that a
   * payload_len of MAX_RECV_MESSAGE_BYTESIZE+1 is accepted when
   * `available` is large enough. */
  RDMA_Transporter::encode_msg_header(
      buf, /*payload=*/(Uint32)MAX_RECV_MESSAGE_BYTESIZE + 1u,
      /*send_seq=*/0u, /*ack_seq=*/0u, /*credit_delta=*/0u, /*flags=*/0u);
  /* Allocate a heap buffer large enough; the on-stack buf[64] is
   * insufficient to back this `available` value, but validate_msg_header
   * only reads the header bytes plus checks the numeric `available`
   * parameter, so it does not actually dereference past the header. */
  Uint32 got_big_payload = 0;
  OK(RDMA_Transporter::validate_msg_header(
      buf,
      (size_t)RDMA_MSG_HEADER_BYTES + (size_t)MAX_RECV_MESSAGE_BYTESIZE + 1u,
      &got_big_payload, nullptr, nullptr, nullptr, nullptr));
  OK(got_big_payload == (Uint32)MAX_RECV_MESSAGE_BYTESIZE + 1u);

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

  /* ----- Phase 1: buffer provider tests -----
   *
   * The new file-local rdma_buffer_acquire() / rdma_buffer_release()
   * helpers route through a perclone or shared-pool provider based on
   * the NDB_RDMA_POOL_MODE env var, with optional 2 MiB hugepage
   * backing under NDB_RDMA_HUGEPAGES. Both env vars are read exactly
   * once via Meyer's-singleton caching, so the test must setenv()
   * before the first call to rdma_buffer_acquire() in this binary.
   * The wire-format tests above never touch the provider, so this
   * ordering is safe.
   *
   * In CI we deliberately exercise the SHARED + best_effort hugepage
   * combination. Most test hosts have no hugepages reserved, so the
   * hugepage path is expected to fall back to posix_memalign; that
   * fall-back is a code path we want covered. */
  ::setenv("NDB_RDMA_POOL_MODE", "shared", /*overwrite=*/1);
  ::setenv("NDB_RDMA_HUGEPAGES", "best_effort", /*overwrite=*/1);

  /* Pool reuse: acquire / release / re-acquire of the same size must
   * hand back the LIFO chunk. acquire also has to memset(0) the chunk,
   * even on reuse, so a sentinel from a prior use never leaks across
   * the boundary. */
  constexpr size_t TEST_SIZE_A = 64u * 1024u;
  rdma_buffer_meta meta1 = {};
  void *p1 = rdma_buffer_acquire(TEST_SIZE_A, &meta1);
  OK(p1 != nullptr);
  OK(meta1.was_pooled);
  /* Freshly acquired chunk is zeroed by the provider. */
  OK(((unsigned char *)p1)[0] == 0u);
  /* Stamp a sentinel that will be wiped on reuse. */
  ((unsigned char *)p1)[0] = 0xABu;
  rdma_buffer_release(p1, TEST_SIZE_A, &meta1);
  /* Meta is cleared by release(). */
  OK(!meta1.was_pooled);
  OK(meta1.mapped_bytes == 0u);

  rdma_buffer_meta meta2 = {};
  void *p2 = rdma_buffer_acquire(TEST_SIZE_A, &meta2);
  OK(p2 != nullptr);
  OK(meta2.was_pooled);
  OK(p2 == p1);
  OK(((unsigned char *)p2)[0] == 0u);
  rdma_buffer_release(p2, TEST_SIZE_A, &meta2);

  /* Different sizes use different free lists; the second acquire must
   * NOT hand back the chunk we just released to the TEST_SIZE_A
   * bucket. */
  constexpr size_t TEST_SIZE_B = 128u * 1024u;
  rdma_buffer_meta meta3 = {};
  void *p3 = rdma_buffer_acquire(TEST_SIZE_B, &meta3);
  OK(p3 != nullptr);
  OK(p3 != p1);
  rdma_buffer_release(p3, TEST_SIZE_B, &meta3);

  /* Zero-byte acquire is a sanity case: returns nullptr and leaves the
   * meta in its "never acquired" state. */
  rdma_buffer_meta meta4 = {};
  void *p4 = rdma_buffer_acquire(0u, &meta4);
  OK(p4 == nullptr);
  OK(!meta4.was_pooled);
  OK(!meta4.was_hugepage);
  OK(meta4.mapped_bytes == 0u);

  return 1;  /* TAP success */
}
#endif /* TEST_RDMA_TRANSPORTER */

#endif /* NDB_RDMA_TRANSPORTER_SUPPORTED */
