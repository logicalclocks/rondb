/*
 * Copyright (c) 2023, 2026, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_MANAGER_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_MANAGER_HPP_

#include "rdrs_dal.h"
#include "config_structs.hpp"
#include "rdrs_const.h"
#include <NdbMutex.h>

#include <cstdint>
#include <cstring>
#include <vector>

/*
 * RONDB-1144: the two pools of fixed-size buffers every pk-read style
 * request draws from. Each pool maps buffers on demand with mmap, keeps
 * returned buffers on a free list, refuses to grow past a cap, and unmaps
 * surplus free buffers after a quiet period. Nothing here is constructed
 * before main(): the global below is trivially constructed and must be
 * initialised explicitly once the configuration is loaded.
 */

/* Counters of one pool. Gauges describe the current state; the last four
 * are cumulative since init. */
struct BufferPoolStats {
  Int64 allocated;          // buffers mapped and owned by the pool
  Int64 free;               // on the free list
  Int64 in_use;             // handed out and not yet returned
  Int64 peak_in_use;        // highest in_use since init
  Int64 total_allocations;  // mmap calls
  Int64 rejected;           // get() refused: cap reached or mmap failed
  Int64 shrunk;             // buffers unmapped by shrink()
  Int64 retries;            // responses rewritten in a fresh buffer
};

class RS_BufferPool {
 public:
  RS_BufferPool() = default;
  ~RS_BufferPool();
  RS_BufferPool(const RS_BufferPool &) = delete;
  RS_BufferPool &operator=(const RS_BufferPool &) = delete;

  /* buffer_bytes: size of every buffer, a multiple of ADDRESS_SIZE.
   * preallocate: buffers mapped now and kept by shrink().
   * max_buffers: get() refuses once this many are mapped; >= preallocate.
   * shrink_after_s: idle seconds before surplus free buffers are unmapped,
   * 0 disables shrink. */
  void init(const char *name, Uint32 buffer_bytes, Uint32 preallocate,
            Uint32 max_buffers, Uint32 shrink_after_s);
  /* Unmaps the free list. Buffers still in use are leaked on purpose:
   * shutdown runs at process exit after request handling has stopped. */
  void shutdown();

  /* A buffer with size == buffer_bytes(), or buffer == nullptr and size 0
   * when the cap is reached or mmap fails (both counted as rejected). */
  RS_Buffer get();
  void put(RS_Buffer buffer);

  /* Unmap free buffers above the preallocated count if the pool has not
   * needed them for shrink_after_s seconds. Returns how many were freed. */
  Uint32 shrink(Int64 now_s);
  void note_retry();
  void get_stats(BufferPoolStats &out);

  Uint32 buffer_bytes() const { return m_bytes; }
  Uint32 max_buffers() const { return m_max; }

 private:
  static char *map_buffer(size_t bytes);
  static void unmap_buffer(char *buffer, size_t bytes);

  const char *m_name = "";
  Uint32 m_bytes = 0;
  Uint32 m_prealloc = 0;
  Uint32 m_max = 0;
  Uint32 m_shrink_after_s = 0;
  /* Time of the last get() that left more than preallocate buffers in use
   * (init time before any), i.e. the last moment the buffers the pool grew
   * by were needed. */
  Int64 m_last_demand_s = 0;
  std::vector<RS_Buffer> m_free;
  BufferPoolStats m_stats{};
  NdbMutex *m_mutex = nullptr;
};

class RS_BufferArrayManager {
 public:
  RS_BufferArrayManager() = default;
  RS_BufferArrayManager(const RS_BufferArrayManager &) = delete;
  RS_BufferArrayManager &operator=(const RS_BufferArrayManager &) = delete;

  /* Explicit initialisation with every input spelled out; tests use this.
   * max_buffers_override 0 derives the cap from memory_limit_bytes,
   * share_percent and the size of one request + response pair. */
  void init(Uint32 req_bytes, Uint32 resp_bytes, Uint32 preallocate,
            Uint32 max_buffers_override, Uint32 share_percent,
            Uint32 shrink_after_s, Uint64 memory_limit_bytes);
  /* init() from globalConfigs and the detected memory limit. */
  void init_from_config();
  void shutdown();

  RS_Buffer get_req_buffer() { return m_req.get(); }
  RS_Buffer get_resp_buffer() { return m_resp.get(); }
  void return_req_buffer(RS_Buffer buffer) { m_req.put(buffer); }
  void return_resp_buffer(RS_Buffer buffer) { m_resp.put(buffer); }

  /* Called from a periodic timer; cheap when there is nothing to free. */
  Uint32 shrink();
  void note_resp_retry() { m_resp.note_retry(); }

  void get_req_buffers_stats(BufferPoolStats &out) { m_req.get_stats(out); }
  void get_resp_buffers_stats(BufferPoolStats &out) { m_resp.get_stats(out); }

  Uint32 req_buffer_bytes() const { return m_req.buffer_bytes(); }
  Uint32 resp_buffer_bytes() const { return m_resp.buffer_bytes(); }
  Uint32 max_buffers() const { return m_max; }
  Uint64 memory_limit_bytes() const { return m_memory_limit; }

  /* The memory limit this process runs under: cgroup v2 memory.max, else
   * cgroup v1 memory.limit_in_bytes, else physical RAM. The paths are
   * parameters so tests can point them at files of their own. */
  static Uint64 detect_memory_limit(
      const char *cgroup_v2_path = "/sys/fs/cgroup/memory.max",
      const char *cgroup_v1_path =
          "/sys/fs/cgroup/memory/memory.limit_in_bytes");
  static Uint64 physical_memory_bytes();
  /* override when non-zero; otherwise share_percent of limit_bytes divided
   * by pair_bytes, never below preallocate + 1. */
  static Uint32 derive_max_buffers(Uint64 limit_bytes, Uint32 share_percent,
                                   Uint64 pair_bytes, Uint32 override_value,
                                   Uint32 preallocate);
  static Int64 now_seconds();
  /* Tests shift the clock the pools read, to place demand inside a quiet
   * period without sleeping. */
  static void set_clock_offset_for_tests(Int64 seconds);

 private:
  RS_BufferPool m_req;
  RS_BufferPool m_resp;
  bool m_initialized = false;
  Uint32 m_max = 0;
  Uint64 m_memory_limit = 0;
};

#define EN_STATUS_MSG_LEN 256
struct EN_Status {
  Uint32 retValue;
  HTTP_CODE http_code;                // 200 for successful operation
  char message[EN_STATUS_MSG_LEN]{};  // error message.
  EN_Status() : retValue(0), http_code(HTTP_CODE::SUCCESS), message("") {
  }
  explicit EN_Status(HTTP_CODE http_code) : retValue(0),
                                            http_code(http_code), message("") {
  }
  explicit EN_Status(Uint32 retValue)
      : retValue(retValue), http_code(HTTP_CODE::SUCCESS), message("") {
  }
  EN_Status(HTTP_CODE http_code, const char *message) : retValue(0),
                                                        http_code(http_code) {
    strncpy(this->message, message, EN_STATUS_MSG_LEN - 1);
    this->message[EN_STATUS_MSG_LEN - 1] = '\0';
  }
  EN_Status(HTTP_CODE http_code, Uint32 retValue)
      : retValue(retValue), http_code(http_code), message("") {
  }
  EN_Status(HTTP_CODE http_code, Uint32 retValue, const char *message)
      : retValue(retValue), http_code(http_code) {
    strncpy(this->message, message, EN_STATUS_MSG_LEN - 1);
    this->message[EN_STATUS_MSG_LEN - 1] = '\0';
  }
};

extern RS_BufferArrayManager rsBufferArrayManager;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_MANAGER_HPP_
