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

#include "buffer_manager.hpp"
#include "buffer_sizes.hpp"

#include <util/require.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>

#include <sys/mman.h>
#include <unistd.h>

/* Trivially constructed; main() calls init_from_config() after the
 * configuration is loaded, tests call init() with explicit inputs. */
RS_BufferArrayManager rsBufferArrayManager;

/* mmap rather than new: the kernel gives the pages back on munmap, so the
 * idle shrink lowers resident memory regardless of glibc's heap trimming
 * heuristics. The pages are untouched until a request writes them. */
char *RS_BufferPool::map_buffer(size_t bytes) {
  void *mem = mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mem == MAP_FAILED) {
    return nullptr;
  }
  return static_cast<char *>(mem);
}

void RS_BufferPool::unmap_buffer(char *buffer, size_t bytes) {
  munmap(buffer, bytes);
}

RS_BufferPool::~RS_BufferPool() {
  shutdown();
}

void RS_BufferPool::init(const char *name, Uint32 buffer_bytes,
                         Uint32 preallocate, Uint32 max_buffers,
                         Uint32 shrink_after_s) {
  require(m_mutex == nullptr);
  require(buffer_bytes >= ADDRESS_SIZE && buffer_bytes % ADDRESS_SIZE == 0);
  require(max_buffers >= preallocate);
  m_name = name;
  m_bytes = buffer_bytes;
  m_prealloc = preallocate;
  m_max = max_buffers;
  m_shrink_after_s = shrink_after_s;
  m_last_demand_s = RS_BufferArrayManager::now_seconds();
  m_stats = BufferPoolStats{};
  m_mutex = NdbMutex_Create();
  m_free.reserve(preallocate);
  for (Uint32 i = 0; i < preallocate; i++) {
    char *mem = map_buffer(buffer_bytes);
    if (mem == nullptr) {
      fprintf(stderr, "Failed to map %s buffer %u of %u (%u bytes)\n", name,
              i + 1, preallocate, buffer_bytes);
      abort();
    }
    m_free.push_back(RS_Buffer{mem, buffer_bytes});
    m_stats.allocated++;
    m_stats.total_allocations++;
  }
}

void RS_BufferPool::shutdown() {
  if (m_mutex == nullptr) {
    return;
  }
  NdbMutex_Lock(m_mutex);
  for (auto &buffer : m_free) {
    unmap_buffer(buffer.buffer, buffer.size);
  }
  m_stats.allocated -= static_cast<Int64>(m_free.size());
  m_free.clear();
  NdbMutex_Unlock(m_mutex);
  NdbMutex_Destroy(m_mutex);
  m_mutex = nullptr;
}

RS_Buffer RS_BufferPool::get() {
  require(m_mutex != nullptr);
  RS_Buffer buffer{nullptr, 0};
  NdbMutex_Lock(m_mutex);
  if (!m_free.empty()) {
    buffer = m_free.back();
    m_free.pop_back();
  } else if (m_stats.allocated < static_cast<Int64>(m_max)) {
    char *mem = map_buffer(m_bytes);
    if (mem != nullptr) {
      buffer = RS_Buffer{mem, m_bytes};
      m_stats.allocated++;
      m_stats.total_allocations++;
    }
  }
  if (buffer.buffer == nullptr) {
    m_stats.rejected++;
    NdbMutex_Unlock(m_mutex);
    return buffer;
  }
  m_stats.in_use++;
  if (m_stats.in_use > m_stats.peak_in_use) {
    m_stats.peak_in_use = m_stats.in_use;
  }
  /* More buffers out than were preallocated means the pool needs the ones
   * it grew by; remember when, so shrink() waits for a quiet period
   * measured from the last such moment. */
  if (m_stats.in_use > static_cast<Int64>(m_prealloc)) {
    m_last_demand_s = RS_BufferArrayManager::now_seconds();
  }
  NdbMutex_Unlock(m_mutex);
  return buffer;
}

void RS_BufferPool::put(RS_Buffer buffer) {
  require(m_mutex != nullptr);
  require(buffer.buffer != nullptr);
  /* Restore the pool's own size, so a returned buffer always matches what
   * get() hands out. */
  buffer.size = m_bytes;
  NdbMutex_Lock(m_mutex);
  m_free.push_back(buffer);
  m_stats.in_use--;
  NdbMutex_Unlock(m_mutex);
}

Uint32 RS_BufferPool::shrink(Int64 now_s) {
  if (m_mutex == nullptr || m_shrink_after_s == 0) {
    return 0;
  }
  std::vector<RS_Buffer> surplus;
  NdbMutex_Lock(m_mutex);
  if (m_free.size() > m_prealloc &&
      now_s - m_last_demand_s >= static_cast<Int64>(m_shrink_after_s)) {
    surplus.assign(m_free.begin() + m_prealloc, m_free.end());
    m_free.resize(m_prealloc);
    m_stats.allocated -= static_cast<Int64>(surplus.size());
    m_stats.shrunk += static_cast<Int64>(surplus.size());
  }
  NdbMutex_Unlock(m_mutex);
  /* munmap outside the lock: request threads must not wait on it. */
  for (auto &buffer : surplus) {
    unmap_buffer(buffer.buffer, buffer.size);
  }
  return static_cast<Uint32>(surplus.size());
}

void RS_BufferPool::note_retry() {
  require(m_mutex != nullptr);
  NdbMutex_Lock(m_mutex);
  m_stats.retries++;
  NdbMutex_Unlock(m_mutex);
}

void RS_BufferPool::get_stats(BufferPoolStats &out) {
  if (m_mutex == nullptr) {
    out = BufferPoolStats{};
    return;
  }
  NdbMutex_Lock(m_mutex);
  m_stats.free = static_cast<Int64>(m_free.size());
  out = m_stats;
  NdbMutex_Unlock(m_mutex);
}

static Int64 g_clock_offset_for_tests = 0;

void RS_BufferArrayManager::set_clock_offset_for_tests(Int64 seconds) {
  g_clock_offset_for_tests = seconds;
}

Int64 RS_BufferArrayManager::now_seconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::steady_clock::now().time_since_epoch())
             .count() +
         g_clock_offset_for_tests;
}

Uint64 RS_BufferArrayManager::physical_memory_bytes() {
  const long pages = sysconf(_SC_PHYS_PAGES);
  const long page_size = sysconf(_SC_PAGESIZE);
  if (pages <= 0 || page_size <= 0) {
    return 0;
  }
  return static_cast<Uint64>(pages) * static_cast<Uint64>(page_size);
}

/* A cgroup limit file holds either a number of bytes or "max" (v2); v1
 * reports "unlimited" as a value near 2^63. Anything that is not a sane
 * limit falls through to the next source. */
static bool read_limit_file(const char *path, Uint64 &out) {
  std::ifstream in(path);
  if (!in.is_open()) {
    return false;
  }
  std::string token;
  in >> token;
  if (token.empty() || token == "max") {
    return false;
  }
  char *end = nullptr;
  const unsigned long long value = strtoull(token.c_str(), &end, 10);
  if (end == token.c_str() || value == 0 ||
      value >= (1ULL << 62)) {  // v1's "no limit" sentinel
    return false;
  }
  out = value;
  return true;
}

Uint64 RS_BufferArrayManager::detect_memory_limit(const char *cgroup_v2_path,
                                                  const char *cgroup_v1_path) {
  Uint64 limit = 0;
  if (read_limit_file(cgroup_v2_path, limit)) {
    return limit;
  }
  if (read_limit_file(cgroup_v1_path, limit)) {
    return limit;
  }
  return physical_memory_bytes();
}

Uint32 RS_BufferArrayManager::derive_max_buffers(Uint64 limit_bytes,
                                                 Uint32 share_percent,
                                                 Uint64 pair_bytes,
                                                 Uint32 override_value,
                                                 Uint32 preallocate) {
  if (override_value != 0) {
    return override_value;
  }
  const Uint64 floor = static_cast<Uint64>(preallocate) + 1;
  if (pair_bytes == 0) {
    return static_cast<Uint32>(floor);
  }
  /* limit * share / 100 without losing the remainder of the division,
   * and without overflowing for limits below 2^57. */
  const Uint64 budget =
      limit_bytes <= (0xFFFFFFFFFFFFFFFFULL / 100)
          ? limit_bytes * share_percent / 100
          : (limit_bytes / 100) * share_percent;
  Uint64 derived = budget / pair_bytes;
  if (derived < floor) {
    derived = floor;
  }
  if (derived > 0xFFFFFFFFULL) {
    derived = 0xFFFFFFFFULL;
  }
  return static_cast<Uint32>(derived);
}

void RS_BufferArrayManager::init(Uint32 req_bytes, Uint32 resp_bytes,
                                 Uint32 preallocate,
                                 Uint32 max_buffers_override,
                                 Uint32 share_percent, Uint32 shrink_after_s,
                                 Uint64 memory_limit_bytes) {
  require(!m_initialized);
  m_memory_limit = memory_limit_bytes;
  m_max = derive_max_buffers(memory_limit_bytes, share_percent,
                             static_cast<Uint64>(req_bytes) + resp_bytes,
                             max_buffers_override, preallocate);
  m_req.init("request", req_bytes, preallocate, m_max, shrink_after_s);
  m_resp.init("response", resp_bytes, preallocate, m_max, shrink_after_s);
  m_initialized = true;
}

void RS_BufferArrayManager::init_from_config() {
  const Internal &cfg = globalConfigs.internal;
  /* The sizes are computed from the NDB limits; there is no setting for
   * them. */
  init(DEFAULT_REQ_BUFFER, DEFAULT_RESP_BUFFER, cfg.preAllocatedBuffers,
       cfg.maxBuffers, cfg.bufferPoolSharePercent, cfg.bufferShrinkAfterS,
       detect_memory_limit());
}

void RS_BufferArrayManager::shutdown() {
  m_req.shutdown();
  m_resp.shutdown();
  m_initialized = false;
}

Uint32 RS_BufferArrayManager::shrink() {
  const Int64 now = now_seconds();
  return m_req.shrink(now) + m_resp.shrink(now);
}
