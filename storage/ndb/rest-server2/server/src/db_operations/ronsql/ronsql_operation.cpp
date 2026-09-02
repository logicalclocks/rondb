/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#include "ronsql_operation.hpp"
#include "src/error_strings.h"
#include "storage/ndb/src/ronsql/RonSQLPreparer.hpp"
#include "storage/ndb/plugin/ndb_sleep.h"

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_RONSQL_OP 1
#endif

#ifdef DEBUG_RONSQL_OP
#define DEB_TRACE() do { \
  printf("ronsql_operation.cpp:%d\n", __LINE__); \
  fflush(stdout); \
} while (0)
#else
#define DEB_TRACE() do { } while (0)
#endif

#include "storage/ndb/src/ronsql/RonSQLPerf.hpp"

RS_Status ronsql_op(RonSQLExecParams& params) {
  std::basic_ostream<char>& err = *params.err_stream;
  static int max_attempts = 10;
  int retry_sleep_ms = 10;
  for (int attempt = 0; attempt < max_attempts; attempt++) {
    bool is_last_attempt = attempt == max_attempts - 1;
    try {
      // Phase stats are last-attempt-wins: each attempt overwrites the
      // previous attempt's values; attempts records how many ran.
      STAT_COUNT(params.phase_stats, attempts, (Uint32)(attempt + 1));
      PERF_TS(t_total);
      STAT_TS(params.phase_stats, s_total);
      RonSQLPreparer executor(params);
      PERF_TS(t_prepare_end);
      PERF_LOG("prepare (constructor)", t_total, t_prepare_end);
      STAT_TS(params.phase_stats, s_prepare_end);
      STAT_SET(params.phase_stats, prepare_us, s_total, s_prepare_end);

      DEB_TRACE();
      executor.execute();
      PERF_TS(t_exec_end);
      PERF_LOG("execute", t_prepare_end, t_exec_end);
      PERF_LOG("total (ronsql_op)", t_total, t_exec_end);
      STAT_TS(params.phase_stats, s_exec_end);
      STAT_SET(params.phase_stats, execute_us, s_prepare_end, s_exec_end);

      DEB_TRACE();
      return RS_OK;
    }
    catch (RonSQLRateLimitError& e) {
      /*
       * RONDB-978: the caller is over its USER rate limit. Report it with the
       * same HTTP code the pk-read and scan endpoints use for the identical
       * kernel rejection (429 via __RONDB_ERROR_CODE_HTTP_CODE) instead of a
       * 500, and do not retry - the client-side backoff window outlives our
       * retry budget, so a retry would only add load to an overflowing bucket.
       */
      DEB_TRACE();
      err << "Caught RonSQLRateLimitError: " << e.what() << "\n";
      return __RS_ERROR(__RONDB_ERROR_CODE_HTTP_CODE(e.get_ndb_error_code()),
                        -1,
                        -1,
                        e.get_ndb_error_code(),
                        -1,
                        std::string(rdrsErrorMessage(ERROR_RONSQL_RATE_LIMIT)),
                        __LINE__,
                        __MYFILENAME__);
    }
    catch (RonSQLRetryableError& e) {
      DEB_TRACE();
      if (is_last_attempt) {
        err << "Caught RonSQLRetryableError after " << max_attempts
            << " attempts: " << e.what() << ".\n";
        return RS_SERVER_ERROR(
            std::string(rdrsErrorMessage(ERROR_RONSQL_TEMPORARY)) +
            " Detail: " + e.what());
      }
      ndb_retry_sleep(retry_sleep_ms);
      retry_sleep_ms = std::min(retry_sleep_ms * 2, 1000);
    }
    catch (RonSQLPermanentError& e) {
      err << "Caught exception: " << e.what() << "\n";
      return RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_RONSQL_PERMANENT)) +
          " Detail: " + e.what());
    }
    catch (...) {
      // This should never happen
      abort();
    }
  }
  // Should be unreachable
  DEB_TRACE();
  abort();
}
