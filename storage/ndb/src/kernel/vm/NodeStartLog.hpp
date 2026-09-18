/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef NODE_START_LOG_HPP
#define NODE_START_LOG_HPP

#include <cstdarg>
#include <cstdio>

#include <ndb_global.h>
#include <EventLogger.hpp>
#include <NdbTick.h>
#include <NodeState.hpp>

#define JAM_FILE_ID 566

/**
 * NodeStartLog - uniform [NODE-START] logging of the data node start.
 *
 * The start of a data node is described as 16 fixed major steps, each
 * with a fixed set of sub-steps. Step numbers are the same for every
 * start type; steps that do not apply to the current start type are
 * announced in the plan line and additionally log a one-line 'skipped'
 * marker at the point where they would have executed, so the numbered
 * sequence in the log is always complete.
 *
 * Line shapes produced (all begin with the greppable [NODE-START] tag):
 *
 *   [NODE-START] plan: node restart, executing 15/16 steps
 *                (skipping: 4 redo-init)
 *   [NODE-START] step 8/16 (restore) started: 1842 fragments to restore
 *   [NODE-START] step 8/16 sub-step 2/2 (restore: restore fragments)
 *                progress LDM(2): 231/460 fragments, elapsed=45s
 *   [NODE-START] step 8/16 (restore) completed, elapsed=312s
 *   [NODE-START] step 4/16 (redo-init) skipped (node restart)
 *   [NODE-START] step 13/16 (wait-lcp) waiting: LCP 1234 must complete,
 *                elapsed=180s
 *   [NODE-START] waiting: start phase 6 barrier, master node 2 grants it
 *                once every node has completed start phase 5, elapsed=30s
 *
 * All helpers print the line to the node log (g_eventLogger, always on)
 * and return the formatted line so that the few call sites that mirror
 * a line to the cluster log can pass it to infoEvent("%s", buf). The
 * ATTRIBUTE_FORMAT annotations below only give compile-time checking of
 * the detail format against its arguments (the same annotation
 * Logger::info and SimulatedBlock::infoEvent carry); nothing here
 * prints with printf, the pieces are snprintf'ed into the caller's
 * buffer and printed once. Progress and waiting lines are throttled by
 * NodeStartLogTimer using the NodeStartLogReportFrequency configuration
 * parameter (seconds, 0 = boundary lines only).
 *
 * The cluster log (management server) receives, through infoEvent at
 * the call sites, only three things: the plan line, the final
 * "[NODE-START] completed: <start type> finished" line, and, once a
 * step has run for 60 s, the report tick's line for it, repeated every
 * 10 minutes: a waiting line, or the progress line of step 7's schema
 * processing, the one tick that reports progress there. The per-step
 * started, completed and skipped lines are node-log only; the cluster
 * log keeps its existing "Start phase N completed" lines as the coarse
 * markers of a healthy start, and a start that stalls announces itself
 * there after 60 s. The failed line is node-log only too: in the kernel
 * ErrorReporter prints it from a process that is already going down,
 * and the angel's exits print it without sending an event. A failed
 * start therefore has no closing line in the cluster log: its narrative
 * stops at the plan line or at an escalated waiting line, or has none
 * when the node fails before admission; when the angel reports the
 * exit, the management server's node-down lines follow (Forced node
 * shutdown completed, Node N Disconnected).
 *
 * The elapsed field of a step line counts from the start of the step (on
 * a per-LDM line from that LDM's start of the step); on a sub-step line
 * (started, progress, waiting, completed) it counts from the start of that
 * sub-step, so a sub-step completion gives the sub-step's own duration.
 * Assist lines count from the start of the assisted sub-step.
 */
struct NodeStartLog {
  static constexpr Uint32 TOTAL_STEPS = 16;
  static constexpr Uint32 BUF_SIZE = 512;
  /**
   * NodeStartLogReportFrequency default, for reports emitted before
   * the configuration is known (the angel's node id allocation).
   */
  static constexpr Uint32 DEFAULT_REPORT_FREQUENCY_SEC = 15;

  enum Step {
    NSL_INIT = 1,           /* process init, memory, READ_CONFIG      */
    NSL_JOIN = 2,           /* QMGR node inclusion protocol           */
    NSL_ADMISSION = 3,      /* CNTR_START_REQ to NDBCNTR master       */
    NSL_REDO_INIT = 4,      /* create + init REDO log files           */
    NSL_START_PERM = 5,     /* DICT lock + START_PERMREQ              */
    NSL_REDO_PREPARE = 6,   /* read REDO log page headers (head)      */
    NSL_METADATA = 7,       /* schema + distribution synchronisation  */
    NSL_RESTORE = 8,        /* restore fragments from LCP             */
    NSL_UNDO_DD = 9,        /* disk data UNDO log + extent scan       */
    NSL_REDO_EXEC = 10,     /* execute REDO log                       */
    NSL_INDEX_REBUILD = 11, /* rebuild ordered indexes                */
    NSL_SYNCHRONIZE = 12,   /* copy fragments from live nodes         */
    NSL_WAIT_LCP = 13,      /* wait for LCP to make node durable      */
    NSL_ACTIVATE = 14,      /* GCP start, activate indexes, FKs       */
    NSL_HANDOVER = 15,      /* SUMA subscription handover             */
    NSL_BARRIER = 16        /* restart barrier, start phase 110       */
  };

  static const char *stepName(Uint32 step) {
    static const char *names[TOTAL_STEPS] = {
        "init",         "join",        "admission",   "redo-init",
        "start-perm",   "redo-prepare", "metadata",   "restore",
        "undo-dd",      "redo-exec",   "index-rebuild", "synchronize",
        "wait-lcp",     "activate",    "handover",    "barrier"};
    if (step < 1 || step > TOTAL_STEPS) return "?";
    return names[step - 1];
  }

  static const char *startTypeName(Uint32 startType) {
    switch (startType) {
      case NodeState::ST_INITIAL_START:
        return "initial start";
      case NodeState::ST_SYSTEM_RESTART:
        return "system restart";
      case NodeState::ST_NODE_RESTART:
        return "node restart";
      case NodeState::ST_INITIAL_NODE_RESTART:
        return "initial node restart";
      case NodeState::ST_SYSTEM_RESTART_NOT_RESTORABLE:
        return "system restart (take-over)";
      default:
        return "start";
    }
  }

  /**
   * Plain-language gloss for the plan line: the start type names are
   * easily confused, so the one line that announces the scenario also
   * says what actually happens.
   */
  static const char *startTypeGloss(Uint32 startType) {
    switch (startType) {
      case NodeState::ST_INITIAL_START:
        return "first start, no data anywhere";
      case NodeState::ST_SYSTEM_RESTART:
        return "all nodes recover together from disk";
      case NodeState::ST_NODE_RESTART:
        return "this node rejoins, keeping its data";
      case NodeState::ST_INITIAL_NODE_RESTART:
        return "this node rejoins empty, data copied from live nodes";
      default:
        return "unknown start type";
    }
  }

  static bool stepApplies(Uint32 step, Uint32 startType) {
    /**
     * Columns: initial start, system restart, node restart, initial
     * node restart. These are the only start types NDBCNTR grants in
     * CNTR_START_CONF: a node that cannot restore on its own
     * (ST_SYSTEM_RESTART_NOT_RESTORABLE) is started later by the
     * master as a node restart. Step 12 applies to a system restart
     * only when some node needs take-over; the runtime emits 'skipped'
     * there otherwise. Step 8 in an initial node restart copies the
     * fragments from the live nodes instead of restoring them from an
     * LCP; it is the bulk data movement of that start type.
     */
    static const Uint8 tab[TOTAL_STEPS][4] = {
        /* 1 init          */ {1, 1, 1, 1},
        /* 2 join          */ {1, 1, 1, 1},
        /* 3 admission     */ {1, 1, 1, 1},
        /* 4 redo-init     */ {1, 0, 0, 1},
        /* 5 start-perm    */ {0, 0, 1, 1},
        /* 6 redo-prepare  */ {0, 1, 1, 0},
        /* 7 metadata      */ {0, 1, 1, 1},
        /* 8 restore       */ {0, 1, 1, 1},
        /* 9 undo-dd       */ {0, 1, 1, 0},
        /*10 redo-exec     */ {0, 1, 1, 0},
        /*11 index-rebuild */ {0, 1, 1, 1},
        /*12 synchronize   */ {0, 1, 1, 1},
        /*13 wait-lcp      */ {1, 1, 1, 1},
        /*14 activate      */ {1, 1, 1, 1},
        /*15 handover      */ {0, 0, 1, 1},
        /*16 barrier       */ {0, 0, 1, 1},
    };
    if (step < 1 || step > TOTAL_STEPS || startType > 3) return true;
    return tab[step - 1][startType] != 0;
  }

  /**
   * The kernel time queues abort on longer delays
   * (NDBD_EXIT_TIME_QUEUE_DELAY).
   */
  static constexpr Uint32 MAX_TICK_DELAY_MILLIS = 32000;

  /**
   * Delay for arming the periodic report tick: one report period,
   * capped at the time queue maximum. When the period exceeds the
   * cap, the tick handlers re-arm with NodeStartLogTimer::
   * next_tick_delay_ms() to keep the report cadence at the
   * configured frequency.
   */
  static Uint32 tickDelayMillis(Uint32 freq_sec) {
    return (freq_sec > MAX_TICK_DELAY_MILLIS / 1000) ? MAX_TICK_DELAY_MILLIS
                                                     : freq_sec * 1000;
  }

  static Uint32 numStepsFor(Uint32 startType) {
    Uint32 count = 0;
    for (Uint32 step = 1; step <= TOTAL_STEPS; step++) {
      if (stepApplies(step, startType)) count++;
    }
    return count;
  }

  /**
   * A sub-step is a phase of the step that runs on its own, one after
   * the other, so that a sub-step line names what the node is doing
   * right now. Work that is interleaved with another phase is not a
   * sub-step of its own: the REDO log files are created and
   * initialized one file at a time (step 4), the REDO execution
   * limits are computed at the start of each of the four execution
   * rounds (step 10), and the initial-node-restart LCP invalidation
   * runs on the live nodes while the start permission handshake is
   * pending (step 5, reported as assist lines under sub-step 2).
   */
  static Uint32 subTotal(Uint32 step, Uint32 startType) {
    switch (step) {
      case NSL_INIT:
        return 4;
      case NSL_JOIN:
        return 3;
      case NSL_ADMISSION:
        return 1;
      case NSL_REDO_INIT:
        return 1;
      case NSL_START_PERM:
        return 2;
      case NSL_REDO_PREPARE:
        return 1;
      case NSL_METADATA:
        /* System restart: sysfile, then one interleaved phase in which
           DICT restores the schema while each table's distribution is
           read and sent to all nodes. Node restarts have four. */
        return (startType == NodeState::ST_SYSTEM_RESTART ||
                startType == NodeState::ST_SYSTEM_RESTART_NOT_RESTORABLE)
                   ? 2
                   : 4;
      case NSL_RESTORE:
        return 2;
      case NSL_UNDO_DD:
        return 4;
      case NSL_REDO_EXEC:
        return 2;
      case NSL_INDEX_REBUILD:
        return 1;
      case NSL_SYNCHRONIZE:
        return 3;
      case NSL_WAIT_LCP:
        return 1;
      case NSL_ACTIVATE:
        /**
         * Only work that runs inside this step's window is a sub-step:
         * system tables are created before step 13 and index
         * activation happens inside the step 7 window, so neither is
         * listed here. GCP is only started on the master in an
         * initial/system start; node restarts only enable the FKs.
         */
        return (startType == NodeState::ST_NODE_RESTART ||
                startType == NodeState::ST_INITIAL_NODE_RESTART)
                   ? 1
                   : 2;
      case NSL_HANDOVER:
        return 2;
      case NSL_BARRIER:
        return 1;
    }
    return 1;
  }

  static const char *subName(Uint32 step, Uint32 sub, Uint32 startType) {
    static const char *unknown = "?";
    switch (step) {
      case NSL_INIT: {
        /* Sub-step 2 is the global memory pool allocation. It maps and
           touches the pools the process needs to start; with LateAlloc
           (the default) the rest is mapped and touched as the blocks
           read their configuration in sub-step 4, which is where the
           bulk of the touch progress then appears. */
        static const char *n[4] = {"fetch configuration",
                                   "allocate memory pools",
                                   "start transporters and service threads",
                                   "read configuration into blocks"};
        return (sub >= 1 && sub <= 4) ? n[sub - 1] : unknown;
      }
      case NSL_JOIN: {
        /* The node joins the heartbeat protocol as the inclusion
           protocol commits; the step's completed line says so. */
        static const char *n[3] = {"check local sysfile",
                                   "president discovery/election",
                                   "node inclusion protocol"};
        return (sub >= 1 && sub <= 3) ? n[sub - 1] : unknown;
      }
      case NSL_ADMISSION: {
        /* The granted start type is the detail of the completed line. */
        return (sub == 1) ? "request start permission" : unknown;
      }
      case NSL_REDO_INIT: {
        return (sub == 1) ? "create and initialize REDO log files" : unknown;
      }
      case NSL_START_PERM: {
        /* In an initial node restart the live nodes invalidate this
           node's old LCPs before the master grants sub-step 2; they
           report that as assist lines under sub-step 2. */
        static const char *n[2] = {"acquire DICT lock",
                                   "start permission handshake"};
        return (sub >= 1 && sub <= 2) ? n[sub - 1] : unknown;
      }
      case NSL_REDO_PREPARE: {
        /* Reading the page headers is what locates the head; the tail
           is found by the execution limits of step 10. */
        return (sub == 1) ? "read REDO log page headers" : unknown;
      }
      case NSL_METADATA: {
        if (startType == NodeState::ST_SYSTEM_RESTART ||
            startType == NodeState::ST_SYSTEM_RESTART_NOT_RESTORABLE) {
          static const char *n[2] = {"synchronize sysfile",
                                     "restore the schema and distribute the"
                                     " tables"};
          return (sub >= 1 && sub <= 2) ? n[sub - 1] : unknown;
        }
        static const char *n[4] = {"pause LCP",
                                   "copy distribution information",
                                   "copy dictionary information",
                                   "include node in protocols"};
        return (sub >= 1 && sub <= 4) ? n[sub - 1] : unknown;
      }
      case NSL_RESTORE: {
        if (startType == NodeState::ST_INITIAL_NODE_RESTART) {
          static const char *n[2] = {"distribute fragment copy requests",
                                     "copy fragments from live nodes"};
          return (sub >= 1 && sub <= 2) ? n[sub - 1] : unknown;
        }
        static const char *n[2] = {"distribute fragment restore requests",
                                   "restore fragments from LCP"};
        return (sub >= 1 && sub <= 2) ? n[sub - 1] : unknown;
      }
      case NSL_UNDO_DD: {
        static const char *n[4] = {"find UNDO log head", "apply UNDO log",
                                   "flush page cache",
                                   "scan tablespace extents"};
        return (sub >= 1 && sub <= 4) ? n[sub - 1] : unknown;
      }
      case NSL_REDO_EXEC: {
        /* Each of the four execution rounds first computes its limits. */
        static const char *n[2] = {"execute REDO log",
                                   "relocate REDO head and invalidate tail"};
        return (sub >= 1 && sub <= 2) ? n[sub - 1] : unknown;
      }
      case NSL_INDEX_REBUILD: {
        return (sub == 1) ? "rebuild ordered indexes" : unknown;
      }
      case NSL_SYNCHRONIZE: {
        static const char *n[3] = {"start take-over threads",
                                   "copy fragments from live nodes",
                                   "enable REDO logging"};
        return (sub >= 1 && sub <= 3) ? n[sub - 1] : unknown;
      }
      case NSL_WAIT_LCP: {
        return (sub == 1) ? "wait for LCP" : unknown;
      }
      case NSL_ACTIVATE: {
        if (startType == NodeState::ST_NODE_RESTART ||
            startType == NodeState::ST_INITIAL_NODE_RESTART) {
          return (sub == 1) ? "enable foreign keys" : unknown;
        }
        static const char *n[2] = {"start GCP", "enable foreign keys"};
        return (sub >= 1 && sub <= 2) ? n[sub - 1] : unknown;
      }
      case NSL_HANDOVER: {
        static const char *n[2] = {"wait for subscribers to connect",
                                   "take over subscription buckets"};
        return (sub >= 1 && sub <= 2) ? n[sub - 1] : unknown;
      }
      case NSL_BARRIER: {
        return (sub == 1) ? "wait at restart barrier" : unknown;
      }
    }
    return unknown;
  }

  /**
   * Format and print one uniform line. 'sub' == 0 gives a major-step
   * line, 'sub' > 0 a sub-step line. 'elapsed_sec' < 0 omits the
   * elapsed field. 'detail_fmt' may be NULL when the verb stands alone.
   * Returns buf so the caller can mirror the line to the cluster log
   * with infoEvent("%s", buf).
   */
  static const char *line(char *buf, Uint32 len, Uint32 step, Uint32 sub,
                          Uint32 startType, const char *verb,
                          Int64 elapsed_sec, const char *detail_fmt = nullptr,
                          ...) ATTRIBUTE_FORMAT(printf, 8, 9);

  /**
   * Same as line(), for work a node performs to assist ANOTHER node's
   * start: the line carries an "[assist node N]" marker after the
   * prefix, so a reader of the assisting node's log can tell these
   * lines from the node's own start. Own-start lines have "step"
   * directly after the prefix, so "[NODE-START] step" greps only
   * those, and "assist node N" greps the peer-side view of node N's
   * start.
   */
  static const char *assist_line(char *buf, Uint32 len, Uint32 assist_node,
                                 Uint32 step, Uint32 sub, Uint32 startType,
                                 const char *verb, Int64 elapsed_sec,
                                 const char *detail_fmt = nullptr, ...)
      ATTRIBUTE_FORMAT(printf, 9, 10);

  /**
   * The plan line, printed once the start type is known
   * (CNTR_START_CONF). Also names the steps that will be skipped.
   */
  static const char *plan(char *buf, Uint32 len, Uint32 startType) {
    Uint32 pos = clamp((Uint32)snprintf(buf, len,
                                        "[NODE-START] plan: %s (%s),"
                                        " executing %u/%u steps",
                                        startTypeName(startType),
                                        startTypeGloss(startType),
                                        numStepsFor(startType), TOTAL_STEPS),
                       len);
    bool first = true;
    for (Uint32 step = 1; step <= TOTAL_STEPS; step++) {
      if (!stepApplies(step, startType)) {
        pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos, "%s%u %s",
                                           first ? " (skipping: " : ", ",
                                           step, stepName(step)),
                    len);
        first = false;
      }
    }
    if (!first) {
      pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos, ")"), len);
    }
    if (startType == NodeState::ST_SYSTEM_RESTART) {
      /* Whether synchronize runs is only known once the master has
         decided about take-overs, after the plan line is printed. */
      pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos,
                                         "; 12 synchronize only if a"
                                         " take-over is needed"),
                  len);
    }
    g_eventLogger->info("%s", buf);
    return buf;
  }

  /**
   * The skipped marker, printed at the point where the step would have
   * executed, so the numbered sequence in the log stays complete.
   */
  static const char *skipped(char *buf, Uint32 len, Uint32 step,
                             Uint32 startType) {
    snprintf(buf, len, "[NODE-START] step %u/%u (%s) skipped (%s)", step,
             TOTAL_STEPS, stepName(step), startTypeName(startType));
    g_eventLogger->info("%s", buf);
    return buf;
  }

  /**
   * The failure line, printed when the node goes down before it has
   * started (ErrorReporter) or the angel gives up before the node
   * process runs. The last 'started' line above it in the node log
   * names the step that was running. Node log only, see the cluster
   * log policy in the header comment.
   */
  static const char *failed(char *buf, Uint32 len, const char *detail_fmt,
                            ...) ATTRIBUTE_FORMAT(printf, 3, 4);

  /**
   * A waiting line without a step, for the cluster-wide wait points of
   * an initial or system restart where NDBCNTR parks the node across
   * several steps (start phase barriers, the phase 4 wait point).
   * Shape: "[NODE-START] waiting: <detail>, elapsed=Ns".
   */
  static const char *wait_line(char *buf, Uint32 len, Int64 elapsed_sec,
                               const char *detail_fmt, ...)
      ATTRIBUTE_FORMAT(printf, 4, 5);

  /**
   * Append ", R <unit>/s" and ", ~Ns left" to a progress detail from
   * the work done so far, the total and the seconds spent in the step.
   * Nothing is appended before the first second or before any work is
   * done; the estimate is omitted once the work is done or while the
   * rate rounds to zero. Returns the number of characters appended.
   */
  static Uint32 appendRateEta(char *buf, Uint32 len, Uint64 done,
                              Uint64 total, Int64 elapsed_sec,
                              const char *unit) {
    if (elapsed_sec <= 0 || done == 0 || len == 0) return 0;
    const Uint64 rate = done / (Uint64)elapsed_sec;
    Uint32 pos = clamp((Uint32)snprintf(buf, len, ", %llu %s/s",
                                        (unsigned long long)rate, unit),
                       len);
    if (rate > 0 && done < total) {
      pos = clamp(pos + (Uint32)snprintf(
                            buf + pos, len - pos, ", ~%llus left",
                            (unsigned long long)((total - done) / rate)),
                  len);
    }
    return pos;
  }

 private:
  static Uint32 clamp(Uint32 pos, Uint32 len) {
    return (pos >= len) ? (len - 1) : pos;
  }

  /* Shared body of line()/assist_line(); assist_node 0 = own start. */
  static const char *vline(char *buf, Uint32 len, Uint32 assist_node,
                           Uint32 step, Uint32 sub, Uint32 startType,
                           const char *verb, Int64 elapsed_sec,
                           const char *detail_fmt, va_list ap)
      ATTRIBUTE_FORMAT(printf, 9, 0);
};

inline const char *NodeStartLog::line(char *buf, Uint32 len, Uint32 step,
                                      Uint32 sub, Uint32 startType,
                                      const char *verb, Int64 elapsed_sec,
                                      const char *detail_fmt, ...) {
  va_list ap;
  va_start(ap, detail_fmt);
  const char *ret = vline(buf, len, 0, step, sub, startType, verb,
                          elapsed_sec, detail_fmt, ap);
  va_end(ap);
  return ret;
}

inline const char *NodeStartLog::assist_line(char *buf, Uint32 len,
                                             Uint32 assist_node, Uint32 step,
                                             Uint32 sub, Uint32 startType,
                                             const char *verb,
                                             Int64 elapsed_sec,
                                             const char *detail_fmt, ...) {
  va_list ap;
  va_start(ap, detail_fmt);
  const char *ret = vline(buf, len, assist_node, step, sub, startType, verb,
                          elapsed_sec, detail_fmt, ap);
  va_end(ap);
  return ret;
}

inline const char *NodeStartLog::vline(char *buf, Uint32 len,
                                       Uint32 assist_node, Uint32 step,
                                       Uint32 sub, Uint32 startType,
                                       const char *verb, Int64 elapsed_sec,
                                       const char *detail_fmt, va_list ap) {
  Uint32 pos = clamp((Uint32)snprintf(buf, len, "[NODE-START] "), len);
  if (assist_node != 0) {
    pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos,
                                       "[assist node %u] ", assist_node),
                len);
  }
  pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos, "step %u/%u ",
                                     step, TOTAL_STEPS),
              len);
  if (sub > 0) {
    pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos,
                                       "sub-step %u/%u (%s: %s) ", sub,
                                       subTotal(step, startType),
                                       stepName(step),
                                       subName(step, sub, startType)),
                len);
  } else {
    pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos, "(%s) ",
                                       stepName(step)),
                len);
  }
  pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos, "%s", verb), len);
  if (detail_fmt != nullptr) {
    pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos, ": "), len);
    pos = clamp(pos + (Uint32)vsnprintf(buf + pos, len - pos, detail_fmt, ap),
                len);
  }
  if (elapsed_sec >= 0) {
    pos = clamp(pos + (Uint32)snprintf(buf + pos, len - pos,
                                       ", elapsed=%llds",
                                       (long long)elapsed_sec),
                len);
  }
  g_eventLogger->info("%s", buf);
  return buf;
}

inline const char *NodeStartLog::wait_line(char *buf, Uint32 len,
                                           Int64 elapsed_sec,
                                           const char *detail_fmt, ...) {
  Uint32 pos = clamp((Uint32)snprintf(buf, len, "[NODE-START] waiting: "), len);
  va_list ap;
  va_start(ap, detail_fmt);
  pos = clamp(pos + (Uint32)vsnprintf(buf + pos, len - pos, detail_fmt, ap),
              len);
  va_end(ap);
  if (elapsed_sec >= 0) {
    (void)clamp(pos + (Uint32)snprintf(buf + pos, len - pos,
                                       ", elapsed=%llds",
                                       (long long)elapsed_sec),
                len);
  }
  g_eventLogger->info("%s", buf);
  return buf;
}

inline const char *NodeStartLog::failed(char *buf, Uint32 len,
                                        const char *detail_fmt, ...) {
  Uint32 pos = clamp((Uint32)snprintf(buf, len, "[NODE-START] failed: "), len);
  va_list ap;
  va_start(ap, detail_fmt);
  (void)clamp(pos + (Uint32)vsnprintf(buf + pos, len - pos, detail_fmt, ap),
              len);
  va_end(ap);
  g_eventLogger->info("%s", buf);
  return buf;
}

/**
 * Per-step timer driving the periodic progress/waiting reports.
 *
 * The reports are emitted from a periodic tick (not from the work loop)
 * so that a step that stops making progress keeps printing the same
 * counters with a growing elapsed time - a stall is directly visible
 * in the log. report_due() throttles the node-log lines to the
 * NodeStartLogReportFrequency configuration parameter. escalate_due()
 * additionally allows a waiting line to be mirrored to the cluster log:
 * first after ESCALATE_FIRST_SECS in one step, then every
 * ESCALATE_REPEAT_SECS. It is evaluated on the report tick, so with a
 * frequency above ESCALATE_FIRST_SECS the first cluster-log line comes
 * at the first tick after it, and with frequency 0 (no ticks) waits are
 * not mirrored at all.
 *
 * The LGMAN/TSMAN undo-dd sub-step lines and the metadata progress of
 * a non-master in a system restart (DBDIH execCOPY_TABREQ) are the
 * exception: they poll report_due() from their work loops, so they stop
 * when the work stops; stall liveness for step 9 is provided by the
 * DBLQH tick and for step 7 of a system restart by the master's tick.
 */
class NodeStartLogTimer {
 public:
  static constexpr Uint32 ESCALATE_FIRST_SECS = 60;
  static constexpr Uint32 ESCALATE_REPEAT_SECS = 600;

  NodeStartLogTimer() : m_active(false) {}

  void start_step() {
    m_step_start = NdbTick_getCurrentTicks();
    m_last_report = m_step_start;
    m_last_escalate = m_step_start;
    m_escalated = false;
    m_active = true;
  }

  void stop_step() { m_active = false; }

  bool is_active() const { return m_active; }

  Uint64 elapsed_sec() const {
    if (!m_active) return 0;
    return NdbTick_Elapsed(m_step_start, NdbTick_getCurrentTicks()).seconds();
  }

  bool report_due(Uint32 freq_sec) {
    if (!m_active || freq_sec == 0) return false;
    const NDB_TICKS now = NdbTick_getCurrentTicks();
    /* Milliseconds, not seconds(): whole-second truncation would
       count a marginally early check as a whole period short. A tick
       that arrives early is simply not due yet; the tick handlers
       re-arm for the remainder with next_tick_delay_ms(). */
    if (NdbTick_Elapsed(m_last_report, now).milliSec() >=
        Uint64(freq_sec) * 1000) {
      m_last_report = now;
      return true;
    }
    return false;
  }

  /**
   * Delay for re-arming the report tick: the rest of the current
   * report period, capped at the time queue maximum. Re-arming with
   * the remainder (not a full capped period) keeps the report cadence
   * at the configured frequency when the period exceeds the cap.
   */
  Uint32 next_tick_delay_ms(Uint32 freq_sec) const {
    const Uint64 period_ms = Uint64(freq_sec) * 1000;
    Uint64 remain_ms = period_ms;
    if (m_active) {
      const Uint64 since_ms =
          NdbTick_Elapsed(m_last_report, NdbTick_getCurrentTicks()).milliSec();
      remain_ms = (since_ms < period_ms) ? (period_ms - since_ms) : 1;
    }
    return (remain_ms > NodeStartLog::MAX_TICK_DELAY_MILLIS)
               ? NodeStartLog::MAX_TICK_DELAY_MILLIS
               : (Uint32)remain_ms;
  }

  bool escalate_due() {
    if (!m_active) return false;
    const NDB_TICKS now = NdbTick_getCurrentTicks();
    if (!m_escalated) {
      if (NdbTick_Elapsed(m_step_start, now).seconds() >=
          ESCALATE_FIRST_SECS) {
        m_escalated = true;
        m_last_escalate = now;
        return true;
      }
      return false;
    }
    if (NdbTick_Elapsed(m_last_escalate, now).seconds() >=
        ESCALATE_REPEAT_SECS) {
      m_last_escalate = now;
      return true;
    }
    return false;
  }

 private:
  NDB_TICKS m_step_start;
  NDB_TICKS m_last_report;
  NDB_TICKS m_last_escalate;
  bool m_escalated;
  bool m_active;
};

/**
 * Progress sources owned by another block than the one that reports
 * them. Plain declarations keep the readers free of the owners' headers;
 * the definitions live next to the data.
 *
 * nsl_lqh_copy_row_ops_total() (DblqhMain.cpp): rows received on the
 * fragment copy path by all DBLQH workers, read by the DBDIH step 12
 * tick in the main thread (the workers count atomically).
 *
 * nsl_dict_restart_progress() (Dbdict.cpp): the position of the DBDICT
 * schema restore, read by the DBDIH step 7 tick in the same thread.
 * Returns false when no schema restore is running.
 *
 * nsl_dih_sr_metadata_start() (DbdihMain.cpp): the tick (as Uint64, 0
 * when the step has not started) at which a non-master node's step 7
 * started in a system restart, read by the DBLQH proxy when it accounts
 * the step's completion at its first START_FRAGREQ. The proxy runs in
 * the rep thread (mt.cpp thr_LOCAL) and DBDIH in the main thread
 * (thr_GLOBAL): DBDIH publishes the value with a release store.
 */
Uint64 nsl_lqh_copy_row_ops_total();
bool nsl_dict_restart_progress(Uint32 &pass, Uint32 &passes, Uint32 &object,
                               Uint32 &last_object);
Uint64 nsl_dih_sr_metadata_start();
/**
 * nsl_dih_performed_copy_phase() / nsl_dih_wait_lcp_reported()
 * (DbdihMain.cpp): whether this node was taken over at wait point 4.2 of
 * a system restart and whether DBDIH already logged its own step 13 for
 * that take-over; read by NDBCNTR at wait point 5.2, where a non-master
 * prints the step 12 skipped marker and the step 13 boundaries.
 */
bool nsl_dih_performed_copy_phase();
bool nsl_dih_wait_lcp_reported();
/**
 * nsl_lqh_proxy_undo_dd_start() (DblqhProxy.cpp): the node-wide start of
 * step 9 as a tick value (NDB_TICKS::getUint64), 0 until the last LDM has
 * finished its restore; read by the LDM workers (atomic, other thread) to
 * anchor their step 9 waiting/completed lines at the node-wide start and
 * to report the wait for the other LDMs' restore as a step 8 wait.
 */
Uint64 nsl_lqh_proxy_undo_dd_start();
/**
 * nsl_lqh_proxy_redo_prepare_done() (DblqhProxy.cpp): counts one LDM's
 * step 6 (redo-prepare) completion in the proxy (atomic, other thread)
 * and returns the new count with the number of LDMs holding REDO log
 * parts, so that the last of them prints the node-wide completion.
 */
Uint32 nsl_lqh_proxy_redo_prepare_done(Uint32 &ldms_with_log_parts);
/**
 * nsl_cntr_local_lcp_barrier() (NdbcntrMain.cpp): the local-checkpoint
 * barrier of the REDO logging phase of a take-over (step 12 sub-step 3).
 * DBLQH answers the first COPY_ACTIVEREQ of that phase only once every
 * LDM has completed a local LCP of the fragments it copied, the GCI in
 * that checkpoint is restorable and the log tails are cut (NDBCNTR's
 * WAIT_ALL_COMPLETE_LCP protocol). Returns 0 when no such barrier is
 * pending, 1 while the LDMs checkpoint (ldms_done of ldms have
 * finished), 2 once all have and gci_needed must still become restorable
 * (gci_done is), 3 while the log tails are being cut. Read by the DBDIH
 * step 12 tick; NDBCNTR and DBDIH share the main thread.
 *
 * nsl_dih_sr_receiving_tables() (DbdihMain.cpp): whether a non-master of
 * a system restart has entered step 7 sub-step 2 (receiving the tables
 * from the master), with that sub-step's start tick (Uint64) and the
 * tables received so far; read by the DBLQH proxy (rep thread) when it
 * completes the step at its first START_FRAGREQ, published by DBDIH with
 * release stores.
 */
Uint32 nsl_cntr_local_lcp_barrier(Uint32 &ldms_done, Uint32 &ldms,
                                  Uint32 &gci_needed, Uint32 &gci_done);
bool nsl_dih_sr_receiving_tables(Uint64 &sub_start, Uint32 &tables);

#undef JAM_FILE_ID

#endif
