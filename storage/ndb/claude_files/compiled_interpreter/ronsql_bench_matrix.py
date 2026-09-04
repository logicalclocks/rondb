#!/usr/bin/env python3
"""ronsql_bench_matrix.py — RonSQL vs MySQL vs compiled-interpreter matrix.

Drives the rondb-cli benchmarks (.bench_ronsql / .bench_sql <query> T N)
over the ronsqlcrunch MTR cluster and reports latency, throughput and
where the time goes, for every combination of

    engine    : ronsql (RDRS REST /ronsql), mysqld (aggregation pushdown
                ON), mysqld_nopush (aggregation pushdown OFF)
    compiler  : CompiledInterpreter OFF / ON on the data nodes
    threads   : client thread counts (--threads 1,8)
    query     : every query of the rondb-cli registry (.bench_ronsql list /
                .bench_sql list) or a subset

Flow: start the cluster (mtr --suite=ronsqlcrunch setup --start-and-exit,
with your cpubind.cnf and optional Rondis), load TPC-H with rondb-cli
.load_tpch, then for each compiler arm flip the data-node mode
(ndb_mgm ALL SET CompiledInterpreter, falling back to a cluster restart
+ reload when the build does not support the SET) and run the matrix.
Every case runs the CLI in a subprocess and streams its warmup /
progress / result lines live, followed by a one-line summary and an
ETA. Per case the driver also records the ndbinfo.jit counter deltas
(programs compiled / reused / fallback, rows executed, compile time)
and, for the mysqld engines, mysqld's NDB API counters (time spent
waiting for the data nodes, scan batches, rows read, pushed queries).

Where the time goes:
  - RonSQL: the server-side phase breakdown RDRS returns in the
    x-ronsql-phases header (parse / analyze / load / plan / compile /
    prepare / ndbprep / send / firstbatch / drain / print / execute),
    plus the HTTP+client overhead = client latency - execute - prepare.
  - mysqld: NDB wait time per request (Ndb_api_wait_nanos_count) versus
    the remainder spent in mysqld, scan batches and rows read per
    request, pushed queries per request.
  - both: ndbinfo.jit deltas per case.

Usage (from the repo root):
  python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
      [--build prod_build] [--sf 0.1] [--threads 1,8] [--seconds 5 | --requests N]
      [--queries all|fs|offline_fs|tpch_cte|tpch_official|name,name,...]
      [--engines ronsql,mysqld,mysqld_nopush] [--compiler off,on]
      [--cpubind FILE] [--client-cpus 16-19] [--rondis] [--rdrs-threads 64]
      [--toggle auto|set|restart] [--no-load] [--keep-cluster]
      [--no-start --mysql-port P --mysql-sock S --rdrs-port P --connectstring C]
      [--out DIR] [--quick] [--verbose]

  --quick     = --threads 1 --seconds 2 (smoke run)
  --stop      = only stop a cluster left running by --keep-cluster

Outputs: <out>/results.json (every case, all parsed numbers),
<out>/report.md (the tables printed at the end), <out>/cases/*.txt
(raw CLI output per case), <out>/cluster/ (mtr output, extra cnf).
"""
import argparse
import configparser
import datetime
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, '..', '..', '..', '..'))

PHASES = ['parse', 'analyze', 'load', 'plan', 'compile', 'prepare', 'subquery',
          'ndbprep', 'send', 'firstbatch', 'drain', 'print', 'execute']
JIT_COLS = ['programs_compiled', 'programs_reused', 'programs_fallback',
            'rows_executed', 'compile_ns_total']
MYSQLD_STATUS = ['Ndb_api_wait_nanos_count', 'Ndb_api_wait_exec_complete_count',
                 'Ndb_api_wait_scan_result_count', 'Ndb_api_scan_batch_count',
                 'Ndb_api_read_row_count', 'Ndb_api_bytes_received_count',
                 'Ndb_api_table_scan_count', 'Ndb_api_range_scan_count',
                 'Ndb_api_pruned_scan_count', 'Ndb_pushed_queries_executed',
                 'Ndb_pushed_reads']
PUSHDOWN_VARS = ['ndb_pushdown_aggregate', 'ndb_join_pushdown_aggregate',
                 'ndb_join_pushdown_aggregate_outer_join']
ENGINES = {
    'ronsql':        ('bench_ronsql', None),
    'mysqld':        ('bench_sql', 'ON'),
    'mysqld_nopush': ('bench_sql', 'OFF'),
}
ANSI = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')
RE_LAT = re.compile(r'([0-9.]+)(us|ms|s)\b')
RE_WARMUP = re.compile(r'Warmup: ([0-9.]+(?:us|ms|s)), (\d+) result rows')
RE_REQ = re.compile(r'Requests: (\d+) \(errors: (\d+)\)')
RE_TPUT = re.compile(r'Throughput: ([0-9.]+) queries/sec')
RE_LATLINE = re.compile(r'Latency: min=(\S+) avg=(\S+) max=(\S+) p95=(\S+) p99=(\S+) p99\.9=(\S+)')
RE_PHASE = re.compile(r'^\s*(' + '|'.join(PHASES) + r')\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s*$')
RE_ROWS = re.compile(r'rows drained\s+([0-9.]+) per request')
RE_DONE = re.compile(r'Benchmark \S+ completed in ([0-9.]+)s')
RE_LIST = re.compile(r'^    (\S+)\s+(.*\S)\s*$')
RE_PROGRESS = re.compile(r'Progress: (\d+)/(\d+) requests')


# ---------------------------------------------------------------- utils
def log(msg=''):
    print(msg, flush=True)


def ts():
    return datetime.datetime.now().strftime('%H:%M:%S')


def lat_ms(s):
    m = RE_LAT.match(s)
    if not m:
        return None
    v = float(m.group(1))
    return {'us': v / 1000.0, 'ms': v, 's': v * 1000.0}[m.group(2)]


def fmt_ms(v):
    if v is None:
        return '-'
    if v < 1.0:
        return '%.0fus' % (v * 1000)
    if v < 100:
        return '%.2fms' % v
    return '%.0fms' % v


def fmt_ratio(a, b):
    if a is None or b is None or b == 0:
        return '-'
    return '%.2fx' % (a / b)


def run(cmd, cwd=None, env=None, check=True, quiet=False, timeout=None):
    """Run to completion, return (rc, output). Output is captured."""
    if not quiet:
        log('   $ ' + ' '.join(cmd))
    p = subprocess.run(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, timeout=timeout)
    out = p.stdout.decode('utf-8', 'replace')
    if check and p.returncode != 0:
        raise RuntimeError('command failed (rc=%d): %s\n%s' % (p.returncode, ' '.join(cmd), out[-4000:]))
    return p.returncode, out


def stream(cmd, cwd=None, env=None, line_cb=None, prefix='   | '):
    """Run streaming stdout+stderr line by line; return (rc, all_lines)."""
    p = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    lines = []
    for raw in p.stdout:
        line = ANSI.sub('', raw.decode('utf-8', 'replace').rstrip('\n'))
        lines.append(line)
        if line_cb is None:
            log(prefix + line)
        else:
            line_cb(line)
    p.wait()
    return p.returncode, lines


def which_bin(build, name):
    for d in ('runtime_output_directory', 'bin', os.path.join('tools', 'rondb-cli')):
        p = os.path.join(build, d, name)
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    p = shutil.which(name)
    if p:
        return p
    raise RuntimeError('binary not found in %s: %s' % (build, name))


def median(xs):
    return statistics.median(xs) if xs else None


# ------------------------------------------------------------- cluster
class Cluster:
    """The ronsqlcrunch mtr cluster: start / discover ports / stop."""

    def __init__(self, a):
        self.a = a
        self.build = a.build
        self.mtr_dir = os.path.join(a.build, 'mysql-test')
        self.var = os.path.join(self.mtr_dir, 'var')
        self.mysql = which_bin(a.build, 'mysql')
        self.mysqladmin = which_bin(a.build, 'mysqladmin')
        self.ndb_mgm = which_bin(a.build, 'ndb_mgm')
        self.mysql_port = a.mysql_port
        self.mysql_sock = a.mysql_sock
        self.mysql_ports = []
        self.mysql_socks = []
        self.rdrs_port = a.rdrs_port
        self.rondis_port = None
        self.connectstring = a.connectstring
        self.started_by_us = False
        self.set_supported = None      # None = unknown, True/False after probe

    # -- mtr start / stop -------------------------------------------------
    def extra_cnf(self, mode):
        """Write the --defaults-extra-file for this start: user cpubind +
        Rondis + RDRS threads + CompiledInterpreter mode."""
        parts = []
        if self.a.cpubind:
            with open(self.a.cpubind) as f:
                parts.append('# --- %s ---\n%s\n' % (self.a.cpubind, f.read()))
        if self.a.rondis:
            parts.append('[rdrs.1.1]\nenable-rondis=true\n'
                         'config-json-template=suite/ronsqlcrunch/rdrs_rondis_config_template.json\n')
        parts.append('[rdrs.1.1]\nRESTNumThreads=%d\n' % self.a.rdrs_threads)
        parts.append('[cluster_config.1]\nCompiledInterpreter=%s\n' % mode)
        if self.a.num_cpus:
            parts.append('[cluster_config.1]\nNumCPUs=%d\n' % self.a.num_cpus)
        path = os.path.join(self.a.out, 'cluster', 'extra_%s.cnf' % mode.lower())
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write('\n'.join(parts))
        return path

    def start(self, mode):
        extra = self.extra_cnf(mode)
        cmd = ['./mtr', '--suite=ronsqlcrunch', 'setup', '--start-and-exit',
               '--defaults-extra-file=' + os.path.abspath(extra)]
        log('%s == starting cluster (CompiledInterpreter=%s): %s' % (ts(), mode, ' '.join(cmd)))
        mtr_log = os.path.join(self.a.out, 'cluster', 'mtr_start_%s.log' % mode.lower())
        lines = []
        def cb(line):
            lines.append(line)
            if self.a.verbose or re.search(r'port|Started|error|fail|warning|Applying cpu', line, re.I):
                log('   | ' + line)
        rc, _ = stream(cmd, cwd=self.mtr_dir, line_cb=cb)
        with open(mtr_log, 'w') as f:
            f.write('\n'.join(lines) + '\n')
        if rc != 0:
            raise RuntimeError('mtr --start-and-exit failed (rc=%d), see %s' % (rc, mtr_log))
        self.started_by_us = True
        self.discover()
        self.wait_ready()

    def discover(self):
        cp = configparser.ConfigParser(strict=False, allow_no_value=True,
                                       interpolation=None, delimiters=('=',))
        cp.optionxform = str
        cp.read(os.path.join(self.var, 'my.cnf'))
        self.mysql_ports, self.mysql_socks = [], []
        for sec in cp.sections():
            if re.match(r'^mysqld\.\d+\.1$', sec):
                self.mysql_ports.append(int(cp[sec]['port']))
                self.mysql_socks.append(cp[sec]['socket'])
        self.mysql_port, self.mysql_sock = self.mysql_ports[0], self.mysql_socks[0]
        self.connectstring = cp['mysql_cluster.1']['ndb_connectstring']
        with open(os.path.join(self.var, 'rdrs.1.1_config.json')) as f:
            j = json.load(f)
        self.rdrs_port = int(j['REST']['ServerPort'])
        self.rondis_port = int(j.get('Rondis', {}).get('ServerPort', 0)) if j.get('Rondis', {}).get('Enable') else None
        log('   mysqld port %d (socket %s), rdrs port %d, rondis %s, connectstring %s'
            % (self.mysql_port, self.mysql_sock, self.rdrs_port, self.rondis_port or '-', self.connectstring))

    def wait_ready(self, timeout=120):
        """Wait for a 200 from RDRS /0.2.0/health: it answers 200 only when
        its RonDB connection is up and at least one data node is STARTED
        (503 otherwise), which is the readiness we need before loading."""
        deadline = time.time() + timeout
        url = 'http://127.0.0.1:%d/0.2.0/health' % self.rdrs_port
        last = None
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=3) as resp:
                    if resp.status == 200:
                        break
                    last = 'HTTP %d' % resp.status
            except urllib.error.HTTPError as e:      # 503 while not ready, 4xx = misconfig
                last = 'HTTP %d %s' % (e.code, e.read().decode('utf-8', 'replace')[:120])
            except Exception as e:                   # connection refused / timeout
                last = e
            time.sleep(1)
        else:
            raise RuntimeError('RDRS not healthy on %s: %s' % (url, last))
        self.sql('SELECT 1')

    def stop(self):
        log('%s == stopping cluster' % ts())
        for sock in (self.mysql_socks or [self.mysql_sock]):
            if sock:
                run([self.mysqladmin, '-uroot', '-S', sock, 'shutdown'], check=False, quiet=True)
        if self.connectstring:
            run([self.ndb_mgm, '-c', self.connectstring, '-e', 'SHUTDOWN'], check=False, quiet=True, timeout=300)
        pidf = os.path.join(self.var, 'run', 'rdrs.1.1.pid')
        if os.path.isfile(pidf):
            try:
                os.kill(int(open(pidf).read().strip()), signal.SIGTERM)
            except (OSError, ValueError):
                pass
        # leftovers by pid file (ndb_mgmd, mysqld)
        deadline = time.time() + 60
        rundir = os.path.join(self.var, 'run')
        while time.time() < deadline and os.path.isdir(rundir) and os.listdir(rundir):
            time.sleep(1)
        for pf in (os.listdir(rundir) if os.path.isdir(rundir) else []):
            try:
                os.kill(int(open(os.path.join(rundir, pf)).read().strip()), signal.SIGKILL)
            except (OSError, ValueError):
                pass
        self.started_by_us = False

    # -- SQL helpers -------------------------------------------------------
    def sql(self, query, db=None):
        cmd = [self.mysql, '-uroot', '-S', self.mysql_sock, '--batch', '--skip-column-names', '-e', query]
        if db:
            cmd.append(db)
        rc, out = run(cmd, quiet=True, check=False)
        if rc != 0:
            raise RuntimeError('mysql failed: %s\n%s' % (query, out))
        return [l.split('\t') for l in out.splitlines() if l.strip()]

    def jit_counters(self):
        rows = self.sql('SELECT %s FROM ndbinfo.jit' % ','.join('SUM(%s)' % c for c in JIT_COLS))
        return dict(zip(JIT_COLS, [int(float(v)) for v in rows[0]])) if rows else {}

    def mysqld_status(self):
        rows = self.sql("SHOW GLOBAL STATUS WHERE Variable_name IN (%s)"
                        % ','.join("'%s'" % v for v in MYSQLD_STATUS))
        return {k: int(v) for k, v in rows}

    def set_pushdown(self, on):
        self.sql('SET GLOBAL ' + ', '.join('%s=%s' % (v, on) for v in PUSHDOWN_VARS))

    def measure_idle(self, seconds=3.0):
        """Per-second rate of mysqld's NDB API counters while nothing runs:
        the schema-distribution / index-stat / binlog threads sit in NDB API
        waits all the time, so Ndb_api_wait_nanos_count grows by ~1 s per
        second even when idle. The report subtracts rate * case wall time."""
        s0 = self.mysqld_status()
        time.sleep(seconds)
        s1 = self.mysqld_status()
        return {k: (s1.get(k, 0) - s0.get(k, 0)) / seconds for k in MYSQLD_STATUS}

    def analyze_tpch(self):
        """ANALYZE TABLE so the MySQL optimizer has NDB index statistics from
        the first case on (otherwise plans drift between the arms as the
        index-stat thread catches up, which is an order effect, not JIT)."""
        tables = [r[0] for r in self.sql('SHOW TABLES', db='tpch')]
        if tables:
            self.sql('ANALYZE TABLE ' + ', '.join('tpch.' + t for t in tables))
        return tables

    # -- compiler mode ----------------------------------------------------
    def jit_mode_values(self):
        try:
            rows = self.sql('SELECT node_id, config_value FROM ndbinfo.config_values WHERE config_param = 709')
            return {int(n): v for n, v in rows}   # config_value is a string column
        except RuntimeError:
            return {}

    def try_set_mode(self, mode):
        rc, out = run([self.ndb_mgm, '-c', self.connectstring, '-e', 'ALL SET CompiledInterpreter %s' % mode],
                      check=False, quiet=True)
        ok = rc == 0 and not re.search(r'unknown|unsupported|error|usage', out, re.I)
        if self.a.verbose or not ok:
            log('   ndb_mgm SET rc=%d: %s' % (rc, out.strip().replace('\n', ' | ')[:300]))
        return ok


# --------------------------------------------------------------- driver
class Driver:
    def __init__(self, a):
        self.a = a
        self.cl = Cluster(a)
        self.rondb = a.rondb or which_bin(a.build, 'rondb')
        self.results = []
        self.case_times = []
        self.n_cache = {}
        os.makedirs(os.path.join(a.out, 'cases'), exist_ok=True)

    # -- rondb-cli --------------------------------------------------------
    def cli_cmd(self, command):
        cmd = []
        if self.a.client_cpus:
            cmd += ['taskset', '-c', self.a.client_cpus]
        cmd += [self.rondb, '--mysql-port', str(self.cl.mysql_port), '--rdrs-port', str(self.cl.rdrs_port)]
        if self.cl.rondis_port:
            cmd += ['--rondis-port', str(self.cl.rondis_port)]
        else:
            cmd += ['--no-rondis']
        cmd += ['-e', command]
        return cmd

    def cli(self, command, line_cb=None):
        return stream(self.cli_cmd(command), line_cb=line_cb)

    def list_queries(self):
        out = {}
        for eng, cmd in (('ronsql', '.bench_ronsql list'), ('sql', '.bench_sql list')):
            rc, lines = self.cli(cmd, line_cb=lambda l: None)
            names = []
            for l in lines:
                m = RE_LIST.match(l)
                if m and m.group(1) != 'all' and '[.bench_sql only]' not in l:
                    names.append((m.group(1), m.group(2)))
            if not names:
                raise RuntimeError('could not parse "%s" output:\n%s' % (cmd, '\n'.join(lines)))
            out[eng] = names
        # pair RonSQL names with their .bench_sql twin (cte_ prefix for the CTE rewrites)
        sql_names = [n for n, _ in out['sql']]
        pairs = []
        for n, d in out['ronsql']:
            sql = n if n in sql_names else ('cte_' + n if 'cte_' + n in sql_names else None)
            pairs.append({'name': n, 'sql': sql, 'desc': d, 'mysql_only': False})
        covered = {p['sql'] for p in pairs}
        for n, d in out['sql']:
            if n not in covered:
                pairs.append({'name': n, 'sql': n, 'desc': d, 'mysql_only': True})
        return pairs

    def select_queries(self, pairs):
        sel = self.a.queries
        if sel in ('all', ''):
            return pairs
        cats = {'fs': lambda n: n.startswith('fs_'),
                'offline_fs': lambda n: n.startswith('offline_fs_'),
                'tpch_cte': lambda n: n.startswith('tpch_q') and not n.endswith('_official'),
                'tpch_official': lambda n: n.startswith('tpch_q') and n in [p['sql'] for p in pairs if p['mysql_only']]}
        if sel in cats:
            return [p for p in pairs if cats[sel](p['name'])]
        wanted = set(sel.split(','))
        chosen = [p for p in pairs if p['name'] in wanted or p['sql'] in wanted]
        missing = wanted - {p['name'] for p in chosen} - {p['sql'] for p in chosen}
        if missing:
            raise RuntimeError('unknown queries: %s' % ', '.join(sorted(missing)))
        return chosen

    # -- load ---------------------------------------------------------------
    EXPECTED_LINEITEM_PER_SF = 6000000

    def ensure_loaded(self):
        want = int(round(self.EXPECTED_LINEITEM_PER_SF * self.a.sf))   # = tpchScaled() in tpch.go
        have = 0
        try:
            have = int(self.cl.sql('SELECT COUNT(*) FROM tpch.lineitem')[0][0])
        except RuntimeError:
            pass
        if self.a.no_load:
            log('%s == --no-load: tpch.lineitem has %d rows' % (ts(), have))
            return
        if have and abs(have - want) <= max(1, want // 100):
            log('%s == tpch already loaded (lineitem %d rows for sf %g), skipping load' % (ts(), have, self.a.sf))
            self.analyze()
            return
        if have:
            log('%s == tpch has %d lineitem rows, want %d: dropping and reloading' % (ts(), have, want))
            self.cli('.drop_tpch')
        log('%s == loading TPC-H sf=%g (%d threads, batch %d)' % (ts(), self.a.sf, self.a.load_threads, self.a.load_batch))
        t0 = time.time()
        rc, lines = self.cli('.load_tpch %g %d %d' % (self.a.sf, self.a.load_threads, self.a.load_batch),
                             line_cb=lambda l: log('   | ' + l) if l.strip() else None)
        if rc != 0 or not any('TPC-H load complete' in l for l in lines):
            raise RuntimeError('.load_tpch failed (rc=%d)' % rc)
        log('   load took %.0fs' % (time.time() - t0))
        self.analyze()

    def analyze(self):
        t0 = time.time()
        tables = self.cl.analyze_tpch()
        log('%s == ANALYZE TABLE on %d tpch tables (%.1fs) so the MySQL optimizer has index statistics'
            % (ts(), len(tables), time.time() - t0))

    # -- compiler arm -------------------------------------------------------
    def enter_arm(self, mode, first):
        """Make the data nodes run with CompiledInterpreter=mode.

        First arm: start the cluster with the mode pinned in the extra
        cnf (or, with --no-start, SET it on the running cluster), then
        load TPC-H. Later arms: ndb_mgm ALL SET CompiledInterpreter when
        the build supports it (--toggle auto/set), else stop + start +
        reload (--toggle restart, or auto without SET support)."""
        if first:
            if self.a.no_start:
                self.cl.wait_ready()
                if self.a.toggle != 'restart':
                    self.cl.set_supported = self.cl.try_set_mode(mode)
                    if not self.cl.set_supported:
                        log('   WARNING: cannot SET CompiledInterpreter on this cluster; '
                            'assuming it already runs with %s' % mode)
            else:
                self.cl.start(mode)
            self.ensure_loaded()
            return
        if self.a.toggle in ('auto', 'set') and self.cl.set_supported is not False:
            if self.cl.try_set_mode(mode):
                self.cl.set_supported = True
                vals = self.cl.jit_mode_values()
                if vals:
                    log('   ndbinfo config_values(CompiledInterpreter=709): %s' % vals)
                return
            self.cl.set_supported = False
            if self.a.toggle == 'set':
                raise RuntimeError('ALL SET CompiledInterpreter not supported by this build (use --toggle restart)')
            log('   SET not supported by this build: restarting the cluster for the %s arm' % mode)
        if self.a.no_start:
            raise RuntimeError('cannot switch to %s on a --no-start cluster without SET support' % mode)
        self.cl.stop()
        self.cl.start(mode)
        self.ensure_loaded()

    # -- one case -----------------------------------------------------------
    def requests_for(self, engine, q, arm, threads):
        if self.a.requests:
            return self.a.requests
        key = (engine, q['name'], arm)
        if key not in self.n_cache:
            log('   probe: %s %s T=1 N=%d (sizes the run to ~%gs)' % (engine, q['name'], self.a.probe_requests, self.a.seconds))
            probe = self.run_cli_bench(engine, q, 1, self.a.probe_requests, tag='probe', record=False)
            avg = probe.get('avg_ms') if probe else None
            if avg is None or avg <= 0:
                n = self.a.min_requests
            else:
                n = int(self.a.seconds * 1000.0 / avg)
                n = max(self.a.min_requests, min(self.a.max_requests, n))
            self.n_cache[key] = n
        return self.n_cache[key]

    def run_cli_bench(self, engine, q, threads, n, tag, record=True):
        bench, pushdown = ENGINES[engine]
        name = q['name'] if engine == 'ronsql' else q['sql']
        command = '.%s %s %d %d' % (bench, name, threads, n)
        r = {'engine': engine, 'query': q['name'], 'sql_name': q['sql'], 'threads': threads,
             'requests_per_thread': n, 'command': command, 'ok': False, 'phases': {}, 'error': None}
        if pushdown is not None:
            self.cl.set_pushdown(pushdown)
        jit0 = self.cl.jit_counters()
        st0 = self.cl.mysqld_status() if pushdown is not None else None
        t0 = time.time()
        shown = []
        def cb(line):
            s = line.strip()
            if not s:
                return
            interesting = (s.startswith(('Warmup', 'Progress', 'Throughput', 'Latency', 'Requests', 'rows drained'))
                           or ('error' in s.lower() and 'errors: 0' not in s)
                           or RE_PHASE.match(line) is not None or 'not available' in s)
            if self.a.verbose or interesting:
                log('   | ' + line)
            shown.append(line)
        rc, lines = self.cli(command, line_cb=cb)
        r['wall_s'] = time.time() - t0
        r['rc'] = rc
        text = '\n'.join(lines)
        if record:
            with open(os.path.join(self.a.out, 'cases', '%s.txt' % tag), 'w') as f:
                f.write(text + '\n')
        for line in lines:
            m = RE_WARMUP.search(line)
            if m:
                r['warmup_ms'], r['result_rows'] = lat_ms(m.group(1)), int(m.group(2))
            m = RE_REQ.search(line)
            if m:
                r['requests'], r['errors'] = int(m.group(1)), int(m.group(2))
            m = RE_TPUT.search(line)
            if m:
                r['qps'] = float(m.group(1))
            m = RE_LATLINE.search(line)
            if m:
                for k, v in zip(('min_ms', 'avg_ms', 'max_ms', 'p95_ms', 'p99_ms', 'p999_ms'), m.groups()):
                    r[k] = lat_ms(v)
            m = RE_PHASE.match(line)
            if m:
                r['phases'][m.group(1)] = {'avg_ms': lat_ms(m.group(2)), 'p95_ms': lat_ms(m.group(3)),
                                           'p99_ms': lat_ms(m.group(4)), 'max_ms': lat_ms(m.group(5))}
            m = RE_ROWS.search(line)
            if m:
                r['rows_drained'] = float(m.group(1))
            m = RE_DONE.search(line)
            if m:
                r['bench_s'] = float(m.group(1))
        r['ok'] = rc == 0 and 'qps' in r and r.get('errors', 1) == 0
        if not r['ok']:
            err = [l for l in lines if 'error' in l.lower() or 'fail' in l.lower()]
            r['error'] = (err[-1] if err else 'rc=%d, no result line' % rc)[:300]
        jit1 = self.cl.jit_counters()
        r['jit_delta'] = {k: jit1.get(k, 0) - jit0.get(k, 0) for k in JIT_COLS}
        if st0 is not None:
            st1 = self.cl.mysqld_status()
            r['mysqld_delta'] = {k: st1.get(k, 0) - st0.get(k, 0) for k in MYSQLD_STATUS}
        return r

    def run_case(self, idx, total, arm, engine, q, threads, rep=0):
        n = self.requests_for(engine, q, arm, threads)
        tag = '%s_%s_%s_T%d' % (arm.lower(), engine, q['name'], threads) + ('_r%d' % rep if rep else '')
        t0 = time.time()
        log('')
        log('%s [%d/%d] compiler=%s engine=%s query=%s threads=%d requests/thread=%d%s'
            % (ts(), idx, total, arm, engine, q['name'], threads, n, (' repeat %d' % (rep + 1)) if rep else ''))
        r = self.run_cli_bench(engine, q, threads, n, tag=tag)
        r.update({'compiler': arm, 'tag': tag, 'rep': rep})
        self.results.append(r)
        self.case_times.append(time.time() - t0)
        self.summary_line(r)
        done, avg = idx, sum(self.case_times) / len(self.case_times)
        log('   ETA: %d cases left, ~%.0fs (avg %.0fs/case)' % (total - done, (total - done) * avg, avg))
        self.save_json()

    def ndb_wait_ms(self, r):
        """Per-request NDB wait for a mysqld case, idle-baseline corrected."""
        md = r.get('mysqld_delta')
        if not md:
            return None
        n = (r.get('requests') or 0) + 1
        raw = md['Ndb_api_wait_nanos_count']
        corr = raw - self.idle.get('Ndb_api_wait_nanos_count', 0.0) * r.get('wall_s', 0.0)
        return max(0.0, corr) / n / 1e6

    def summary_line(self, r):
        if not r['ok']:
            log('   => FAILED: %s' % r['error'])
            return
        parts = ['%.1f q/s' % r['qps'], 'avg %s' % fmt_ms(r.get('avg_ms')), 'p95 %s' % fmt_ms(r.get('p95_ms'))]
        if r['engine'] == 'ronsql' and r['phases']:
            top = max(r['phases'].items(), key=lambda kv: kv[1]['avg_ms'] if kv[0] not in ('execute', 'prepare') else -1)
            ex = r['phases'].get('execute', {}).get('avg_ms')
            pr = r['phases'].get('prepare', {}).get('avg_ms')
            parts.append('server prepare %s + execute %s, top phase %s %s'
                         % (fmt_ms(pr), fmt_ms(ex), top[0], fmt_ms(top[1]['avg_ms'])))
            if r.get('avg_ms') is not None and ex is not None and pr is not None:
                parts.append('http+client %s' % fmt_ms(r['avg_ms'] - ex - pr))
        md = r.get('mysqld_delta')
        if md:
            nreq = (r.get('requests') or 0) + 1
            parts.append('ndb wait %s/req, %.1f scan batches/req, %.0f rows/req, pushed %.1f/req'
                         % (fmt_ms(self.ndb_wait_ms(r)),
                            md['Ndb_api_scan_batch_count'] / nreq,
                            md['Ndb_api_read_row_count'] / nreq,
                            md['Ndb_pushed_queries_executed'] / nreq))
        jd = r['jit_delta']
        parts.append('jit compiled %d reused %d fallback %d rows %d'
                     % (jd['programs_compiled'], jd['programs_reused'], jd['programs_fallback'], jd['rows_executed']))
        log('   => ' + ', '.join(parts))

    def save_json(self):
        meta = {'build': self.a.build, 'sf': self.a.sf, 'threads': self.a.threads, 'seconds': self.a.seconds,
                'order': self.a.order, 'repeat': self.a.repeat, 'idle_rates': getattr(self, 'idle', {}),
                'requests': self.a.requests, 'engines': self.a.engines, 'compiler': self.a.compiler,
                'cpubind': self.a.cpubind, 'client_cpus': self.a.client_cpus, 'rondis': self.a.rondis,
                'started': self.started, 'host': os.uname().nodename, 'os': os.uname().sysname,
                'arch': os.uname().machine}
        with open(os.path.join(self.a.out, 'results.json'), 'w') as f:
            json.dump({'meta': meta, 'cases': self.results}, f, indent=1)

    # -- main loop ------------------------------------------------------------
    def main(self):
        a = self.a
        self.started = datetime.datetime.now().isoformat(timespec='seconds')
        log('%s == ronsql_bench_matrix: build=%s sf=%g threads=%s engines=%s compiler=%s %s'
            % (ts(), a.build, a.sf, a.threads, a.engines, a.compiler,
               ('requests=%d' % a.requests) if a.requests else ('~%gs/case' % a.seconds)))
        arms = [m.upper() for m in a.compiler]
        self.idle = {}
        try:
            self.enter_arm(arms[0], first=True)
            pairs = self.select_queries(self.list_queries())
            log('%s == %d queries: %s' % (ts(), len(pairs), ', '.join(p['name'] for p in pairs)))
            if any(e.startswith('mysqld') for e in a.engines):
                self.idle = self.cl.measure_idle(3.0)
                log('%s == idle baseline: mysqld waits %.2f s/s in the NDB API with nothing running (subtracted from ndb wait)'
                    % (ts(), self.idle.get('Ndb_api_wait_nanos_count', 0.0) / 1e9))
            if a.order == 'query-major' and len(arms) > 1:
                # Interleave the arms per query so plan drift / cache warm-up
                # cannot masquerade as a compiler effect. Needs the cheap
                # runtime SET; with a restart per switch fall back to arm-major.
                if a.toggle == 'restart' or not (self.cl.set_supported or self.cl.try_set_mode(arms[0])):
                    log('   runtime SET CompiledInterpreter not available: using --order arm-major')
                    a.order = 'arm-major'
                else:
                    self.cl.set_supported = True
            def eligible(q, eng):
                if eng == 'ronsql' and q['mysql_only']:
                    return False
                if eng != 'ronsql' and q['sql'] is None:
                    return False
                return True
            cases = []
            if a.order == 'arm-major':
                for rep in range(a.repeat):
                    for arm in arms:
                        for threads in a.threads:
                            for q in pairs:
                                for eng in a.engines:
                                    if eligible(q, eng):
                                        cases.append((arm, threads, q, eng, rep))
            else:
                for threads in a.threads:
                    for q in pairs:
                        for rep in range(a.repeat):
                            for arm in arms:
                                for eng in a.engines:
                                    if eligible(q, eng):
                                        cases.append((arm, threads, q, eng, rep))
            total = len(cases)
            log('%s == %d cases, order %s, repeat %d (report shows the median run per case)'
                % (ts(), total, a.order, a.repeat))
            idx = 0
            cur_arm = None
            for arm, threads, q, eng, rep in cases:
                if arm != cur_arm:
                    if cur_arm is not None:
                        self.enter_arm(arm, first=False)
                    cur_arm = arm
                    if a.order == 'arm-major':
                        log('')
                        log('%s ===== compiler arm %s =====' % (ts(), arm))
                idx += 1
                self.run_case(idx, total, arm, eng, q, threads, rep)
        except KeyboardInterrupt:
            log('\n%s == interrupted' % ts())
        finally:
            self.save_json()
            if self.results:
                report = self.report()
                log('')
                log(report)
                with open(os.path.join(a.out, 'report.md'), 'w') as f:
                    f.write(report)
                log('== results: %s/results.json, %s/report.md, raw CLI output in %s/cases/'
                    % (a.out, a.out, a.out))
            if self.cl.started_by_us and not a.keep_cluster:
                self.cl.stop()
            elif self.cl.started_by_us:
                log('== cluster left running (--keep-cluster): mysqld port %d, rdrs port %d; stop with --stop'
                    % (self.cl.mysql_port, self.cl.rdrs_port))
        return 0 if all(r['ok'] for r in self.results) else 1

    # -- report -----------------------------------------------------------------
    def find(self, **kw):
        """The representative run for a (query, engine, compiler, threads):
        with --repeat, the ok run with the median avg latency; otherwise the
        single run (a failed one if that is all there is)."""
        hits = [r for r in self.results if all(r.get(k) == v for k, v in kw.items())]
        ok = sorted([r for r in hits if r['ok'] and r.get('avg_ms') is not None], key=lambda r: r['avg_ms'])
        if ok:
            return ok[len(ok) // 2]
        return hits[0] if hits else None

    def report(self):
        a = self.a
        R = self.results
        arms = [m.upper() for m in a.compiler]
        qnames = []
        for r in R:
            if r['query'] not in qnames:
                qnames.append(r['query'])
        out = []
        out.append('# RonSQL / MySQL / compiled-interpreter benchmark matrix')
        out.append('')
        out.append('build=%s sf=%g threads=%s engines=%s compiler=%s order=%s repeat=%d cpubind=%s client_cpus=%s host=%s (%s %s) started %s'
                   % (a.build, a.sf, a.threads, ','.join(a.engines), ','.join(arms), a.order, a.repeat, a.cpubind or '-',
                      a.client_cpus or '-', os.uname().nodename, os.uname().sysname, os.uname().machine, self.started))
        out.append('')
        failed = [r for r in R if not r['ok']]
        if failed:
            out.append('FAILED cases: ' + ', '.join('%s (%s)' % (r['tag'], r['error']) for r in failed))
            out.append('')

        def get(query, engine, arm, threads, key):
            r = self.find(query=query, engine=engine, compiler=arm, threads=threads)
            return r.get(key) if r and r['ok'] else None

        # A. latency + throughput per engine, OFF vs ON
        for threads in a.threads:
            out.append('## A. Latency (avg) and throughput per engine, compiler OFF vs ON — %d thread%s'
                       % (threads, 's' if threads != 1 else ''))
            out.append('')
            hdr = '| query |'
            sep = '|---|'
            for eng in a.engines:
                for arm in arms:
                    hdr += ' %s %s avg |' % (eng, arm)
                    sep += '---:|'
                if len(arms) == 2:
                    hdr += ' %s ON/OFF |' % eng
                    sep += '---:|'
                for arm in arms:
                    hdr += ' %s %s q/s |' % (eng, arm)
                    sep += '---:|'
            out += [hdr, sep]
            for qn in qnames:
                row = '| %s |' % qn
                for eng in a.engines:
                    avgs = [get(qn, eng, arm, threads, 'avg_ms') for arm in arms]
                    row += ''.join(' %s |' % fmt_ms(v) for v in avgs)
                    if len(arms) == 2:
                        # speedup = OFF latency / ON latency (>1 = ON faster)
                        row += ' %s |' % fmt_ratio(avgs[0], avgs[1])
                    row += ''.join(' %s |' % ('%.0f' % v if v is not None else '-')
                                   for v in [get(qn, eng, arm, threads, 'qps') for arm in arms])
                out.append(row)
            out.append('')

        # B. mysqld vs ronsql (per arm, per thread count)
        if 'ronsql' in a.engines and any(e.startswith('mysqld') for e in a.engines):
            out.append('## B. RonSQL vs MySQL server (avg latency; ratio = mysqld / ronsql, >1 = RonSQL faster)')
            out.append('')
            hdr, sep = '| query | threads | compiler | ronsql |', '|---|---:|---|---:|'
            for eng in a.engines:
                if eng != 'ronsql':
                    hdr += ' %s | %s/ronsql |' % (eng, eng)
                    sep += '---:|---:|'
            out += [hdr, sep]
            for qn in qnames:
                for threads in a.threads:
                    for arm in arms:
                        rs = get(qn, 'ronsql', arm, threads, 'avg_ms')
                        if rs is None and not any(get(qn, e, arm, threads, 'avg_ms') for e in a.engines if e != 'ronsql'):
                            continue
                        row = '| %s | %d | %s | %s |' % (qn, threads, arm, fmt_ms(rs))
                        for eng in a.engines:
                            if eng != 'ronsql':
                                m = get(qn, eng, arm, threads, 'avg_ms')
                                row += ' %s | %s |' % (fmt_ms(m), fmt_ratio(m, rs))
                        out.append(row)
            out.append('')

        # C. RonSQL phases (threads = smallest)
        if 'ronsql' in a.engines:
            t = a.threads[0]
            out.append('## C. RonSQL — where the time goes (server-side phases, avg per request, %d thread%s)'
                       % (t, 's' if t != 1 else ''))
            out.append('')
            out.append('client = end-to-end latency seen by rondb-cli; http+client = client - prepare - execute '
                       '(RDRS HTTP handling, JSON, network); firstbatch = data-node execution until the first '
                       'result row (single-table: the whole DoAggregation); load = NDB dictionary lookups; '
                       'rows = result rows drained per request.')
            out.append('')
            cols = ['parse', 'analyze', 'load', 'plan', 'compile', 'ndbprep', 'send', 'firstbatch', 'drain', 'print']
            out.append('| query | compiler | client | prepare | execute | http+client | ' + ' | '.join(cols) + ' | rows | top |')
            out.append('|---|---|---:|---:|---:|---:|' + '---:|' * len(cols) + '---:|---|')
            for qn in qnames:
                for arm in arms:
                    r = self.find(query=qn, engine='ronsql', compiler=arm, threads=t)
                    if not r or not r['ok']:
                        continue
                    ph = r['phases']
                    pv = lambda k: ph.get(k, {}).get('avg_ms')
                    ex, pr = pv('execute'), pv('prepare')
                    over = (r['avg_ms'] - (ex or 0) - (pr or 0)) if r.get('avg_ms') is not None else None
                    top = max(((k, pv(k) or 0) for k in cols), key=lambda kv: kv[1]) if ph else ('-', 0)
                    out.append('| %s | %s | %s | %s | %s | %s | %s | %s | %s |'
                               % (qn, arm, fmt_ms(r.get('avg_ms')), fmt_ms(pr), fmt_ms(ex), fmt_ms(over),
                                  ' | '.join(fmt_ms(pv(k)) for k in cols),
                                  ('%.1f' % r['rows_drained']) if 'rows_drained' in r else '-',
                                  '%s %s' % (top[0], fmt_ms(top[1]))))
            out.append('')

        # D. mysqld split: NDB wait vs mysqld side
        meng = [e for e in a.engines if e.startswith('mysqld')]
        if meng:
            t = a.threads[0]
            out.append('## D. MySQL server — where the time goes (%d thread%s)' % (t, 's' if t != 1 else ''))
            out.append('')
            out.append('ndb wait = Ndb_api_wait_nanos_count per request (time the mysqld connection waited for '
                       'data nodes), corrected by the idle baseline (%.2f s/s of background NDB API waiting '
                       'measured before the matrix); mysqld = client latency - ndb wait (parsing, optimizer, '
                       'row processing, result transfer); batches / rows = scan batches and rows received from '
                       'NDB per request; pushed = pushed (SPJ) queries executed per request. Counters include '
                       'the warmup request and are approximate at short case durations.'
                       % (self.idle.get('Ndb_api_wait_nanos_count', 0.0) / 1e9))
            out.append('')
            out.append('| query | engine | compiler | client | ndb wait | mysqld | batches/req | rows/req | pushed/req | KB recv/req |')
            out.append('|---|---|---|---:|---:|---:|---:|---:|---:|---:|')
            for qn in qnames:
                for eng in meng:
                    for arm in arms:
                        r = self.find(query=qn, engine=eng, compiler=arm, threads=t)
                        if not r or not r['ok'] or not r.get('mysqld_delta'):
                            continue
                        md, n = r['mysqld_delta'], (r.get('requests') or 0) + 1
                        wait = self.ndb_wait_ms(r)
                        out.append('| %s | %s | %s | %s | %s | %s | %.1f | %.0f | %.2f | %.1f |'
                                   % (qn, eng, arm, fmt_ms(r['avg_ms']), fmt_ms(wait), fmt_ms(max(0.0, r['avg_ms'] - wait)),
                                      md['Ndb_api_scan_batch_count'] / n, md['Ndb_api_read_row_count'] / n,
                                      md['Ndb_pushed_queries_executed'] / n, md['Ndb_api_bytes_received_count'] / n / 1024))
            out.append('')

        # E. JIT counters per case (ON arm only, first thread count)
        if 'ON' in arms:
            t = a.threads[0]
            out.append('## E. ndbinfo.jit deltas per case, compiler ON, %d thread%s (per request incl. warmup)' % (t, 's' if t != 1 else ''))
            out.append('')
            out.append('| query | engine | compiled/req | reused/req | fallback | rows executed/req | compile us/req | compile share |')
            out.append('|---|---|---:|---:|---:|---:|---:|---:|')
            for qn in qnames:
                for eng in a.engines:
                    r = self.find(query=qn, engine=eng, compiler='ON', threads=t)
                    if not r or not r['ok']:
                        continue
                    jd, n = r['jit_delta'], (r.get('requests') or 0) + 1
                    cus = jd['compile_ns_total'] / n / 1000
                    share = (cus / (r['avg_ms'] * 1000) * 100) if r.get('avg_ms') else 0.0
                    out.append('| %s | %s | %.2f | %.2f | %d | %.0f | %.1f | %.1f%% |'
                               % (qn, eng, jd['programs_compiled'] / n, jd['programs_reused'] / n,
                                  jd['programs_fallback'], jd['rows_executed'] / n, cus, share))
            out.append('')

        # F. throughput scaling across thread counts
        if len(a.threads) > 1:
            out.append('## F. Throughput scaling (q/s) across thread counts')
            out.append('')
            hdr = '| query | engine | compiler |' + ''.join(' T=%d |' % t for t in a.threads) + ' T%d/T%d |' % (a.threads[-1], a.threads[0])
            out.append(hdr)
            out.append('|---|---|---|' + '---:|' * (len(a.threads) + 1))
            for qn in qnames:
                for eng in a.engines:
                    for arm in arms:
                        qps = [get(qn, eng, arm, t, 'qps') for t in a.threads]
                        if all(v is None for v in qps):
                            continue
                        out.append('| %s | %s | %s |' % (qn, eng, arm)
                                   + ''.join(' %s |' % ('%.0f' % v if v is not None else '-') for v in qps)
                                   + ' %s |' % fmt_ratio(qps[-1], qps[0]))
            out.append('')
        return '\n'.join(out)


# ----------------------------------------------------------------- main
def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--build', default='prod_build', help='build directory (default prod_build)')
    ap.add_argument('--rondb', help='rondb-cli binary (default: <build>/runtime_output_directory/rondb)')
    ap.add_argument('--sf', type=float, default=0.1, help='TPC-H scale factor for .load_tpch (default 0.1)')
    ap.add_argument('--load-threads', type=int, default=8)
    ap.add_argument('--load-batch', type=int, default=200)
    ap.add_argument('--threads', default='1,8', help='client thread counts (default 1,8)')
    ap.add_argument('--seconds', type=float, default=5.0, help='target seconds per case; requests/thread derived from a probe (default 5)')
    ap.add_argument('--requests', type=int, help='fixed requests per thread (overrides --seconds)')
    ap.add_argument('--probe-requests', type=int, default=3)
    ap.add_argument('--min-requests', type=int, default=5)
    ap.add_argument('--max-requests', type=int, default=5000)
    ap.add_argument('--queries', default='all', help='all | fs | offline_fs | tpch_cte | tpch_official | name,name,...')
    ap.add_argument('--engines', default='ronsql,mysqld,mysqld_nopush')
    ap.add_argument('--compiler', default='off,on', help='compiler arms in order (default off,on)')
    ap.add_argument('--toggle', default='auto', choices=['auto', 'set', 'restart'],
                    help='how to switch CompiledInterpreter between arms: ndb_mgm SET, cluster restart, or auto')
    ap.add_argument('--repeat', type=int, default=1,
                    help='run every case N times (interleaved); the report shows the median run per case')
    ap.add_argument('--order', default='query-major', choices=['query-major', 'arm-major'],
                    help='query-major (default): OFF and ON back to back per query (needs the runtime SET); '
                         'arm-major: all OFF cases, then all ON cases')
    ap.add_argument('--cpubind', help='cpubind.cnf (mtr --defaults-extra-file; see suite/ronsqlcrunch/cpubind.cnf)')
    ap.add_argument('--client-cpus', help='taskset CPU list for the rondb-cli benchmark client (Linux)')
    ap.add_argument('--num-cpus', type=int, help='override NumCPUs for the data nodes')
    ap.add_argument('--rondis', action='store_true', help='also start Rondis in RDRS')
    ap.add_argument('--rdrs-threads', type=int, default=64, help='RDRS REST.NumThreads (default 64)')
    ap.add_argument('--no-start', action='store_true', help='use a running cluster (give --mysql-port/--mysql-sock/--rdrs-port/--connectstring)')
    ap.add_argument('--mysql-port', type=int)
    ap.add_argument('--mysql-sock')
    ap.add_argument('--rdrs-port', type=int)
    ap.add_argument('--connectstring')
    ap.add_argument('--no-load', action='store_true', help='do not load TPC-H (already loaded)')
    ap.add_argument('--keep-cluster', action='store_true', help='leave the cluster running at the end')
    ap.add_argument('--stop', action='store_true', help='only stop the cluster of <build> (after --keep-cluster)')
    ap.add_argument('--report-only', action='store_true', help='only regenerate <out>/report.md from <out>/results.json')
    ap.add_argument('--out', default=os.path.join(REPO, 'ronsql_bench_out'))
    ap.add_argument('--quick', action='store_true', help='smoke run: --threads 1 --seconds 2')
    ap.add_argument('--verbose', action='store_true', help='stream every CLI / mtr line')
    a = ap.parse_args()
    a.build = os.path.abspath(a.build)
    a.out = os.path.abspath(a.out)
    if a.quick:
        a.threads, a.seconds = '1', 2.0
    a.threads = [int(t) for t in a.threads.split(',')]
    a.engines = [e.strip() for e in a.engines.split(',') if e.strip()]
    for e in a.engines:
        if e not in ENGINES:
            ap.error('unknown engine %s (choose from %s)' % (e, ', '.join(ENGINES)))
    a.compiler = [c.strip().lower() for c in a.compiler.split(',') if c.strip()]
    for c in a.compiler:
        if c not in ('off', 'on', 'auto'):
            ap.error('--compiler values must be off/on/auto')
    if a.no_start and not (a.mysql_port and a.mysql_sock and a.rdrs_port and a.connectstring):
        ap.error('--no-start needs --mysql-port, --mysql-sock, --rdrs-port and --connectstring')
    if a.cpubind:
        a.cpubind = os.path.abspath(a.cpubind)
        if not os.path.isfile(a.cpubind):
            ap.error('no such cpubind file: %s' % a.cpubind)
    return a


def main():
    a = parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.report_only:
        with open(os.path.join(a.out, 'results.json')) as f:
            j = json.load(f)
        meta = j['meta']
        for k in ('sf', 'threads', 'engines', 'compiler', 'cpubind', 'client_cpus', 'order', 'repeat'):
            if k in meta:
                setattr(a, k, meta[k])
        d = Driver.__new__(Driver)
        d.a, d.results, d.started = a, j['cases'], meta.get('started', '?')
        d.idle = meta.get('idle_rates', {}) or {}
        report = d.report()
        with open(os.path.join(a.out, 'report.md'), 'w') as f:
            f.write(report)
        log(report)
        return 0
    if a.stop:
        cl = Cluster(a)
        try:
            cl.discover()
        except Exception as e:
            log('cannot discover the cluster in %s: %s' % (cl.var, e))
        cl.stop()
        return 0
    return Driver(a).main()


if __name__ == '__main__':
    sys.exit(main())
