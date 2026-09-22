#!/usr/bin/env python3
"""Triage a ronsql_bench_matrix.py run: which queries need performance work.

Reads one <run>/results.json (or the run directory) and prints a Markdown
report that ranks every RonSQL-capable query by how far it is from the
MySQL server on the same statement, at the lowest thread count (latency)
and at the highest (throughput), attributes RonSQL time to its server-side
phases, reports the compiled-interpreter effect, and — with --baseline —
flags regressions against an earlier run of the same matrix.

    python3 ronsql_bench_triage.py /tmp/fs_hw_run4 \
        --baseline storage/ndb/claude_files/fs_ronsql/bench_results/2026-09-11-prod_build-run2 \
        --out /tmp/fs_hw_run4/triage.md

Classes (ratio = RonSQL / MySQL for latency, MySQL / RonSQL for q/s; both
> 1 mean RonSQL is behind):

    PARITY    ratio <= --parity (default 1.25)
    SLOW      ratio <= --slow   (default 3.0)
    CRITICAL  ratio >  --slow
    FAIL      the RonSQL case failed (rejection, error, crash)

Regression rule (benchmarks.md §8): a case regresses when avg worsens by
more than --regress-avg (15 %) or p99 by more than --regress-p99 (25 %)
against the baseline case with the same query, engine, compiler arm and
thread count.  A plan-pin warning in the case log is reported next to it
so a plan change can be told from a code regression.
"""
import argparse
import glob
import json
import os
import re
import sys

PHASE_COLS = ['parse', 'analyze', 'load', 'plan', 'compile', 'ndbprep', 'send',
              'firstbatch', 'drain', 'print']
MYSQL_ENGINES = ['mysqld_nopush', 'mysqld']
RE_PIN = re.compile(r'plan pin', re.IGNORECASE)


def fmt_ms(v):
    if v is None:
        return '-'
    if v < 1.0:
        return '%.0fus' % (v * 1000)
    if v < 1000.0:
        return '%.2fms' % v
    return '%.2fs' % (v / 1000)


def fmt_ratio(v):
    return '-' if v is None else '%.2fx' % v


def fmt_qps(c):
    if not c or not c.get('ok') or c.get('qps') is None:
        return '-'
    return ('%.1f' if c['qps'] < 10 else '%.0f') % c['qps']


def fmt_pct(v):
    return '-' if v is None else '%+.0f%%' % (v * 100)


def ratio(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


class Run:
    def __init__(self, path):
        if os.path.isdir(path):
            self.dir = path
            path = os.path.join(path, 'results.json')
        else:
            self.dir = os.path.dirname(os.path.abspath(path))
        with open(path) as f:
            d = json.load(f)
        self.meta = d.get('meta', {})
        self.cases = d.get('cases', [])
        self.threads = sorted({c['threads'] for c in self.cases})
        self.engines = []
        self.arms = []
        self.queries = []
        for c in self.cases:
            if c['engine'] not in self.engines:
                self.engines.append(c['engine'])
            if c.get('compiler') and c['compiler'] not in self.arms:
                self.arms.append(c['compiler'])
            if c['query'] not in self.queries:
                self.queries.append(c['query'])

    def find(self, query, engine, arm, threads):
        """Representative case: the ok run with the median avg latency
        (matches ronsql_bench_matrix.py's report), else a failed one."""
        hits = [c for c in self.cases
                if c['query'] == query and c['engine'] == engine
                and c.get('compiler') == arm and c['threads'] == threads]
        ok = sorted([c for c in hits if c['ok'] and c.get('avg_ms') is not None],
                    key=lambda c: c['avg_ms'])
        if ok:
            return ok[len(ok) // 2]
        return hits[0] if hits else None

    def pin_warnings(self, case):
        """Plan-pin lines from the raw CLI log of a case (cases/<tag>.txt)."""
        tag = case.get('tag')
        if not tag:
            return []
        for p in glob.glob(os.path.join(self.dir, 'cases', tag + '.txt')):
            try:
                with open(p, errors='replace') as f:
                    return [l.strip() for l in f if RE_PIN.search(l)]
            except OSError:
                return []
        return []


def classify(r, parity, slow):
    if r is None:
        return 'N/A'
    if r <= parity:
        return 'PARITY'
    if r <= slow:
        return 'SLOW'
    return 'CRITICAL'


def top_phase(case):
    ph = case.get('phases') or {}
    best = None
    for k in PHASE_COLS:
        v = ph.get(k, {}).get('avg_ms')
        if v is not None and (best is None or v > best[1]):
            best = (k, v)
    return best


def triage(run, base, a):
    mysql_engine = next((e for e in a.mysql_engines if e in run.engines), None)
    arm = a.arm if a.arm in run.arms else (run.arms[0] if run.arms else None)
    other_arm = next((x for x in run.arms if x != arm), None)
    t1, tmax = run.threads[0], run.threads[-1]
    rows = []
    for qn in run.queries:
        rs1 = run.find(qn, 'ronsql', arm, t1)
        if rs1 is None:
            continue  # MySQL-only entry (twins, official TPC-H)
        row = {'query': qn, 'ronsql': rs1}
        ms1 = run.find(qn, mysql_engine, arm, t1) if mysql_engine else None
        row['mysql'] = ms1
        if not rs1['ok']:
            row['lat_class'] = 'FAIL'
            row['lat_ratio'] = None
            row['fail'] = rs1.get('error')
        else:
            row['lat_ratio'] = ratio(rs1.get('avg_ms'), ms1.get('avg_ms') if ms1 and ms1['ok'] else None)
            row['lat_class'] = classify(row['lat_ratio'], a.parity, a.slow)
        # throughput at the highest thread count
        rsm = run.find(qn, 'ronsql', arm, tmax) if tmax != t1 else None
        msm = run.find(qn, mysql_engine, arm, tmax) if (mysql_engine and tmax != t1) else None
        row['ronsql_max'], row['mysql_max'] = rsm, msm
        if rsm and rsm['ok']:
            row['tput_ratio'] = ratio(msm.get('qps') if msm and msm['ok'] else None, rsm.get('qps'))
            row['tput_class'] = classify(row['tput_ratio'], a.parity, a.slow)
            row['ronsql_scale'] = ratio(rsm.get('qps'), rs1.get('qps'))
            row['mysql_scale'] = ratio(msm.get('qps') if msm and msm['ok'] else None,
                                       ms1.get('qps') if ms1 and ms1['ok'] else None)
        else:
            row['tput_ratio'], row['tput_class'] = None, ('FAIL' if rsm else 'N/A')
            row['ronsql_scale'] = row['mysql_scale'] = None
        # phases
        if rs1['ok']:
            ph = rs1.get('phases') or {}
            ex = ph.get('execute', {}).get('avg_ms')
            row['execute_share'] = ratio(ex, rs1.get('avg_ms'))
            row['top_phase'] = top_phase(rs1)
            row['client_overhead'] = rs1.get('client_overhead_ms')
            row['rows'] = rs1.get('rows_drained')
        else:
            row['execute_share'] = row['top_phase'] = row['client_overhead'] = row['rows'] = None
        # compiled interpreter effect (arm vs the other arm, T1)
        row['jit_ratio'] = None
        if other_arm and rs1['ok']:
            ro = run.find(qn, 'ronsql', other_arm, t1)
            if ro and ro['ok']:
                # OFF / ON  (> 1 = the compiled arm is faster)
                off, on = (rs1, ro) if arm == 'OFF' else (ro, rs1)
                row['jit_ratio'] = ratio(off.get('avg_ms'), on.get('avg_ms'))
        # plan pins
        row['pins'] = run.pin_warnings(rs1)
        # baseline
        row['base'] = []
        if base:
            for eng in ('ronsql', mysql_engine):
                if not eng:
                    continue
                for th in run.threads:
                    for ar in run.arms:
                        cur = run.find(qn, eng, ar, th)
                        old = base.find(qn, eng, ar, th)
                        if not (cur and old and cur['ok'] and old['ok']):
                            continue
                        d_avg = ratio(cur.get('avg_ms'), old.get('avg_ms'))
                        d_p99 = ratio(cur.get('p99_ms'), old.get('p99_ms'))
                        if d_avg is None:
                            continue
                        verdict = 'same'
                        if d_avg - 1 > a.regress_avg or (d_p99 is not None and d_p99 - 1 > a.regress_p99):
                            verdict = 'REGRESSION'
                        elif 1 - d_avg > a.regress_avg:
                            verdict = 'IMPROVED'
                        row['base'].append({'engine': eng, 'threads': th, 'arm': ar,
                                            'avg_old': old.get('avg_ms'), 'avg_new': cur.get('avg_ms'),
                                            'd_avg': d_avg - 1, 'd_p99': (d_p99 - 1) if d_p99 is not None else None,
                                            'verdict': verdict})
        rows.append(row)
    return rows, mysql_engine, arm, other_arm, t1, tmax


def severity(row):
    """Sort key: failures first, then the worst of the two ratios."""
    if row['lat_class'] == 'FAIL':
        return (3, 0.0)
    worst = max(row['lat_ratio'] or 0.0, row['tput_ratio'] or 0.0)
    reg = any(b['verdict'] == 'REGRESSION' for b in row['base'])
    cls = {'CRITICAL': 2, 'SLOW': 1}.get(row['lat_class'], 0)
    cls = max(cls, {'CRITICAL': 2, 'SLOW': 1}.get(row['tput_class'], 0))
    return (cls if cls else (1 if reg else 0), worst)


def report(run, base, a):
    rows, meng, arm, other_arm, t1, tmax = triage(run, base, a)
    out = []
    m = run.meta
    out.append('# RonSQL performance triage')
    out.append('')
    out.append('run: build=%s sf=%s threads=%s engines=%s arms=%s%s' % (
        m.get('build', '?'), m.get('sf', '?'), run.threads, ','.join(run.engines), ','.join(run.arms),
        (' baseline=%s' % a.baseline) if base else ''))
    out.append('')
    out.append('MySQL reference engine: `%s`; compiler arm for the engine comparison: `%s`. '
               'Latency ratio = RonSQL avg / MySQL avg at T=%d; throughput ratio = MySQL q/s / RonSQL q/s at T=%d '
               '(both > 1 = RonSQL behind). Classes: PARITY <= %.2fx, SLOW <= %.2fx, CRITICAL above; '
               'FAIL = the RonSQL case did not run.' % (meng, arm, t1, tmax, a.parity, a.slow))
    out.append('')

    # 1. needs-work list
    ranked = sorted(rows, key=severity, reverse=True)
    needs = [r for r in ranked if r['lat_class'] in ('FAIL', 'SLOW', 'CRITICAL')
             or r['tput_class'] in ('SLOW', 'CRITICAL')
             or any(b['verdict'] == 'REGRESSION' for b in r['base'])]
    out.append('## 1. Needs work (%d of %d RonSQL-capable queries)' % (len(needs), len(rows)))
    out.append('')
    if needs:
        out.append('| # | query | latency T=%d | throughput T=%d | RonSQL / MySQL avg | RonSQL q/s / MySQL q/s | top phase | rows/req | plan pins | baseline |'
                   % (t1, tmax))
        out.append('|---:|---|---|---|---:|---:|---|---:|---|---|')
        for i, r in enumerate(needs, 1):
            rs, ms = r['ronsql'], r['mysql']
            rsm, msm = r.get('ronsql_max'), r.get('mysql_max')
            lat = '%s (%s)' % (r['lat_class'], fmt_ratio(r['lat_ratio'])) if r['lat_class'] != 'FAIL' else 'FAIL: %s' % (r.get('fail') or '')[:80]
            tput = '%s (%s)' % (r['tput_class'], fmt_ratio(r['tput_ratio'])) if r['tput_class'] not in ('N/A', 'FAIL') else r['tput_class']
            avgs = '%s / %s' % (fmt_ms(rs.get('avg_ms')), fmt_ms(ms.get('avg_ms')) if ms and ms['ok'] else '-')
            qps = '%s / %s' % (fmt_qps(rsm), fmt_qps(msm))
            tp = '%s %s' % (r['top_phase'][0], fmt_ms(r['top_phase'][1])) if r['top_phase'] else '-'
            pins = '; '.join(r['pins'])[:80] if r['pins'] else '-'
            regs = [b for b in r['base'] if b['verdict'] != 'same']
            bl = '; '.join('%s %s T%d %s avg %s p99 %s' % (b['verdict'], b['engine'], b['threads'], b['arm'],
                                                            fmt_pct(b['d_avg']), fmt_pct(b['d_p99']))
                           for b in regs) if regs else ('same' if r['base'] else '-')
            out.append('| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s |'
                       % (i, r['query'], lat, tput, avgs, qps, tp,
                          ('%.1f' % r['rows']) if r['rows'] is not None else '-', pins, bl))
    else:
        out.append('Nothing: every RonSQL-capable query is at parity with the MySQL server and no case regressed.')
    out.append('')

    # 2. full latency + phases table
    out.append('## 2. Every RonSQL-capable query at T=%d, arm %s' % (t1, arm))
    out.append('')
    out.append('| query | class | RonSQL avg | p99 | MySQL avg | ratio | execute share | http+client | top phase | rows/req | OFF/ON |')
    out.append('|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|')
    for r in sorted(rows, key=lambda r: r['query']):
        rs, ms = r['ronsql'], r['mysql']
        out.append('| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
            r['query'], r['lat_class'], fmt_ms(rs.get('avg_ms')), fmt_ms(rs.get('p99_ms')),
            fmt_ms(ms.get('avg_ms')) if ms and ms['ok'] else '-', fmt_ratio(r['lat_ratio']),
            ('%.0f%%' % (r['execute_share'] * 100)) if r['execute_share'] is not None else '-',
            fmt_ms(r['client_overhead']),
            ('%s %s' % (r['top_phase'][0], fmt_ms(r['top_phase'][1]))) if r['top_phase'] else '-',
            ('%.1f' % r['rows']) if r['rows'] is not None else '-',
            fmt_ratio(r['jit_ratio'])))
    out.append('')

    # 3. throughput table
    if tmax != t1:
        out.append('## 3. Throughput at T=%d (q/s) and scaling T%d/T%d' % (tmax, tmax, t1))
        out.append('')
        out.append('| query | class | RonSQL q/s | MySQL q/s | MySQL/RonSQL | RonSQL scale | MySQL scale | RonSQL avg @T%d | p99 @T%d |' % (tmax, tmax))
        out.append('|---|---|---:|---:|---:|---:|---:|---:|---:|')
        for r in sorted(rows, key=lambda r: r['query']):
            rsm, msm = r.get('ronsql_max'), r.get('mysql_max')
            out.append('| %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
                r['query'], r['tput_class'], fmt_qps(rsm), fmt_qps(msm),
                fmt_ratio(r['tput_ratio']), fmt_ratio(r['ronsql_scale']), fmt_ratio(r['mysql_scale']),
                fmt_ms(rsm.get('avg_ms')) if rsm and rsm['ok'] else '-',
                fmt_ms(rsm.get('p99_ms')) if rsm and rsm['ok'] else '-'))
        out.append('')

    # 4. compiled interpreter
    if other_arm:
        jit = [r for r in rows if r['jit_ratio'] is not None]
        wins = [r for r in jit if r['jit_ratio'] >= 1 + a.jit_noise]
        losses = [r for r in jit if r['jit_ratio'] <= 1 - a.jit_noise]
        out.append('## 4. Compiled interpreter (OFF avg / ON avg at T=%d; > 1 = ON faster; noise band ±%.0f%%)'
                   % (t1, a.jit_noise * 100))
        out.append('')
        out.append('faster with ON: %s' % (', '.join('%s %s' % (r['query'], fmt_ratio(r['jit_ratio'])) for r in wins) or 'none'))
        out.append('')
        out.append('slower with ON: %s' % (', '.join('%s %s' % (r['query'], fmt_ratio(r['jit_ratio'])) for r in losses) or 'none'))
        out.append('')

    # 5. baseline
    if base:
        out.append('## 5. Against the baseline (%s): avg > +%.0f%% or p99 > +%.0f%% = REGRESSION, avg < -%.0f%% = IMPROVED'
                   % (a.baseline, a.regress_avg * 100, a.regress_p99 * 100, a.regress_avg * 100))
        out.append('')
        out.append('| query | engine | threads | arm | baseline avg | now avg | avg | p99 | verdict |')
        out.append('|---|---|---:|---|---:|---:|---:|---:|---|')
        n = 0
        for r in sorted(rows, key=lambda r: r['query']):
            for b in r['base']:
                if b['verdict'] == 'same' and not a.all_baseline:
                    continue
                n += 1
                out.append('| %s | %s | %d | %s | %s | %s | %s | %s | %s |' % (
                    r['query'], b['engine'], b['threads'], b['arm'], fmt_ms(b['avg_old']), fmt_ms(b['avg_new']),
                    fmt_pct(b['d_avg']), fmt_pct(b['d_p99']), b['verdict']))
        if not n:
            out.append('| (no case moved beyond the noise band) | | | | | | | | |')
        out.append('')

    counts = {}
    for r in rows:
        counts[r['lat_class']] = counts.get(r['lat_class'], 0) + 1
    regs = sum(1 for r in rows for b in r['base'] if b['verdict'] == 'REGRESSION')
    out.append('SUMMARY queries=%d %s regressions=%d' % (
        len(rows), ' '.join('%s=%d' % (k.lower(), counts[k]) for k in ('FAIL', 'CRITICAL', 'SLOW', 'PARITY', 'N/A') if k in counts), regs))
    return '\n'.join(out) + '\n', rows


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('run', help='<out> directory of ronsql_bench_matrix.py or its results.json')
    ap.add_argument('--baseline', help='an earlier run (directory or results.json) for the regression rule')
    ap.add_argument('--mysql-engines', default=','.join(MYSQL_ENGINES),
                    help='MySQL reference engines in order of preference (default %s)' % ','.join(MYSQL_ENGINES))
    ap.add_argument('--arm', default='OFF', help='compiler arm for the engine comparison (default OFF)')
    ap.add_argument('--parity', type=float, default=1.25, help='PARITY up to this ratio (default 1.25)')
    ap.add_argument('--slow', type=float, default=3.0, help='SLOW up to this ratio, CRITICAL above (default 3.0)')
    ap.add_argument('--regress-avg', type=float, default=0.15, help='avg regression threshold (default 0.15)')
    ap.add_argument('--regress-p99', type=float, default=0.25, help='p99 regression threshold (default 0.25)')
    ap.add_argument('--jit-noise', type=float, default=0.10, help='OFF/ON noise band (default 0.10)')
    ap.add_argument('--all-baseline', action='store_true', help='list every baseline comparison, not only the movers')
    ap.add_argument('--out', help='write the Markdown report here (default: stdout only)')
    ap.add_argument('--json', help='write the per-query triage rows as JSON here')
    a = ap.parse_args()
    a.mysql_engines = [e for e in a.mysql_engines.split(',') if e]
    return a


def main():
    a = parse_args()
    run = Run(a.run)
    base = Run(a.baseline) if a.baseline else None
    text, rows = report(run, base, a)
    sys.stdout.write(text)
    if a.out:
        with open(a.out, 'w') as f:
            f.write(text)
    if a.json:
        slim = []
        for r in rows:
            slim.append({k: v for k, v in r.items() if k not in ('ronsql', 'mysql', 'ronsql_max', 'mysql_max')})
        with open(a.json, 'w') as f:
            json.dump(slim, f, indent=1, default=str)
    return 0


if __name__ == '__main__':
    sys.exit(main())
