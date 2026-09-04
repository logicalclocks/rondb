#!/usr/bin/env python3
"""RONDB-1056 — interpreter vs compiled-interpreter benchmark driver.

Runs mysql-test/suite/ndb_push_agg/include/jit_bench_body.inc under both
arms — ndb_push_agg (CompiledInterpreter=OFF) and ndb_push_agg_jit (ON) —
via mtr, collects the bench binaries' per-iteration timings from
$MYSQL_TMP_DIR/jit_bench/*.txt after each run, and prints the OFF-vs-ON
table (median over iterations, the first iteration dropped as warm-up
when there are more than two). Results are also written as Markdown.

    jit_bench_off_vs_on.py [--build debug_build] [--sf 0.05] [--iters 5]
                           [--orders 20000] [--out jit_bench_out]
                           [--arms off,on] [--no-run]

--no-run only re-parses a previous run's collected files.
"""
import argparse, os, re, shutil, statistics, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, '..', '..', '..', '..'))
ARMS = {'off': 'ndb_push_agg', 'on': 'ndb_push_agg_jit'}

RE_ITER  = re.compile(r'^Iteration (\d+): (PASS|FAIL)')
RE_TOTAL = re.compile(r'^\s*Total:\s+([\d.]+) ms')
RE_SCAN  = re.compile(r'^\s*Scan\+Join:\s+([\d.]+) ms')
RE_SQL   = re.compile(r'^\s*SQL query:\s+([\d.]+) ms')
RE_PASSED = re.compile(r'^(PASSED|FAILED)\s*$')
RE_ELAPSED = re.compile(r'^elapsed_us=(\d+)')

def run_arm(arm, build, sf, iters, orders, out):
    mtr_dir = os.path.join(build, 'mysql-test')
    env = dict(os.environ, JIT_BENCH_SF=str(sf), JIT_BENCH_ITERS=str(iters),
               JIT_BENCH_ORDERS=str(orders))
    cmd = ['./mtr', '--parallel=1', f'{ARMS[arm]}.jit_bench']
    print(f'== {arm.upper()} arm: {" ".join(cmd)}  (sf={sf} iters={iters} orders={orders})',
          flush=True)
    rc = subprocess.call(cmd, cwd=mtr_dir, env=env)
    src = os.path.join(mtr_dir, 'var', 'tmp', 'jit_bench')
    dst = os.path.join(out, arm)
    if os.path.isdir(dst):
        shutil.rmtree(dst)
    if os.path.isdir(src):
        shutil.copytree(src, dst)
        print(f'   collected {len(os.listdir(dst))} files -> {dst}')
    else:
        print(f'   WARNING: no {src} — the bench did not run', file=sys.stderr)
    if rc != 0:
        print(f'   WARNING: mtr exit {rc} for the {arm} arm', file=sys.stderr)
    return rc

def parse_bench(path):
    """-> dict(iters=[(n, ok, total, scan, sql)], passed=bool)"""
    iters = []
    cur = None
    passed = None
    for line in open(path, 'rb').read().decode('utf-8', 'replace').splitlines():
        m = RE_ELAPSED.match(line)          # sql_agg_* files: one line per run
        if m:
            ms = int(m.group(1)) / 1000.0
            iters.append({'n': len(iters) + 1, 'ok': True, 'total': ms,
                          'scan': ms, 'sql': None})
            passed = True
            continue
        m = RE_ITER.match(line)
        if m:
            cur = {'n': int(m.group(1)), 'ok': m.group(2) == 'PASS',
                   'total': None, 'scan': None, 'sql': None}
            iters.append(cur); continue
        if cur is None: 
            m = RE_PASSED.match(line)
            if m: passed = (m.group(1) == 'PASSED')
            continue
        for key, rx in (('total', RE_TOTAL), ('scan', RE_SCAN), ('sql', RE_SQL)):
            m = rx.match(line)
            if m: cur[key] = float(m.group(1))
        m = RE_PASSED.match(line)
        if m: passed = (m.group(1) == 'PASSED')
    return {'iters': iters, 'passed': passed}

def median(vals):
    vals = [v for v in vals if v is not None]
    if not vals: return None
    if len(vals) > 2: vals = vals[1:]          # drop the warm-up iteration
    return statistics.median(vals)

def best(vals):
    vals = [v for v in vals if v is not None]
    return min(vals) if vals else None

def parse_ndbinfo(path):
    """SELECT * FROM ndbinfo.jit output (tab-separated, header row) ->
    {column: sum over rows} for every numeric column."""
    if not os.path.exists(path): return {}
    lines = open(path, 'rb').read().decode('utf-8', 'replace').splitlines()
    if len(lines) < 2: return {}
    cols = lines[0].split('\t')
    sums = {}
    for row in lines[1:]:
        for c, v in zip(cols, row.split('\t')):
            try:
                sums[c] = sums.get(c, 0) + int(v)
            except ValueError:
                pass
    return sums

def ndbinfo_delta(out, arm):
    b = parse_ndbinfo(os.path.join(out, arm, 'ndbinfo_jit_before.txt'))
    a = parse_ndbinfo(os.path.join(out, arm, 'ndbinfo_jit_after.txt'))
    return {c: a[c] - b.get(c, 0) for c in a}

def fmt(v):
    return f'{v:9.2f}' if v is not None else '        -'

def speedup(a, b):
    return f'{a / b:5.2f}x' if (a and b) else '     -'

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--build', default=os.path.join(REPO, 'debug_build'))
    ap.add_argument('--sf', default='0.05')
    ap.add_argument('--iters', type=int, default=7)
    ap.add_argument('--orders', type=int, default=20000)
    ap.add_argument('--out', default=os.path.join(REPO, 'jit_bench_out'))
    ap.add_argument('--arms', default='off,on')
    ap.add_argument('--no-run', action='store_true')
    a = ap.parse_args()
    arms = [x.strip() for x in a.arms.split(',') if x.strip()]
    os.makedirs(a.out, exist_ok=True)
    if not a.no_run:
        for arm in arms:
            run_arm(arm, a.build, a.sf, a.iters, a.orders, a.out)

    data = {}
    for arm in arms:
        d = os.path.join(a.out, arm)
        data[arm] = {}
        if not os.path.isdir(d): continue
        for f in sorted(os.listdir(d)):
            if not f.endswith('.txt') or f == 'load_tpch.txt' or f.startswith('ndbinfo_'): continue
            data[arm][f[:-4]] = parse_bench(os.path.join(d, f))

    benches = sorted(set().union(*[set(v) for v in data.values()]))
    lines = []
    hdr = (f'{"bench":24s} {"it":>3s} | {"Scan+Join (data-node work), ms":^44s} | {"SQL query (mysqld), ms":^33s}\n'
           f'{"(sql_agg_*: pushed-down SQL aggregation over lineitem, whole query, no join)":s}')
    sub = (f'{"":24s} {"":>3s} | {"OFF med":>9s} {"ON med":>9s} {"speed":>6s} {"OFF min":>7s} {"ON min":>7s} {"speed":>6s} | '
           f'{"OFF med":>9s} {"ON med":>9s} {"speed":>6s}')
    lines.append(hdr); lines.append(sub); lines.append('-' * len(sub))
    bad = []
    for b in benches:
        off = data.get('off', {}).get(b); on = data.get('on', {}).get(b)
        def m(x, k): return median([i[k] for i in x['iters']]) if x else None
        def bst(x, k): return best([i[k] for i in x['iters']]) if x else None
        n = max(len(off['iters']) if off else 0, len(on['iters']) if on else 0)
        for arm, x in (('off', off), ('on', on)):
            if x and (x['passed'] is False or any(not i['ok'] for i in x['iters'])):
                bad.append(f'{b} ({arm})')
        def f7(v): return f'{v:7.1f}' if v is not None else '      -'
        lines.append(f'{b:24s} {n:3d} | {fmt(m(off,"scan"))} {fmt(m(on,"scan"))} {speedup(m(off,"scan"), m(on,"scan"))} '
                     f'{f7(bst(off,"scan"))} {f7(bst(on,"scan"))} {speedup(bst(off,"scan"), bst(on,"scan"))} | '
                     f'{fmt(m(off,"sql"))} {fmt(m(on,"sql"))} {speedup(m(off,"sql"), m(on,"sql"))}')
    lines.append('')
    lines.append('med = median over iterations (first iteration dropped when > 2); min = best iteration; speed = OFF / ON.')
    lines.append(f'sf={a.sf} iters={a.iters} orders={a.orders}  build={a.build}')
    for arm in arms:
        d = ndbinfo_delta(a.out, arm)
        keys = [k for k in ('programs_compiled', 'programs_reused', 'programs_fallback',
                            'rows_executed') if k in d]
        others = [k for k in d if k not in keys]
        if d:
            lines.append(f'ndbinfo.jit delta, {arm.upper()} arm: ' +
                         '  '.join(f'{k}={d[k]}' for k in keys + others))
        else:
            lines.append(f'ndbinfo.jit delta, {arm.upper()} arm: (not captured)')
    if bad:
        lines.append('FAILED benches: ' + ', '.join(bad))
    text = '\n'.join(lines)
    print(text)
    md = os.path.join(a.out, 'summary.md')
    with open(md, 'w') as fh:
        fh.write('```\n' + text + '\n```\n')
    print(f'\n(written to {md})')
    return 1 if bad else 0

if __name__ == '__main__':
    sys.exit(main())
