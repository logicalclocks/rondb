#!/usr/bin/env python3
"""Split a data-node perf profile into where a RonSQL aggregation spends its
CPU (m3_run6_plan.md section D, census run 7 profiles).

Input, per profile NAME (the files the run 7 profile step writes):
  NAME.self.txt   perf report --no-children --sort pid,sym --stdio
                  --percent-limit 0.2, recorded with call stacks (LBR or
                  DWARF); the callee-first call chains under each entry
                  decide its category
  NAME.thr0.tsv   ndbinfo.threadstat (node_id, thr_no, thr_nm, os_tid,
  NAME.thr1.tsv   os_now, os_ru_utime, os_ru_stime) before and after the
                  perf window: CPU time and name of every data-node thread

Categories, first match on the sample's chain wins: merge (the owner's
COMPLETE merge), redist_send / redist_apply (CTE redistribution), cte_final,
cte_lookup, cte_scan, scan_agg (aggregation of scanned rows), scan, spin_idle;
kernel for kernel samples, other for the rest.  A shared helper (for example
GBHashTable::findInBucket) is split between categories in proportion to its
call-chain branches; entries without a visible chain are classified by their
own symbol.

Usage: ronsql_perf_categories.py [--hash-split] NAME [NAME ...]
  --hash-split   also show the hash-table functions split by category
"""

import collections
import re
import sys

HDR = re.compile(r'^\s+([\d.]+)%\s+(\d+):(\S+)\s+\[(.)\]\s+(.*\S)\s*$')
BRANCH = re.compile(r'--([\d.]+)%--(.*\S)\s*$')

CATEGORIES = [
    ('merge', r'JoinAggInterpreter::mergeFrom|Dblqh::continueJoinAggMerge'),
    ('redist_send', r'continueJoinAggRedistribute|sendRedistribute|flushRedistBatch|appendRedistBatch|'
                    r'initRedistBatches|sendScalarRedistributeReq'),
    ('redist_apply', r'execJOIN_AGG_REDISTRIBUTE_REQ|processRedistQueue|queueRedistGroup|continueRedistQueueDrain'),
    ('cte_final', r'execJOIN_AGG_FINAL_REP|checkCteReady|continueCteAvgFinalize|continueCteLimitFinalize|'
                  r'continueAggInterpTeardown'),
    ('cte_lookup', r'execCTE_LOOKUP_REQ|cteLookup|CteLookup|cte_lookup'),
    ('cte_scan', r'execCTE_SCAN_REQ|cteScanReqImpl|CteScan|cteScan'),
    ('scan_agg', r'JoinAggInterpreter::ProcessRec|handleJoinAggRow|processRecWithLinkedAttrs|'
                 r'AggInterpreter::ProcessRec'),
    ('scan', r'execACC_CHECK_SCAN|scanNext|next_scanconf|execTUPKEYREQ|scanTupkeyConf|execSCAN_FRAGREQ|'
             r'execSCAN_NEXTREQ|continue_next_scan_conf|interpreterStartLab|Dbtup::scan|Dbacc|Dbtux'),
    ('spin_idle', r'NdbSpin|check_yield|check_recv_yield|yield_rt|update_spin|do_sleep|epoll|futex|'
                  r'nanosleep|sched_yield|pthread_cond'),
]
CATEGORIES = [(name, re.compile(rx)) for name, rx in CATEGORIES]
LOOP_SYMBOLS = re.compile(r'mt_job_thread_main|mt_receiver_thread_main')
HASH_SYMBOLS = re.compile(r'^GBHashTable|mergeAccumulators|memcmp')


def classify(frames):
    text = '\n'.join(frames)
    for name, rx in CATEGORIES:
        if rx.search(text):
            return name
    return None


def parse(path):
    """Entries of a self-time report: pct, tid, mode, symbol, chain lines."""
    entries, cur = [], None
    for line in open(path, errors='replace'):
        m = HDR.match(line)
        if m:
            cur = {'pct': float(m.group(1)), 'tid': int(m.group(2)), 'mode': m.group(4),
                   'sym': m.group(5), 'lines': []}
            entries.append(cur)
        elif cur is not None and line.strip() and not line.startswith('#'):
            cur['lines'].append(line.rstrip('\n'))
    return entries


def branches(lines):
    """(own percentage, frames from the symbol upwards) for every node of a
    callee-first call-chain tree; a node's own share is its percentage
    minus its direct children's."""
    nodes = []          # [column of the marker, pct, frames]
    for ln in lines:
        m = BRANCH.search(ln)
        if m:
            nodes.append([ln.index('--' + m.group(1)), float(m.group(1)), [m.group(2)]])
        elif nodes:
            frame = ln.strip().lstrip('|').strip()
            if frame:
                nodes[-1][2].append(frame)
    out, stack = [], []
    for i, (col, pct, frames) in enumerate(nodes):
        while stack and stack[-1][0] >= col:
            stack.pop()
        path = [f for s in stack for f in s[2]] + frames
        stack.append((col, pct, frames))
        children = 0.0
        first_child = nodes[i + 1][0] if i + 1 < len(nodes) and nodes[i + 1][0] > col else None
        j = i + 1
        while first_child is not None and j < len(nodes) and nodes[j][0] > col:
            if nodes[j][0] == first_child:
                children += nodes[j][1]
            j += 1
        if pct - children > 0.005:
            out.append((pct - children, path))
    return out


def entry_categories(e):
    """{category: share of the entry's percentage}."""
    sym = e['sym']
    if e['mode'] == 'k' or sym.startswith('0xffffffff'):
        return {'kernel': e['pct']}
    direct = classify([sym])
    if direct is not None:
        return {direct: e['pct']}
    brs = branches(e['lines'])
    visible = sum(p for p, _ in brs)
    if visible <= 0:
        return {'spin_idle' if LOOP_SYMBOLS.search(sym) else 'other': e['pct']}
    cats = collections.defaultdict(float)
    for p, path in brs:
        cats[classify([sym] + path) or 'other'] += e['pct'] * p / visible
    return cats


def threads(before, after):
    def read(path):
        rows = {}
        for i, ln in enumerate(open(path)):
            f = ln.rstrip('\n').split('\t')
            if i == 0 or len(f) < 7:
                continue
            rows[int(f[3])] = {'node': int(f[0]), 'thr': int(f[1]), 'nm': f[2], 'now': int(f[4]),
                               'cpu': int(f[5]) + int(f[6])}
        return rows
    a, b = read(before), read(after)
    return {tid: {'node': a[tid]['node'], 'thr': a[tid]['thr'], 'nm': a[tid]['nm'],
                  'secs': (b[tid]['now'] - a[tid]['now']) / 1000.0,
                  'cpu': (b[tid]['cpu'] - a[tid]['cpu']) / 1e6}
            for tid in a if tid in b}


def report(name, hash_split):
    entries = parse(name + '.self.txt')
    total = collections.defaultdict(float)
    per_tid = collections.defaultdict(lambda: collections.defaultdict(float))
    other = collections.Counter()
    hashes = collections.defaultdict(lambda: collections.defaultdict(float))
    for e in entries:
        sym = re.sub(r'\(.*', '', e['sym']).strip()
        for cat, share in entry_categories(e).items():
            total[cat] += share
            per_tid[e['tid']][cat] += share
            if cat == 'other':
                other[sym] += share
            if HASH_SYMBOLS.search(sym):
                hashes[sym][cat] += share
    print('== %s: %d entries, %.1f%% of the samples above the report limit'
          % (name, len(entries), sum(total.values())))
    for cat, v in sorted(total.items(), key=lambda x: -x[1]):
        print('  %-13s %6.2f%%' % (cat, v))
    print('  largest "other":', ', '.join('%s %.2f' % (s, v) for s, v in other.most_common(10)))
    if hash_split:
        print('  hash-table functions by category:')
        for sym, cats in sorted(hashes.items(), key=lambda x: -sum(x[1].values())):
            print('    %-40s %5.2f  %s' % (sym[:40], sum(cats.values()), ', '.join(
                '%s %.2f' % (c, v) for c, v in sorted(cats.items(), key=lambda x: -x[1]))))
    try:
        thr = threads(name + '.thr0.tsv', name + '.thr1.tsv')
    except OSError:
        thr = {}
    if thr:
        cpu = sum(t['cpu'] for t in thr.values())
        secs = max(t['secs'] for t in thr.values())
        print('  threads: %.1f CPU-s in %.1f s = %.2f busy (spinning included):' % (cpu, secs, cpu / secs))
        for tid, t in sorted(thr.items(), key=lambda x: (x[1]['node'], x[1]['thr'])):
            top = ', '.join('%s %.1f' % (c, v) for c, v in
                            sorted(per_tid.get(tid, {}).items(), key=lambda x: -x[1])[:5])
            print('    node %d %-4s %d  tid %-7d %5.2f s = %3.0f%%  [%s]'
                  % (t['node'], t['nm'], t['thr'], tid, t['cpu'], 100 * t['cpu'] / t['secs'], top))


def main():
    args = sys.argv[1:]
    hash_split = '--hash-split' in args
    names = [a for a in args if not a.startswith('--')]
    if not names:
        sys.exit(__doc__)
    for name in names:
        report(name, hash_split)


if __name__ == '__main__':
    main()
