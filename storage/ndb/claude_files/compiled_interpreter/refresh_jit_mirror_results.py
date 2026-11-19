#!/usr/bin/env python3
"""Refresh the RONDB-1056 JIT mirror suite ndb_push_agg_jit's .result files
from its base suite after a rebase, then the RonSQL mirror suites.

The RonSQL mirror suites (ronsql_jit, ronsql_cte_jit, ronsql_fs_jit and the
topology suites ronsql_cte_ng*, ronsql_fs_ng* and ronsql_ng2r2) are generated
by mysql-test/suite/ronsql/mirrors/ronsql_mirrors.pl from
ronsql_mirrors.cfg, which also holds their recorded fallback-delta pins;
this script only runs it (--write, or --check with --check).  Use that
script directly for the RonSQL suites.

Every mirror wrapper in ndb_push_agg_jit is
   SET @<var> = ...;  --source <base test>;  SELECT ... AS <name>_jit_fallback_delta;
(the strict-arming includes emit nothing), so its .result MUST be exactly
   SET line + the base suite's .result + the SELECT block with the pin.
<var> is jit_c0 in most wrappers; some use their own name (sum64_jit_c0,
...), and it is read from the wrapper.
A rebase that changes a base test's output (renumbered ERROR_INSERT echo
lines, added queries, ...) updates the base .result but not the mirror's.
This script rebuilds every stale mirror result from the base result,
PRESERVING the recorded fallback-delta pin (byte-wise: some results carry
non-UTF-8 bytes). Native canaries (no --source) are left alone. Run from
mysql-test/. Pins may still be wrong after a base test grew — the next
run shows the census number. With --check nothing is written: the stale
mirrors are listed and the exit status is 1 if there are any.
"""
import glob, os, re, subprocess, sys

PAIRS = [('ndb_push_agg', 'ndb_push_agg_jit')]
RONSQL_MIRRORS = 'suite/ronsql/mirrors/ronsql_mirrors.pl'
SET_RE = re.compile(rb'^SET @(\w+) = \(SELECT SUM\(programs_fallback\) '
                    rb'FROM ndbinfo\.jit\);$', re.M)

def main(check):
    ok = 0; stale = []; skipped = []
    for base, jit in PAIRS:
        for w in sorted(glob.glob(f'suite/{jit}/t/*.test')):
            name = os.path.basename(w)[:-5]
            wt = open(w, 'rb').read()
            src = re.search(rb'^--source (suite/[^ \n]+\.test)$', wt, re.M)
            setv = SET_RE.search(wt)
            if not (src and setv):
                skipped.append(name); continue
            var = setv.group(1)
            baseres = src.group(1).decode().replace('/t/', '/r/')[:-5] + '.result'
            mres = f'suite/{jit}/r/{name}.result'
            if not (os.path.exists(baseres) and os.path.exists(mres)):
                skipped.append(name + ' (no result)'); continue
            b = open(baseres, 'rb').read(); m = open(mres, 'rb').read()
            if not b.endswith(b'\n'): b += b'\n'
            nb = name.encode()
            pin = re.search(nb + rb'_jit_fallback_delta\n(-?\d+)\n$', m)
            if not pin:
                skipped.append(name + ' (no pin)'); continue
            expected = (setv.group(0) + b'\n' + b +
                        b'SELECT CAST((SELECT SUM(programs_fallback) FROM '
                        b'ndbinfo.jit) - @' + var + b' AS SIGNED)\n' +
                        b'AS ' + nb +
                        b'_jit_fallback_delta;\n' + nb + b'_jit_fallback_delta\n' +
                        pin.group(1) + b'\n')
            if expected == m:
                ok += 1
            else:
                stale.append((jit, name, pin.group(1).decode()))
                if not check:
                    open(mres, 'wb').write(expected)
    verb = 'stale' if check else 'regenerated'
    print(f'consistent: {ok}  {verb}: {len(stale)}  '
          f'skipped (native canaries / no pin): {len(skipped)}')
    for jit, name, pin in stale:
        kept = '' if check else ' preserved'
        print(f'  {verb} {jit}.{name} (pin {pin}{kept})')
    return 1 if check and stale else 0

if __name__ == '__main__':
    if not os.path.isdir('suite'):
        sys.exit('run from mysql-test/')
    check = '--check' in sys.argv[1:]
    rc = main(check)
    sys.stdout.flush()
    rc_ronsql = subprocess.call(['perl', RONSQL_MIRRORS,
                                 '--check' if check else '--write'])
    sys.exit(1 if rc or rc_ronsql else 0)
