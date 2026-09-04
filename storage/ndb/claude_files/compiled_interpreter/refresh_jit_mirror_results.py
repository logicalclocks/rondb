#!/usr/bin/env python3
"""Refresh the RONDB-1056 JIT mirror suites' .result files from their base
suites after a rebase.

Every mirror wrapper in ndb_push_agg_jit / ronsql_jit / ronsql_cte_jit is
   SET @jit_c0 = ...;  --source <base test>;  SELECT ... AS <name>_jit_fallback_delta;
(the strict-arming includes emit nothing), so its .result MUST be exactly
   SET line + the base suite's .result + the SELECT block with the pin.
A rebase that changes a base test's output (renumbered ERROR_INSERT echo
lines, added queries, ...) updates the base .result but not the mirror's.
This script rebuilds every stale mirror result from the base result,
PRESERVING the recorded fallback-delta pin (byte-wise: some results carry
non-UTF-8 bytes). Native canaries (no --source) are left alone. Run from
mysql-test/. Pins may still be wrong after a base test grew — the next
run shows the census number.
"""
import glob, os, re, sys

PAIRS = [('ndb_push_agg', 'ndb_push_agg_jit'),
         ('ronsql', 'ronsql_jit'),
         ('ronsql_cte', 'ronsql_cte_jit')]
SET_LINE = b'SET @jit_c0 = (SELECT SUM(programs_fallback) FROM ndbinfo.jit);\n'
SEL_LINE = (b'SELECT CAST((SELECT SUM(programs_fallback) FROM ndbinfo.jit) '
            b'- @jit_c0 AS SIGNED)\n')

def main():
    ok = 0; stale = []; skipped = []
    for base, jit in PAIRS:
        for w in sorted(glob.glob(f'suite/{jit}/t/*.test')):
            name = os.path.basename(w)[:-5]
            src = re.search(rb'^--source (suite/[^ \n]+\.test)$',
                            open(w, 'rb').read(), re.M)
            if not src:
                skipped.append(name); continue
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
            expected = (SET_LINE + b + SEL_LINE + b'AS ' + nb +
                        b'_jit_fallback_delta;\n' + nb + b'_jit_fallback_delta\n' +
                        pin.group(1) + b'\n')
            if expected == m:
                ok += 1
            else:
                stale.append((jit, name, pin.group(1).decode()))
                open(mres, 'wb').write(expected)
    print(f'consistent: {ok}  regenerated: {len(stale)}  '
          f'skipped (native canaries / no pin): {len(skipped)}')
    for jit, name, pin in stale:
        print(f'  regenerated {jit}.{name} (pin {pin} preserved)')
    return 0

if __name__ == '__main__':
    if not os.path.isdir('suite'):
        sys.exit('run from mysql-test/')
    sys.exit(main())
