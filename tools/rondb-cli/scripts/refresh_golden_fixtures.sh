#!/usr/bin/env bash
# refresh_golden_fixtures.sh — regenerate the captured Hopsworks golden
# corpus (RONDB-1121 E8) with the Java exporter HopsworksGoldenDump and
# stage it for review.
#
# The corpus in internal/fsq/testdata/hopsworks_golden/ is the contract the
# Go emitter port conforms to (phase_e2.md §1).  It is produced ONLY by the
# real Hopsworks builder; never edit fixtures by hand and never synthesize
# expected JSON in Go.
#
# Usage:
#   scripts/refresh_golden_fixtures.sh [--tree /path/to/hopsworks] [--input DIR] [--apply]
#
#   --tree   Hopsworks checkout holding HopsworksGoldenDump.java
#            (default: $HOPSWORKS_TREE or /Users/mikael/github/hopsworks_ronsql)
#   --input  optional case directory (-Dhopsworks.golden.input): replaces the
#            built-in corpus, e.g. to add cases (HopsworksGoldenDump.md)
#   --apply  copy the new corpus over internal/fsq/testdata/hopsworks_golden
#            after review; without it the script only exports and diffs.
#
# Steps: 1) mvn runs the exporter into a fresh directory; 2) the new
# manifest is diffed against the imported one (provenance, file set and
# SHA-256s); 3) with --apply the corpus is replaced and `go test
# ./internal/fsq/emit/` runs the conformance test.  Bump emit.HopsworksRef
# deliberately when the reference Hopsworks commit changes (phase_e2.md).
set -euo pipefail

here="$(cd "$(dirname "$0")/.." && pwd)"
corpus="$here/internal/fsq/testdata/hopsworks_golden"
tree="${HOPSWORKS_TREE:-/Users/mikael/github/hopsworks_ronsql}"
input=""
apply=0
while [ $# -gt 0 ]; do
  case "$1" in
    --tree) tree="$2"; shift 2 ;;
    --input) input="$2"; shift 2 ;;
    --apply) apply=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -f "$tree/hopsworks-api/src/test/java/io/hops/hopsworks/api/featurestore/trainingdataset/HopsworksGoldenDump.java" ] \
  || { echo "HopsworksGoldenDump.java not found under $tree (use --tree)" >&2; exit 2; }

out="$(mktemp -d /tmp/hopsworks-golden.XXXXXX)"; rmdir "$out"   # the exporter insists on a new directory
echo "== exporting with the Hopsworks builder at $(git -C "$tree" rev-parse --short HEAD) into $out"
mvn_args=(-pl hopsworks-api -am -Dtest=HopsworksGoldenDump -Dsurefire.failIfNoSpecifiedTests=false
          -Duser.timezone=UTC "-Dhopsworks.golden.output=$out" test)
[ -n "$input" ] && mvn_args+=("-Dhopsworks.golden.input=$input")
( cd "$tree" && mvn "${mvn_args[@]}" )
[ -f "$out/manifest.json" ] || { echo "export produced no manifest" >&2; exit 1; }

echo "== provenance"
python3 - "$corpus/manifest.json" "$out/manifest.json" <<'PY'
import json, sys
old, new = (json.load(open(p)) for p in sys.argv[1:3])
for k in sorted(set(old["provenance"]) | set(new["provenance"])):
    a, b = old["provenance"].get(k), new["provenance"].get(k)
    print(f"  {k:24s} {'same   ' if a == b else 'CHANGED'} {a!r} -> {b!r}" if a != b else f"  {k:24s} same    {a!r}")
of, nf = old["files"], new["files"]
print("== files: %d imported, %d exported" % (len(of), len(nf)))
for f in sorted(set(of) | set(nf)):
    if f not in nf: print("  removed ", f)
    elif f not in of: print("  added   ", f)
    elif of[f] != nf[f]: print("  changed ", f)
PY

if [ "$apply" -eq 1 ]; then
  echo "== applying $out -> $corpus"
  rm -f "$corpus"/*.json
  cp "$out"/*.json "$corpus"/
  ( cd "$here" && go test ./internal/fsq/emit/ )
  echo "== applied; review 'git diff --stat internal/fsq/testdata/hopsworks_golden' and update emit.HopsworksRef if the commit moved"
else
  echo "== dry run: review $out, then re-run with --apply"
fi
