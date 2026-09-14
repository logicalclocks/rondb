#!/bin/bash
# probe_cli.sh — crash-safe RonSQL probing through ronsql_cli (RONDB-1121).
#
# Runs every statement of a SQL file (one statement per line, '#' comments
# allowed) through ronsql_cli, one process per statement, and classifies
# the outcome: OK, ERROR (permanent, exit 1), RETRY (exit 3), CRASH (killed
# by a signal, e.g. an abort in the NDB API) or OTHER.  A crash kills only
# the ronsql_cli process, so a whole list can be bisected in one run
# without restarting RDRS.  A statement can still crash a DATA NODE (F1 did):
# check `pgrep -l ndbmtd` after a run that reports node-failure errors.
#
# Usage:
#   probe_cli.sh --mtr-var <build>/mysql-test/var -f statements.sql [-D test]
#   probe_cli.sh --connect-string host:port -f statements.sql [-D test]
# The MTR form reads NDB_CONNECTSTRING from var/my.cnf of a cluster started
# with `./mtr --suite=ronsql_fs <test> --start-and-exit`.
set -u
DB=test; FILE=""; CS=""; MTRVAR=""; CLI=""
while [ $# -gt 0 ]; do
  case "$1" in
    --mtr-var) MTRVAR="$2"; shift 2;;
    --connect-string) CS="$2"; shift 2;;
    -f) FILE="$2"; shift 2;;
    -D) DB="$2"; shift 2;;
    --cli) CLI="$2"; shift 2;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done
[ -n "$FILE" ] || { echo "need -f statements.sql" >&2; exit 2; }
if [ -z "$CS" ]; then
  [ -n "$MTRVAR" ] || { echo "need --connect-string or --mtr-var" >&2; exit 2; }
  CS=$(grep -m1 '^NDB_CONNECTSTRING' "$MTRVAR/my.cnf" | sed 's/^[^=]*= *//')
  [ -n "$CS" ] || { echo "NDB_CONNECTSTRING not found in $MTRVAR/my.cnf" >&2; exit 2; }
fi
if [ -z "$CLI" ]; then
  for c in "$MTRVAR/../../runtime_output_directory/ronsql_cli" "$(dirname "$0")/../../../../../debug_build/runtime_output_directory/ronsql_cli" "$(command -v ronsql_cli)"; do
    [ -n "$c" ] && [ -x "$c" ] && CLI="$c" && break
  done
fi
[ -x "$CLI" ] || { echo "ronsql_cli not found; pass --cli <path>" >&2; exit 2; }
echo "ronsql_cli: $CLI"
echo "connect-string: $CS   database: $DB"
echo
n=0; crashes=0
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in ''|'#'*) continue;; esac
  n=$((n+1))
  out=$("$CLI" --connect-string "$CS" -D "$DB" --output-format TEXT -e "$line" 2>&1)
  rc=$?
  case $rc in
    0)   status="OK   ";;
    1)   status="ERROR";;
    3)   status="RETRY";;
    *)   if [ $rc -ge 128 ]; then status="CRASH(sig $((rc-128)))"; crashes=$((crashes+1)); else status="OTHER(rc=$rc)"; fi;;
  esac
  printf '[%2d] %-16s %s\n' "$n" "$status" "$line"
  printf '%s\n' "$out" | head -4 | sed 's/^/      | /'
done < "$FILE"
echo
echo "$n statements, $crashes crash(es)"
