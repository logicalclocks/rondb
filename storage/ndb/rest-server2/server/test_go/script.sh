#!/usr/bin/env bash
set -euo pipefail

# Show help by `./script.sh help` or just `./script.sh`

cmd-help() {
  [ $# == 0 ] || die "Command $cmd does not take arguments."
  cat <<"EOF"
# Before using this script, start a cluster and export a variable pointing to
# the rdrs2 config location:
...rondb/build$ ./mysql-test/mtr --suite rdrs2 --start-and-exit
...rondb/build$ export \
  RDRS_CONFIG_FILE=`realpath mysql-test/var/rdrs.1.1_config.json`

# Available commands:

$ ./script.sh help
#    Show this help

$ ./script.sh test [PACKAGE]
#    Run a test case.
#    PACKAGE default: hopsworks.ai/rdrs2/internal/integrationtests/batchpkread
#    To see available packages, run `./script.sh list`

$ ./script.sh bench [PACKAGE] [DURATION]
#    Run a benchmark.
#    PACKAGE default: hopsworks.ai/rdrs2/internal/integrationtests/batchpkread
#    DURATION default: 30
#    To see available packages, run `./script.sh list`

$ ./script.sh list
#   Show packages that can be used with `./script test` and `./script bench`.

$ ./script.sh restart
#    Restart rdrs2 without restarting the entire cluster. This is useful for
#    quick iteration. Note that stdout/stderr will be discarded.

EOF
}

cmd-test() {
  [ -f "${RDRS_CONFIG_FILE}" ] || die 'RDRS_CONFIG_FILE must be set and point to an existing file.'
  TEST_PACKAGE=hopsworks.ai/rdrs2/internal/integrationtests/batchpkread
  case $# in
    0) ;;
    1) TEST_PACKAGE="$1" ;;
    *) die "Too many arguments" ;;
  esac

  # Run Golang test, capture exit code and stdout.
  # * `-p 1` ensures that only one test is run in parallel by the `go test`
  #   process. This is necessary since some of these tests change the database
  #   state.
  # * `-count 1` is the idiomatic way to prevent `go test` from using cached
  #   test runs. This is necessary since the `go test` logic doesn't look into
  #   (all) C++ sources to detect changes, meaning cache invalidation of test
  #   runs wouldn't work correctly.
  go test -v \
     -p 1 \
     -count 1 \
     $TEST_PACKAGE
}

cmd-bench() {
  [ -f "${RDRS_CONFIG_FILE}" ] || die 'RDRS_CONFIG_FILE must be set and point to an existing file.'
  BENCHMARK_PACKAGE=hopsworks.ai/rdrs2/internal/integrationtests/batchpkread
  BENCHMARK_DURATION=30
  case $# in
    0) ;;
    1) if [[ "$1" =~ ^[0-9]+$ ]]; then
         BENCHMARK_DURATION="$1"
       else
         BENCHMARK_PACKAGE="$1"
       fi ;;
    2) BENCHMARK_PACKAGE="$1"
       BENCHMARK_DURATION="$2" ;;
    *) die "Too many arguments" ;;
  esac
  go test -v \
     -test.bench BenchmarkSimple \
     -test.run=thisexpressionwontmatchanytest \
     -benchmem \
     -benchtime=${BENCHMARK_DURATION}s \
     -cpuprofile cpu.out \
     -memprofile mem.out \
     $BENCHMARK_PACKAGE
}

cmd-list() {
  [ $# == 0 ] || die "Command $cmd does not take arguments."
  go list ./...
}

cmd-restart() {
  [ $# == 0 ] || die "Command $cmd does not take arguments."
  [ -f "${RDRS_CONFIG_FILE}" ] || die 'RDRS_CONFIG_FILE must be set and point to an existing file.'
  pidfile="$(< ${RDRS_CONFIG_FILE} grep -E '^ *"PIDFile"' |
             sed -r 's@^ *"PIDFile": *"(/[^"]+)",$@\1@;')"
  [ -f "$pidfile" ] || die "PID file not found: $pidfile"
  pid="$(< $pidfile)"
  rdrs2confdir="$(dirname ${RDRS_CONFIG_FILE})"
  rdrs2bin="$(realpath ${rdrs2confdir}/../../bin/rdrs2)"
  [ -x "$rdrs2bin" ] || die "rdrs2 binary not found: $rdrs2bin"
  echo "Killing rdrs2 server PID $pid"
  kill $pid
  echo -n 'Waiting for PID $pid to exit...'
  while kill -0 $pid 2> /dev/null; do sleep 1; echo -n .; done
  echo done
  echo "Restarting rdrs2 server"
  $rdrs2bin >/dev/null 2>/dev/null &
}

die() {
  echo "$*"
  exit 1
}

cmd=help
if [ $# -gt 0 ]; then
  cmd="$1"
  shift
fi
case "$cmd" in
  help|test|bench|list|restart)
    cmd-$cmd "$@" ;;
  *)
    die "Unknown sub-command: $cmd" ;;
esac
