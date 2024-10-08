#!/usr/bin/env bash

if [ $# -gt 0 ]; then cat <<"EOF" # Show help text e.g. by ./bench.sh --help
# How to benchmark:

# 1) Start a cluster and remember config location
...rondb/build/mysql-test$ ./mtr --suite rdrs2 --start-and-exit
...rondb/build/mysql-test$ export \
  RDRS_CONFIG_FILE=`realpath var/rdrs.1.1_config.json`

# 2) Optional: Override defaults. You can list the available test packages:
...rondb/storage/ndb/rest-server2/server/test_go$ go list ./...
#    and then override it
...rondb/storage/ndb/rest-server2/server/test_go$ export \
  BENCHMARK_PACKAGE="hopsworks.ai/rdrs2/internal/integrationtests/pkread"
#    You can also override the duration
...rondb/storage/ndb/rest-server2/server/test_go$ export \
  BENCHMARK_DURATION=60

# 3) Run the benchmark
...rondb/storage/ndb/rest-server2/server/test_go$ ./bench.sh

# 4) After recompiling, to iterate quickly you may want to restart rdrs2 without
#    restarting the entire cluster:
...rondb/build/mysql-test$ kill `cat var/run/rdrs.1.1.pid`
...rondb/build/mysql-test$ ../bin/rdrs2 &

EOF
exit 0; fi

set -eu
BENCHMARK_DURATION=${BENCHMARK_DURATION:-30}
BENCHMARK_PACKAGE=${BENCHMARK_PACKAGE:-hopsworks.ai/rdrs2/internal/integrationtests/batchpkread}
go test -v \
   -test.bench Benchmark \
   -test.run=thisexpressionwontmatchanytest \
   -benchmem \
   -benchtime=${BENCHMARK_DURATION}s \
   -cpuprofile cpu.out \
   -memprofile mem.out \
   $BENCHMARK_PACKAGE
