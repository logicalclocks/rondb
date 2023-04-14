# This file is part of the RonDB REST API Server
# Copyright (c) 2023 Hopsworks AB
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

GITCOMMIT=`git rev-parse --short HEAD`
BUILDTIME=`date +%FT%T%z`
HOSTNAME=`hostname`
BRANCH=`git rev-parse --abbrev-ref HEAD`
SERVER_BIN="rdrs"

# config params

# location of the generated binary file
OUTPUT_DIR?=./bin/server/${SERVER_BIN}

# RonDB build dir that contains libs needed for the API serer
RDRS_LIB_DIR?=$(PWD)/../../../../build/library_output_directory

# Configuration to use for testing
# testing using mtr: $(PWD)/resources/config/config_mtr.json
RDRS_CONFIG_FILE?=$(PWD)/resources/config/config_dockertest.json

# Testing. Override this to run a single test. e.g.
# make test RDRS_CONFIG_FILE=`pwd`/resources/config/config_mtr.json TEST_PACKAGE=hopsworks.ai/rdrs/internal/integrationtests/pkread TEST=TestDataTypesBlobs/blob1 
TEST?=Test
TEST_PACKAGE?=./...

# code coverage 
COVERAGE_OUTPUT_FILE?=./coverage.out
COVERAGE_HTML_OUTPUT_FILE?=./coverage.html
COVERAGE_FUNC_OUTPUT_FILE?=/dev/stdout

#CGO_LDFLAGS
MY_CGO_LDFLAGS?="-g -O3 -L${RDRS_LIB_DIR}/ -Wl,-rpath ${RDRS_LIB_DIR} -lrdrclient"

all: rdrs

BUILD_COMMAND=go build -ldflags="-w \
     -X hopsworks.ai/rdrs/version.GITCOMMIT=${GITCOMMIT} \
     -X hopsworks.ai/rdrs/version.BUILDTIME=${BUILDTIME} \
     -X hopsworks.ai/rdrs/version.HOSTNAME=${HOSTNAME} \
     -X hopsworks.ai/rdrs/version.BRANCH=${BRANCH}" \
     -o ${OUTPUT_DIR}/rdrs ./cmd/server/main.go

rdrs:
	export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:${RDRS_LIB_DIR} && \
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${RDRS_LIB_DIR} && \
	CGO_LDFLAGS=${MY_CGO_LDFLAGS} ${BUILD_COMMAND}

clean:
	go clean -cache
	rm -rf ./bin/*

build_proto:
	protoc --go_out=. --go-grpc_out=. ./api/rdrs.proto

# By default, unit tests in different packages are run concurrently by the order of CPUs.
# Using t.Parallel() allows tests within packages to run concurrently.
# We disallow cross-package concurrency using `-p 1` since some of our tests have side 
# 	effects such as creating and removing databases. If two tests require the same
# 	database, then it may accidentally be deleted by one test whilst the other one
# 	is still using it.
# Info on running tests in parallel in Go:
# 	https://stackoverflow.com/a/58987438/9068781
test:
	@go clean -testcache
	@echo "Getting cross-package (integration) coverage and writing it to $(COVERAGE_OUTPUT_FILE)"
	export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:${RDRS_LIB_DIR} && \
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${RDRS_LIB_DIR} && \
	export RDRS_CONFIG_FILE=$(RDRS_CONFIG_FILE) && \
	export CGO_LDFLAGS=${MY_CGO_LDFLAGS} && \
	go test -v -p 1 -run $(TEST) -coverprofile $(COVERAGE_OUTPUT_FILE) -coverpkg  $(TEST_PACKAGE) `go list $(TEST_PACKAGE) | grep -v cmd`

# Run specific benchmark (remove profiling for better performance):
# go test -v \
# 	-bench BenchmarkSimple \
# 	-run=xxx \
# 	-benchmem \
# 	-benchtime=10s \
# 	-cpuprofile cpu.out \
# 	-memprofile mem.out \
# 	hopsworks.ai/rdrs/internal/integrationtests/ping

# Run specific unit test:
# go test \
# 	-run TestPing \
# 	-cpuprofile cpu.out \
# 	-memprofile mem.out \
# 	hopsworks.ai/rdrs/internal/integrationtests/ping

# Otherwise use e.g. `go tool pprof cpu.out` for an interactive CLI
create-profiling-pdfs:
	go tool pprof -pdf cpu.out > cpu_profile.pdf
	go tool pprof -pdf mem.out > mem_profile.pdf

print-func-coverage: test
	go tool cover -func $(COVERAGE_OUTPUT_FILE) > $(COVERAGE_FUNC_OUTPUT_FILE)

show-html-coverage: test
	go tool cover -html $(COVERAGE_OUTPUT_FILE) -o $(COVERAGE_HTML_OUTPUT_FILE) && xdg-open $(COVERAGE_HTML_OUTPUT_FILE)
