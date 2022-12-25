# This file is part of the RonDB REST API Server
# Copyright (c) 2022 Hopsworks AB
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
RDRS_LIB_DIR?=./../data-access-rondb/build
OUTPUT_DIR?=./bin/server/${SERVER_BIN}

COVERAGE_OUTPUT_FILE?=./coverage.out
COVERAGE_HTML_OUTPUT_FILE?=./coverage.html
COVERAGE_FUNC_OUTPUT_FILE?=/dev/stdout

DIRS = $(shell find . -type d)
FILES = $(shell find . -type f -iname '*.go')

# TODO: Add debug flag here to deal with -03 flag
# CGO_LDFLAGS="-g -O3 -L${RDRS_LIB_DIR} -lrdrclient"

all: rdrs
#static linking https://ragoragino.github.io/softwareengineering/miscellaneous/2021/03/23/cgo-static.html
#-tags osusergo,netgo

BUILD_COMMAND=go build -ldflags="-w \
     -X hopsworks.ai/rdrs/version.GITCOMMIT=${GITCOMMIT} \
     -X hopsworks.ai/rdrs/version.BUILDTIME=${BUILDTIME} \
     -X hopsworks.ai/rdrs/version.HOSTNAME=${HOSTNAME} \
     -X hopsworks.ai/rdrs/version.BRANCH=${BRANCH}" \
     -o ${OUTPUT_DIR}/rdrs ./cmd/server/main.go

rdrs:
	CGO_LDFLAGS="-g -L${RDRS_LIB_DIR} -lrdrclient" ${BUILD_COMMAND}

clean:
	go clean -cache
	rm -rf ./bin/*

build_proto:
	protoc --go_out=. --go-grpc_out=. ./api/rdrs.proto

test:
	@go clean -testcache
	@echo "Getting cross-package (integration) coverage and writing it to $(COVERAGE_OUTPUT_FILE)"
	CGO_LDFLAGS="-g -L${RDRS_LIB_DIR} -lrdrclient" go test -p 1 -coverprofile $(COVERAGE_OUTPUT_FILE) -coverpkg ./... `go list ./... | grep -v cmd`

print-func-coverage: test
	go tool cover -func $(COVERAGE_OUTPUT_FILE) > $(COVERAGE_FUNC_OUTPUT_FILE)

show-html-coverage: test
	go tool cover -html $(COVERAGE_OUTPUT_FILE) -o $(COVERAGE_HTML_OUTPUT_FILE) && xdg-open $(COVERAGE_HTML_OUTPUT_FILE)
