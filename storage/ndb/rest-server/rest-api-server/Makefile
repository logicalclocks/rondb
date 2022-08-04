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

DIRS = $(shell find . -type d)
FILES = $(shell find . -type f -iname '*.go')

all: rdrs
#static linking https://ragoragino.github.io/softwareengineering/miscellaneous/2021/03/23/cgo-static.html
#-tags osusergo,netgo

BUILD_COMMAND=go build  -ldflags="-w \
     -X hopsworks.ai/rdrs/version.GITCOMMIT=${GITCOMMIT} \
     -X hopsworks.ai/rdrs/version.BUILDTIME=${BUILDTIME} \
     -X hopsworks.ai/rdrs/version.HOSTNAME=${HOSTNAME} \
     -X hopsworks.ai/rdrs/version.BRANCH=${BRANCH}" \
     -o ${OUTPUT_DIR}/rdrs ./cmd/server/main.go

LINK_COMMAND=CGO_LDFLAGS="-g -O2 -L${RDRS_LIB_DIR} -lrdrclient"

rdrs:
	CGO_LDFLAGS="-g -O2 -L${RDRS_LIB_DIR} -lrdrclient" ${BUILD_COMMAND}

clean:
	go clean -cache
	rm -rf ./bin/*

build_proto:
	protoc --go_out=. --go-grpc_out=. ./api/rdrs.proto

test:
	go clean -testcache
	CGO_LDFLAGS="-g -O2 -L${RDRS_LIB_DIR} -lrdrclient" go test -v -p 1 ./... -coverprofile coverage.out
	go tool cover -html=coverage.out -o coverage.html && xdg-open coverage.html

