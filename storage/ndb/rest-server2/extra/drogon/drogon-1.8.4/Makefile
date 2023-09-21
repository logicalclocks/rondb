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

# location of the generated binary file
OUTPUT_DIR?=./bin/server/${SERVER_BIN}


all: rdrs


rdrs:
	export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:${RDRS_LIB_DIR} && \
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${RDRS_LIB_DIR} && \
	CGO_LDFLAGS=${MY_CGO_LDFLAGS} ${BUILD_COMMAND}
