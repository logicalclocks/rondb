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

RDRS_DROGON_BINARY_DIR?=UNSET
RDRS_DROGON_SOURCE_DIR?=UNSET
RDRS_DROGON_INSTALL_DIR?=UNSET

all: rdrs2

rdrs2:
	$(info "=================> Building RDRS2")
	mkdir -p ${RDRS_DROGON_BINARY_DIR}
	cd ${RDRS_DROGON_BINARY_DIR}
	cmake ${RDRS_DROGON_SOURCE_DIR}
