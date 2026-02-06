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

# Suppress GCC 12 false positives that reappear during LTO at link time
# for any target linking simdjson or drogon (directly or transitively):
# -Wmaybe-uninitialized from AVX512 intrinsics in simdjson icelake backend,
# -Wstringop-overflow from std::string over-inlining in jsoncpp.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_link_options(-Wno-maybe-uninitialized -Wno-stringop-overflow)
endif()

add_subdirectory(extra)
add_subdirectory(server)
