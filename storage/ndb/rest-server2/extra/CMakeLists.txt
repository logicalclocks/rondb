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



# Both simdjson and drogon depend on jsoncpp. simdjson has vendored jsoncpp (see
# simdjson/simdjson*/dependencies/jsoncppdist) while drogon expects to find it
# already installed (see drogon/drogon-*/cmake_modules/FindJsoncpp.cmake). We
# add simdjson first so that drogon can use the version of jsoncpp that is
# already part of the source tree under simdjson.

add_subdirectory(simdjson)
add_subdirectory(drogon)
