#!/bin/bash
# Generating RonDB clusters of variable sizes with docker compose
# Copyright (c) 2023, 2023 Hopsworks AB and/or its affiliates.

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

################
### Examples ###
################

# - "Node 1: not connected"             # exit code 0
# - "3: Node not found"                 # exit code 1
# - "Node 2: started (RonDB-22.10.5)"   # exit code 0
# - "94: Node not found"                # exit code 255

MGM_CONNECTION_STRING=$1
NODE_ID=$2

# An alternative to using ndb_waiter, since the ndb_waiter occupies an API slot.
# The mgm client uses the mgmds slot instead.
STATUS=$(ndb_mgm --ndb-connectstring $MGM_CONNECTION_STRING -e "$NODE_ID status")
echo $STATUS

case $STATUS in
*"started"*)
    exit 0
    ;;
*"starting"* | *"not connected"* | *"not found"* | *"Invalid"*)
    exit 1
    ;;
esac
