#!/bin/sh

/srv/hops/mysql/bin/ndb_mgm -c $MGM_CONN_STRING -e "EXIT SINGLE USER MODE"
echo $?
