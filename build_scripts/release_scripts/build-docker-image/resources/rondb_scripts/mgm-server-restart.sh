#!/bin/sh

/srv/hops/mysql-cluster/ndb/scripts/mgm-server-stop.sh

/srv/hops/mysql-cluster/ndb/scripts/mgm-server-start.sh

exit $?
