#!/bin/sh

/srv/hops/mysql-cluster/ndb/scripts/ndbd-stop.sh

/srv/hops/mysql-cluster/ndb/scripts/ndbd-start.sh

exit $?
