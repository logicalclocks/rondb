#!/usr/bin/env sh

SKIP_GRANTS=
SKIP_WAIT=0
if [ $# -gt 0 ]; then
    if [ "$1" = "-f" ]; then
        pkill -9 mysqld
    elif [ "$1" = "--skip-grant-tables" ]; then
        SKIP_GRANTS="--skip-grant-tables"
    elif [ "$1" = "--skip-ndb-wait" ]; then
        SKIP_WAIT=1
    else
        echo "usage: $0 [-f|--skip-grant-tables|--skip-ndb-wait]"
        exit 2
    fi
fi

echo "Testing if a mysql server is already running on this host..."

MYSQL_SOCKET=$(/srv/hops/mysql-cluster/ndb/scripts/get-mysql-socket.sh)
/srv/hops/mysql/bin/mysqladmin -S "$MYSQL_SOCKET" -s -u root ping
# Don't redirect error, as this will give a '0' return result &> /dev/null
if [ $? -eq 0 ]; then
    echo "A MySQL Server is already running at socket. Not starting another MySQL Server at this socket. Use '--force' to kill existing mysql servers at this node."
    exit 1
fi

# If there is a stray lock file lying around (and the mysqld is not listening on the socket), remove it.
rm -f /tmp/mysql.sock.lock

export MYSQL_HOME=/srv/hops/mysql-cluster

if [ $SKIP_WAIT -ne 1 ]; then
    /srv/hops/mysql/bin/ndb_waiter -c $MGM_CONN_STRING --timeout=10800 2>&1 >/dev/null
fi

mysqld_command="/srv/hops/mysql/bin/mysqld --defaults-file=/srv/hops/mysql-cluster/my.cnf $SKIP_GRANTS"

# This is not in the original cloud setup;
# It is used for alternative process managers such as supervsisord
# that cannot daemonize processes.
if [ -n "$NO_DAEMON" ]; then
    echo "Starting the MySQL server as a foreground process"
    exec $mysqld_command
else
    $mysqld_command --daemonize --log-error=/srv/hops/mysql-cluster/log/mysqld_out.log --log-error-verbosity=3
fi

exit $?
