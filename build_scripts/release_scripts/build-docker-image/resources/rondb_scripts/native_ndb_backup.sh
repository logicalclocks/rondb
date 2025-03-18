#!/usr/bin/env bash

## Connection string to NDB Management server
MGM_CONN=$MGM_CONN_STRING
## User running NDB Data Nodes
MYSQL_USER=mysql
## Installation directory of MySQL
MYSQL_ROOT=/srv/hops/mysql
## Directory at NDB Data Nodes where the local backup is stored
## NOTE: Do NOT include the BACKUP/ directory that NDB adds by default
REMOTE_MYSQL_BACKUP_DIR=/srv/hops/mysql-cluster/ndb/backups

## Location of the socket to connect to mysql
MYSQL_SOCKET=/srv/hops/mysql-cluster/mysql.sock

## Directory at local machine where backups will be stored temporarily
LOCAL_BACKUP_DIR=
BACKUP_LOG_FILE=/srv/hops/mysql-cluster/log/ndb_native_backup.log
RSYNC_LOG_FILE=/srv/hops/mysql-cluster/log/rsync_native_backup.log


## Use the INFO, WARN, ERROR functions instead
function log {
    now=$(date)
    echo "$now - $1 - $2" >> "$BACKUP_LOG_FILE"
}

function log_info {
    log "INFO" "$1"
}

function log_warn {
    log "WARN" "$1"
}

function log_error {
    log "ERROR" "$1"
}

## Perform sanity checks

function check_for_empty_properties {
    if [ -z ${2// } ];
    then
	log_error "Property $1 is empty. Aborting backup."
	exit 10
    fi
}

function check_for_whitespaces {
    echo "$2" | egrep -q "[[:space:]]"
    if [ $? -eq 0 ];
    then
	log_error "Property $1 has whitespace. Aborting backup."
	exit 11
    fi
}

## $1: Name of the property
## $2: Property value
function sanity_check {
    check_for_empty_properties "$1" "$2"
    check_for_whitespaces "$1" "$2"
}

## Variables should be in double quotes to include any whitespaces
sanity_check "MGM_CONN" "$MGM_CONN"
sanity_check "MYSQL_USER" "$MYSQL_USER"
sanity_check "MYSQL_ROOT" "$MYSQL_ROOT"
sanity_check "REMOTE_MYSQL_BACKUP_DIR" "$REMOTE_MYSQL_BACKUP_DIR"
sanity_check "LOCAL_BACKUP_DIR" "$LOCAL_BACKUP_DIR"
sanity_check "BACKUP_LOG_FILE" "$BACKUP_LOG_FILE"
sanity_check "RSYNC_LOG_FILE" "$RSYNC_LOG_FILE"

## $1: Exit code to be examined
## $2: Error message to be logged
## $3: Backup script exit code
function check_exit_code {
    if [ "$1" -ne 0 ];
    then
	log_error "$2"
	exit "$3"
    fi
}

## Get participating NDB Data Nodes in the cluster
NDB_DATANODES="$($MYSQL_ROOT/bin/ndb_mgm -c $MGM_CONN -e "SHOW" | grep Nodegroup | awk -F ' ' '{print $2}' | cut -c2-)"
log_info "Participating NDB Data Nodes: $NDB_DATANODES"

backup_id=$(date +'%y%m%d%H%M')
log_info "Starting NDB native backup with ID BACKUP-$backup_id"

## Start NDB native backup with backup ID $backup_id
## SNAPSHOTEND: Transactions committed during backup will be restored
## WAIT COMPLETED: Command will return when backup has completed
$MYSQL_ROOT/bin/ndb_mgm -c $MGM_CONN -e "START BACKUP $backup_id SNAPSHOTEND WAIT COMPLETED" >>	$BACKUP_LOG_FILE 2>&1
check_exit_code $? "NDB backup failed. Exit code: $?" 1


log_info "Finished NDB native backup with ID BACKUP-$backup_id"
if [ ! -d $LOCAL_BACKUP_DIR ];
then
    log_warn "Local temporary backup dir $LOCAL_BACKUP_DIR does not exist, creating it..."
    mkdir -p $LOCAL_BACKUP_DIR
fi

## Name pattern should be the same on the rotation script on target machine, if used
backup_name=hops_hopsworks_$(date +'%y-%m-%d_%H-%M').backup
backup_target=$LOCAL_BACKUP_DIR/$backup_name

if [ -d "$backup_target" ];
then
    log_error "Ooops directory $backup_target already exist in the local filesystem! Canceling copying backup from Data Nodes"
    exit 2
fi

## copy backup from Data Nodes to localhost
for i in $NDB_DATANODES
do
    log_info "Moving backup with ID BACKUP-$backup_id from node $i"
    mkdir -p "$backup_target"/"$i"
    rsync -az --log-file=$RSYNC_LOG_FILE $MYSQL_USER@"$i":$REMOTE_MYSQL_BACKUP_DIR/BACKUP/BACKUP-"$backup_id" "$backup_target"/"$i"
    check_exit_code $? "rsync from $i failed, aborting. Exit code: $?" 3
done

# Backing up the schemas
DB_LIST=`$MYSQL_ROOT/bin/mysql -u root -S $MYSQL_SOCKET -Nse "SELECT GROUP_CONCAT(SCHEMA_NAME SEPARATOR ' ') FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('mysql','information_schema','performance_schema','sys','ndbinfo');"`
$MYSQL_ROOT/bin/mysqldump -u root -S $MYSQL_SOCKET --no-data --triggers --routines --events --databases $DB_LIST  > "$backup_target"/schema.sql

# Backing up the users
$MYSQL_ROOT/bin/mysqlpump -u root -S $MYSQL_SOCKET --exclude-databases=%% --exclude-users=root,mysql.sys,mysql.session,mysql.infoschema --users > "$backup_target"/users.sql

pushd $LOCAL_BACKUP_DIR
tar cfz "$backup_name".tar.gz "$backup_name"
popd

log_info "Finished backup"
