#!/usr/bin/env bash

## Cron job that deletes backups from NDB Data Nodes local backup directory.
## This script should be complemented by native_ndb_backup.sh script to perform
## the backup and archive them in a remote host

## Directory where NDB Data Nodes store their local backup
BACKUP_DIR=/srv/hops/mysql-cluster/ndb/backups
## How many n*24 hours are the backup files in NDB Data Nodes going to stay
## until they are removed
RETENTION_TIME=5

BACKUPS_REGEX="$BACKUP_DIR/BACKUP/BACKUP-[0-9]+"

## Perform sanity checks
function check_for_empty_properties {
    if [ -z ${2// } ];
    then
	exit 1
    fi
}

function check_for_whitespaces {
    echo "$2" | egrep -q "[[:space:]]"
    if [ $? -eq 0 ];
    then
	exit 2
    fi
}

## $1: Name of the property
## $2: Property value
function sanity_check {
    check_for_empty_properties "$1" "$2"
    check_for_whitespaces "$1" "$2"
}

sanity_check "BACKUP_DIR" "$BACKUP_DIR"

find $BACKUP_DIR -type d -regextype posix-extended -regex "$BACKUPS_REGEX" -prune -ctime +$RETENTION_TIME -exec rm -rf {} \;
