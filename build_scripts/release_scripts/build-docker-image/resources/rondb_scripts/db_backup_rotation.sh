#!/usr/bin/env bash

## File for managing NDB backup rotation on the archive host
## This file must be put manually on the remote host and configured

## Root of the backup directory 
BACKUP_ROOT_DIR=
## Directory where all the recent backups will go
## NOTE: It should be the same in the backup script
RECENT_DIR=$BACKUP_ROOT_DIR/RECENT
## How many n*24 hours are backup files allowed to stay in RECENT folder
## before moved to ARCHIVE
## 15 days
RECENT_TIMEOUT=
## How many n*24 hours are the archive files allowed to stay in the archive
## folder before being purged
## 150 days
ARCHIVE_TIMEOUT=
## Regular expression for the backup files
## NOTE: It should be the same in the backup script
## For example hops_hopsworks_2018-01-24_17-24.backup.tar.gz
BACKUP_FILE_REGEX="hops\_hopsworks\_[0-9]{4,5}\-[0-9]{1,2}\-[0-9]{1,2}\_[0-9]{1,2}\-[0-9]{1,2}\.backup.tar.gz"
## Log file
LOG_FILE=$BACKUP_ROOT_DIR/backup_rotation.log

ARCHIVE_DIR=$BACKUP_ROOT_DIR/archives
FILES_TO_MOVE=$BACKUP_ROOT_DIR/.files_to_move
FILES_TO_REMOVE=$BACKUP_ROOT_DIR/.files_to_remove
RECENT_BACKUP_FILE_REGEX="$RECENT_DIR/$BACKUP_FILE_REGEX"
ARCHIVE_BACKUP_FILE_REGEX="$ARCHIVE_DIR/.*/$BACKUP_FILE_REGEX"

## Use the INFO, WARN, ERROR functions instead
function log {
    now=$(date)
    echo "$now - $1 - $2" >> "$LOG_FILE"
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
    if [ -z "${2// }" ];
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
sanity_check "BACKUP_ROOT_DIR" "$BACKUP_ROOT_DIR"
sanity_check "RECENT_DIR" "$RECENT_DIR"
sanity_check "LOG_FILE" "$LOG_FILE"
sanity_check "ARCHIVE_DIR" "$ARCHIVE_DIR"
sanity_check "RECENT_TIMEOUT" "$RECENT_TIMEOUT"
sanity_check "ARCHIVE_TIMEOUT" "$ARCHIVE_TIMEOUT"

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

log_info "Starting backup rotation"

if [ ! -d "$ARCHIVE_DIR" ];
then
    mkdir -p -m 700 "$ARCHIVE_DIR"
    log_warn "Created non existing directory $ARCHIVE_DIR"
fi

find "$RECENT_DIR" -type f -regextype posix-extended -regex "$RECENT_BACKUP_FILE_REGEX" -readable -ctime +$RECENT_TIMEOUT > "$FILES_TO_MOVE"
check_exit_code $? "Finding RECENT backups to move failed" 1

while IFS='' read -r backup_file || [[ -n "$backup_file" ]];
do
    filename=$(basename "$backup_file")
    date=$(echo "$filename" | awk -F '_' '{print $3}')
    year=$(echo "$date" | awk -F '-' '{print $1}')
    month=$(echo "$date" | awk -F '-' '{print $2}')

    destination_dir="$ARCHIVE_DIR/${year}_${month}"
    if [ ! -d "$destination_dir" ];
    then
        mkdir -p -m 700 "$destination_dir"
	check_exit_code $? "Failed to create $destination_dir" 2
	
        log_info "Created directory $destination_dir"
    fi

    mv "$backup_file" "$destination_dir"
    check_exit_code $? "Failed to move $backup_file to $destination_dir" 3
    
    log_info "Moved file $backup_file"
    
done < $FILES_TO_MOVE

rm -f "$FILES_TO_MOVE"

find "$ARCHIVE_DIR" -type f -regextype posix-extended -regex "$ARCHIVE_BACKUP_FILE_REGEX" -readable -ctime +$ARCHIVE_TIMEOUT > $FILES_TO_REMOVE
check_exit_code $? "Finding ARCHIVED backups to delete failed" 4

while IFS='' read -r file_to_remove || [[ -n "$file_to_remove" ]];
do
    rm -f "$file_to_remove"
    log_info "Deleted archive file $file_to_remove"
done < $FILES_TO_REMOVE

rm -f "$FILES_TO_REMOVE"

find $ARCHIVE_DIR -type d -empty -delete
log_info "Finished backup rotation"
