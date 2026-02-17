# suite/ndb_ttl/include/ttl_backup_restore_setup.inc
#
# Thin wrapper that sources the standard NDB backup/restore setup
# include to define $NDB_BACKUPS from $NDB_BACKUP_DIR.
#
# Usage:
#   --source suite/ndb_ttl/include/ttl_backup_restore_setup.inc

--source suite/ndb/include/backup_restore_setup.inc
