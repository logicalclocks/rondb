/*
   Copyright (c) 2003, 2025, Oracle and/or its affiliates.
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
    All rights reserved. Use is subject to license terms.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef NDBT_BACKUP_HPP
#define NDBT_BACKUP_HPP

#include <string>

#include <mgmapi.h>
#include <NdbRestarter.hpp>
#include <Vector.hpp>
#include "NdbConfig.hpp"

class NdbBackup : public NdbConfig {
 public:
  NdbBackup(const char *_addr = 0)
      : NdbConfig(_addr), m_default_encryption_password(NULL) {}

  // if len == -1, then function will use strlen() to get size of pwd
  int set_default_encryption_password(const char *pwd, int len);

  int start(unsigned &_backup_id, int flags = 2,
            unsigned int user_backup_id = 0, unsigned int logtype = 0,
            const char *encryption_password = nullptr,
            unsigned int password_length = 0);
  int start() {
    unsigned unused = 0;
    return start(unused);
  }
  int restore(unsigned _backup_id, bool restore_meta = true,
              bool restore_data = true, unsigned error_insert = 0,
              bool restore_epoch = false);

  int NFMaster(NdbRestarter &_restarter);
  int NFMasterAsSlave(NdbRestarter &_restarter);
  int NFSlave(NdbRestarter &_restarter);
  int NF(NdbRestarter &_restarter, int *NFDuringBackup_codes, const int sz,
         bool onMaster);

  int FailMaster(NdbRestarter &_restarter);
  int FailMasterAsSlave(NdbRestarter &_restarter);
  int FailSlave(NdbRestarter &_restarter);
  int Fail(NdbRestarter &_restarter, int *Fail_codes, const int sz,
           bool onMaster);
  int startLogEvent();
  int checkBackupStatus();

  int clearOldBackups();
  int abort(unsigned _backup_id);

  /**
   * Check whether any backup file content (regular files under a
   * BACKUP-* entry) exists in the node's BackupDataDir. Empty
   * directory shells do not count: scoped removal of a failed backup
   * leaves the shared BACKUP-<id> parent in place. Returns 1 if
   * content exists, 0 if none, -1 on error (or on Windows where the
   * check is not implemented).
   */
  int backupDirsExist(int node_id);

  /**
   * Check whether any BACKUP-* directory entry (empty shell or not)
   * exists in the node's BackupDataDir. Complements backupDirsExist:
   * a completed removal of a failed backup must leave neither file
   * content nor the BACKUP-<id> / BACKUP-<id>-PART-N-OF-M directory
   * shells. Returns 1 if any such directory exists, 0 if none, -1 on
   * error (or on Windows where the check is not implemented).
   */
  int backupShellsExist(int node_id);

  /**
   * Fabricate / check a single-threaded-layout fileset for the given
   * backup id in the node's BackupDataDir (the three files directly
   * under BACKUP-<id>, no part directories) - stands in for a valid
   * older single-threaded backup when testing that a failed
   * multithreaded attempt reusing the id never touches it.
   * createStBackupFileset returns 0 on success; stBackupFilesetExists
   * returns 1 if all three files exist, 0 if not, -1 on error (both
   * -1 on Windows).
   */
  int createStBackupFileset(int node_id, unsigned backup_id);
  int stBackupFilesetExists(int node_id, unsigned backup_id);

  /**
   * Check whether any BACKUP-<id>-PART-* directory (or content below
   * one) of the given backup id remains on the node - the
   * multithreaded attempt's own layout, which a debris sweep must
   * remove. Returns 1 if present, 0 if none, -1 on error (or on
   * Windows).
   */
  int backupMtDebrisExists(int node_id, unsigned backup_id);

 private:
  int execRestore(bool _restore_data, bool _restore_meta, bool _restore_epoch,
                  bool _disable_indexes, bool _enable_indexes,
                  int _node_id, unsigned _backup_id, unsigned error_insert = 0,
                  const char *encryption_password = nullptr,
                  int password_length = -1);

  std::string getBackupDataDirForNode(int node_id);
  NdbLogEventHandle log_handle;
  BaseString getNdbRestoreBinaryPath();
  char *m_default_encryption_password;
  size_t m_default_encryption_password_length;
};

#endif
