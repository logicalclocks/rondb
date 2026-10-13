/*
   Copyright (c) 2003, 2025, Oracle and/or its affiliates.

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

#ifndef FS_REMOVE_REQ_H
#define FS_REMOVE_REQ_H

#include "FsOpenReq.hpp"
#include "SignalData.hpp"

#define JAM_FILE_ID 62

/**
 *
 * SENDER:
 * RECIVER: Ndbfs
 */
class FsRemoveReq {
  /**
   * Reciver(s)
   */
  friend class Ndbfs;      // Reciver
  friend class AsyncFile;  // Uses FsOpenReq to decode file open flags
  friend class Filename;
  friend class VoidFs;
  friend class Restore;

  /**
   * Sender(s)
   */
  friend class Backup;
  friend class Dbdict;
  friend class Dbacc;
  friend class Dbtup;
  friend class Ndbcntr;  // For initial start...

 public:
  /**
   * Length of signal
   */
  static constexpr Uint32 SignalLength = 8;

  /**
   * Length including emptyDirectoryOnly. The plain 8-word signal
   * behaves as emptyDirectoryOnly = 0.
   */
  static constexpr Uint32 SignalLengthEmptyDirectoryOnly = 9;

 private:
  /**
   * DATA VARIABLES
   */

  UintR userReference;  // DATA 0
  UintR userPointer;    // DATA 1
  UintR fileNumber[4];  // DATA 2 - 5 // See FsOpen for interpretation

  /**
   * 0 = File -> rm file
   * 1 = Directory -> rm -r path
   */
  UintR directory;

  /**
   * If directory = 1
   *
   * 0 = remove only files/directories in directory specified in fileNumber
   * 1 = remove directory specified in fileNumber
   */
  UintR ownDirectory;

  /**
   * If directory = 1 and ownDirectory = 1
   *
   * 0 = remove recursively (rm -rf)
   * 1 = remove the directory itself only if it is empty (plain rmdir,
   *     never recursing); a missing or non-empty directory is not an
   *     error. This is the only removal mode safe on a directory that
   *     may hold content the sender does not own.
   */
  UintR emptyDirectoryOnly;
};

DECLARE_SIGNAL_SCOPE(GSN_FSREMOVEREQ, Local);

#undef JAM_FILE_ID

#endif
