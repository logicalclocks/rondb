/*
   Copyright (c) 2003, 2024, Oracle and/or its affiliates.
   Copyright (c) 2021, 2024, Hopsworks and/or its affiliates.

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

#include <ndb_global.h>

#include <FastScheduler.hpp>
#include <SignalLoggerManager.hpp>
#include <TimeQueue.hpp>
#include <TransporterRegistry.hpp>
#include "Emulator.hpp"

#include "Configuration.hpp"
#include "SimBlockList.hpp"
#include "ThreadConfig.hpp"
#include "WatchDog.hpp"

#include <NodeState.hpp>
#include "ndbd_malloc_impl.hpp"

#include <NdbMutex.h>

#include <string.h>
#include <EventLogger.hpp>

#define JAM_FILE_ID 329

/**
 * Declare the global variables
 */

#ifndef NO_EMULATED_JAM
/*
  This is the jam buffer used for non-threaded ndbd (but present also
  in threaded ndbd to allow sharing of object files among the two
  binaries).
 */
EmulatedJamBuffer theEmulatedJamBuffer;
#endif

GlobalData globalData;

TimeQueue globalTimeQueue;
FastScheduler globalScheduler;
extern TransporterRegistry globalTransporterRegistry;

#ifdef VM_TRACE
SignalLoggerManager globalSignalLoggers;
#endif

EmulatorData globalEmulatorData;
NdbMutex *theShutdownMutex = 0;

// The jamFileNames table has been moved to Emulator.hpp

const char* JamEvent::getFileName() const
{
  if (getFileId() < sizeof jamFileNames/sizeof jamFileNames[0])
  {
    return jamFileNames[getFileId()];
  } else {
    return NULL;
  }
}

EmulatorData::EmulatorData() {
  theConfiguration = 0;
  theWatchDog = 0;
  theThreadConfig = 0;
  theSimBlockList = 0;
  theShutdownMutex = 0;
  m_socket_server = 0;
  m_mem_manager = 0;
}

void EmulatorData::create() {
  /*
    Global jam() buffer, for non-multithreaded operation.
    For multithreaded ndbd, each thread will set a local jam buffer later.
  */
#ifndef NO_EMULATED_JAM
  EmulatedJamBuffer *jamBuffer = &theEmulatedJamBuffer;
#else
  EmulatedJamBuffer *jamBuffer = nullptr;
#endif
  NDB_THREAD_TLS_JAM = jamBuffer;

  theConfiguration = new Configuration();
  theWatchDog = new WatchDog();
  theThreadConfig = new ThreadConfig();
  theSimBlockList = new SimBlockList();
  m_socket_server = new SocketServer();
  m_mem_manager = new Ndbd_mem_manager();
  globalData.m_global_page_pool.setMutex();

  if (theConfiguration == NULL || theWatchDog == NULL ||
      theThreadConfig == NULL || theSimBlockList == NULL ||
      m_socket_server == NULL || m_mem_manager == NULL) {
    ERROR_SET(fatal, NDBD_EXIT_MEMALLOC, "Failed to create EmulatorData", "");
  }

  if (!(theShutdownMutex = NdbMutex_Create())) {
    ERROR_SET(fatal, NDBD_EXIT_MEMALLOC, "Failed to create shutdown mutex", "");
  }
}

void EmulatorData::destroy() {
  if (theConfiguration) delete theConfiguration;
  theConfiguration = 0;
  if (theWatchDog) delete theWatchDog;
  theWatchDog = 0;
  if (theThreadConfig) delete theThreadConfig;
  theThreadConfig = 0;
  if (theSimBlockList) delete theSimBlockList;
  theSimBlockList = 0;
  if (m_socket_server) delete m_socket_server;
  m_socket_server = 0;
  NdbMutex_Destroy(theShutdownMutex);
  if (m_mem_manager) delete m_mem_manager;
  m_mem_manager = 0;
}
