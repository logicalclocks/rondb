/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
   Copyright (c) 2021, 2023, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/


#include <ndb_global.h>

#include <ndbd_exit_codes.h>
#include "ErrorReporter.hpp"

#include <FastScheduler.hpp>
#include <DebuggerNames.hpp>
#include <NdbHost.h>
#include <NdbConfig.h>
#include <Configuration.hpp>
#include "EventLogger.hpp"

#include "ndb_stacktrace.h"
#include "TimeModule.hpp"

#include <NdbAutoPtr.hpp>
#include <NdbSleep.h>

#define JAM_FILE_ID 490

/* New MESSAGE_LENGTH chosen as 999 to replace the old value
 * of 500. In the old scheme the offset value between messages
 * was actually MESSAGE_LENGTH - 1. To cleanly overwrite two messages
 * of the old length, the new offset would have to be 499 * 2 = 998.
 * Thus, a MESSAGE_LENGTH of 998 + 1 = 999 gives a clean overwrite
 * of two length 499 messages.
 */
#define MESSAGE_LENGTH 999
#define OLD_MESSAGE_LENGTH 499

const char * ndb_basename(const char *path);

#ifdef ERROR_INSERT
int simulate_error_during_error_reporting = 0;
#endif

static
const char*
formatTimeStampString(char* theDateTimeString, size_t len){
  TimeModule DateTime;          /* To create "theDateTimeString" */
  DateTime.setTimeStamp();
  
  BaseString::snprintf(theDateTimeString, len, "%s %d %s %d - %s:%s:%s",
	   DateTime.getDayName(), DateTime.getDayOfMonth(),
	   DateTime.getMonthName(), DateTime.getYear(), DateTime.getHour(),
	   DateTime.getMinute(), DateTime.getSecond());
  
  return theDateTimeString;
}

int
ErrorReporter::get_trace_no(){
  
  FILE *stream;
  unsigned int traceFileNo;
  
  char *file_name= NdbConfig_NextTraceFileName(globalData.ownId);
  NdbAutoPtr<char> tmp_aptr(file_name);

  /* 
   * Read last number from tracefile
   */  
  stream = fopen(file_name, "r+");
  if (stream == NULL)
  {
    traceFileNo = 1;
  }
  else
  {
    char buf[255];
    if (fgets(buf, 255, stream) == NULL)
    {
      traceFileNo = 1;
    }
    else
    {
      const int scan = sscanf(buf, "%u", &traceFileNo);
      if(scan != 1)
      {
        traceFileNo = 1;
      }
    }
    fclose(stream);
    traceFileNo++;
  }

  /**
   * Wrap tracefile no 
   */
  Uint32 tmp = globalEmulatorData.theConfiguration->maxNoOfErrorLogs();
  if (traceFileNo > tmp ) {
    traceFileNo = 1;
  }

  /**
   *  Save new number to the file
   */
  stream = fopen(file_name, "w");
  if(stream != NULL){
    fprintf(stream, "%u", traceFileNo);
    fclose(stream);
  }

  return traceFileNo;
}

// Using my_progname without including all of mysys
extern const char* my_progname;

void
ErrorReporter::formatMessage(int thr_no,
                             Uint32 num_threads, int faultID,
			     const char* problemData, 
			     const char* objRef,
			     const char* theNameOfTheTraceFile,
			     char* messptr){
  int processId;
  ndbd_exit_classification cl;
  ndbd_exit_status st;
  const char *exit_msg = ndbd_exit_message(faultID, &cl);
  const char *exit_cl_msg = ndbd_exit_classification_message(cl, &st);
  const char *exit_st_msg = ndbd_exit_status_message(st);
  int sofar;

  /* Extract the name of the trace file to log explicitly as it is
   * often truncated due to long path names */
  BaseString failingThdTraceFileName("");
  if (theNameOfTheTraceFile) {
    BaseString traceFileFullPath(theNameOfTheTraceFile);
    Vector<BaseString> traceFileComponents;
    int noOfComponents = traceFileFullPath.split(traceFileComponents,
                                                 DIR_SEPARATOR);
    assert(noOfComponents >= 1);
    failingThdTraceFileName = traceFileComponents[noOfComponents-1];
  }

  processId = NdbHost_GetProcessId();
  char thrbuf[100] = "";
  if (thr_no >= 0)
  {
    char thrSuffix[100] = "";
    BaseString::snprintf(thrbuf, sizeof(thrbuf), " thr: %u", thr_no);
    if (thr_no > 0)
    {
      /* Append the thread number to log the causing thread trace file
       * name explicitly
       * Thread 0 is a special case with no suffix */
      BaseString::snprintf(thrSuffix, sizeof(thrSuffix), "_t%u", thr_no);
      failingThdTraceFileName.append(thrSuffix);
    }
  }

  char time_str[39];
  formatTimeStampString(time_str, sizeof(time_str));

  BaseString::snprintf(messptr, MESSAGE_LENGTH,
                       "Time: %s\n"
                       "Status: %s\n"
                       "Message: %s (%s)\n"
                       "Error: %d\n"
                       "Error data: %s\n"
                       "Error object: %s\n"
                       "Program: %s\n"
                       "Pid: %d%s\n"
                       "Version: %s\n"
                       "Trace file name: %s\n"
                       "Trace file path: %s",
                       time_str,
                       exit_st_msg,
                       exit_msg, exit_cl_msg,
                       faultID, 
                       (problemData == NULL) ? "" : problemData, 
                       objRef, 
                       ndb_basename(my_progname),
                       processId, 
                       thrbuf,
                       NDB_VERSION_STRING,
                       theNameOfTheTraceFile ?
                       failingThdTraceFileName.c_str()
                       : "<no tracefile>",
                       theNameOfTheTraceFile ? 
                       theNameOfTheTraceFile : "<no tracefile>");

  if (theNameOfTheTraceFile)
  {
    sofar = (int)strlen(messptr);
    if(sofar < MESSAGE_LENGTH)
    {
      BaseString::snprintf(messptr + sofar, MESSAGE_LENGTH - sofar,
                           " [t%u..t%u]", 1, num_threads);
    }
  }

  sofar = (int)strlen(messptr);
  if(sofar < MESSAGE_LENGTH)
  {
    BaseString::snprintf(messptr + sofar, MESSAGE_LENGTH - sofar,
                         "\n"
                         "***EOM***\n");
  }

  // Add trailing blanks to get a fixed length of the message
  while (strlen(messptr) <= MESSAGE_LENGTH-3){
    strcat(messptr, " ");
  }
  
  messptr[MESSAGE_LENGTH -2]='\n';
  messptr[MESSAGE_LENGTH -1]=0;
  
  return;
}

NdbShutdownType ErrorReporter::s_errorHandlerShutdownType = NST_ErrorHandler;

void
ErrorReporter::handleAssert(const char* message, const char* file, int line, int ec)
{
  char refMessage[200];

  globalData.theStopFlag = true;
#ifdef NO_EMULATED_JAM
  BaseString::snprintf(refMessage, 200, "file: %s lineNo: %d",
	   file, line);
#else
  BaseString::snprintf(refMessage, 200, "%s line: %d",
	   file, line);
#endif
  NdbShutdownType nst = s_errorHandlerShutdownType;
  WriteMessage(ec, message, refMessage, nst);

  NdbShutdown(ec, nst);
  exit(1);                                      // Deadcode
}

void
ErrorReporter::handleError(int messageID,
			   const char* problemData, 
			   const char* objRef,
			   NdbShutdownType nst)
{
  globalData.theStopFlag = true;
  ndb_print_stacktrace();

  if(messageID == NDBD_EXIT_ERROR_INSERT)
  {
    nst = NST_ErrorInsert;
  } 
  else 
  {
    if (nst == NST_ErrorHandler)
      nst = s_errorHandlerShutdownType;
  }
  
  WriteMessage(messageID, ndb_basename(problemData), objRef, nst);

  if (problemData == NULL)
  {
    ndbd_exit_classification cl;
    problemData = ndbd_exit_message(messageID, &cl);
  }

  g_eventLogger->info("%s", problemData);
  g_eventLogger->info("%s", objRef);

  NdbShutdown(messageID, nst);
  exit(1); // kill warning
}

bool ErrorReporter::dumpJam_ok = false;
Uint32 ErrorReporter::dumpJam_oldest = 0;
const JamEvent *ErrorReporter::dumpJam_buffer = 0;
Uint32 ErrorReporter::dumpJam_cursor = 0;

int 
ErrorReporter::WriteMessage(int thrdMessageID,
                            const char* thrdProblemData,
                            const char* thrdObjRef,
                            NdbShutdownType & nst){
  FILE *stream;
  unsigned offset;
  unsigned long maxOffset;  // Maximum size of file.
  char theMessage[MESSAGE_LENGTH];

  /**
   * In the multithreaded case we need to lock a mutex before starting
   * the error processing. The method below will lock this mutex,
   * after locking the mutex it will ensure that there is no other
   * thread that already started the crash handling. If there is
   * already another thread that is processing the thread we will
   * write in the error log while holding the mutex. If we are
   * crashing due to an error insert and we already have an ongoing
   * crash handler then we will never return from this first call.
   * Otherwise we will return, write the error log and never return
   * from the second call to prepare_to_crash below.
   */
  prepare_to_crash(true, (nst == NST_ErrorInsert));

  Uint32 threadCount = globalScheduler.traceDumpGetNumThreads();
  int thr_no = globalScheduler.traceDumpGetCurrentThread();

  /**
   * Format trace file name
   */
  char *theTraceFileName= 0;
  if (globalData.ownId > 0)
    theTraceFileName= NdbConfig_TraceFileName(globalData.ownId,
					      get_trace_no());
  NdbAutoPtr<char> tmp_aptr1(theTraceFileName);
  
  // The first 69 bytes is info about the current offset
  Uint32 noMsg = globalEmulatorData.theConfiguration->maxNoOfErrorLogs();

  maxOffset = (69 + (noMsg * MESSAGE_LENGTH));
  
  char *theErrorFileName= (char *)NdbConfig_ErrorFileName(globalData.ownId);
  NdbAutoPtr<char> tmp_aptr2(theErrorFileName);

  stream = fopen(theErrorFileName, "r+");
  if (stream == NULL) { /* If the file could not be opened. */
    
    // Create a new file, and skip the first 69 bytes, 
    // which are info about the current offset
    stream = fopen(theErrorFileName, "w");
    if(stream == NULL)
    {
      g_eventLogger->info("Unable to open error log file: %s",
                          theErrorFileName);
      return -1;
    }
    fprintf(stream, "%s%u%s", "Current byte-offset of file-pointer is: ", 69,
	    "                        \n\n\n");   
    
    // ...and write the error-message...
    formatMessage(thr_no,
                  threadCount, thrdMessageID,
                  thrdProblemData, thrdObjRef,
                  theTraceFileName, theMessage);
    fprintf(stream, "%s", theMessage);
    fflush(stream);
    
    /* ...and finally, at the beginning of the file, 
       store the position where to
       start writing the next message. */
    offset = ftell(stream);
    // If we have not reached the maximum number of messages...
    if (offset <= (maxOffset - MESSAGE_LENGTH)){
      fseek(stream, 40, SEEK_SET);
      // ...set the current offset...
      fprintf(stream,"%d", offset);
    } else {
      fseek(stream, 40, SEEK_SET);
      // ...otherwise, start over from the beginning.
      fprintf(stream, "%u%s", 69, "             ");
    }
  } else {
    // Go to the latest position in the file...
    fseek(stream, 40, SEEK_SET);
    if (fscanf(stream, "%u", &offset) != 1)
    {
      abort();
    }

    /* In case of upgrade from 500 -> 999 message length,
     * check if an odd number of messages have been written
     * from the start of the file. If so, increase the offset
     * by the length of 1 message (of old length) to simulate
     * even number of messages being written previously.
     * This will ensure clean overwriting of messages of
     * new length when the error log loops back to the
     * beginning of the file.
     */
    bool oddNoOfMessages = false;
    if(((offset - 69) / OLD_MESSAGE_LENGTH) % 2 == 1 )
      oddNoOfMessages = true;
    if (oddNoOfMessages)
    {
      /* Adjust the offset by the length (old) of 1
       * message. Also check if the maximum number of
       * messages has been reached.
       */
      offset = offset + 499;
      if (offset > (maxOffset - MESSAGE_LENGTH))
        offset = 69;
    }
    fseek(stream, offset, SEEK_SET);
    
    // ...and write the error-message there...
    formatMessage(thr_no,
                  threadCount, thrdMessageID,
                  thrdProblemData, thrdObjRef,
                  theTraceFileName, theMessage);
    fprintf(stream, "%s", theMessage);
    fflush(stream);
    
    /* ...and finally, at the beginning of the file, 
       store the position where to
       start writing the next message. */
    offset = ftell(stream);
    
    // If we have not reached the maximum number of messages...
    if (offset <= (maxOffset - MESSAGE_LENGTH)){
      fseek(stream, 40, SEEK_SET);
      // ...set the current offset...
      fprintf(stream,"%d", offset);
    } else {
      fseek(stream, 40, SEEK_SET);
      // ...otherwise, start over from the beginning.
      fprintf(stream, "%u%s", 69, "             ");
    }
  }
  fflush(stream);
  fclose(stream);

  prepare_to_crash(false, (nst == NST_ErrorInsert));

#ifdef ERROR_INSERT
  if (simulate_error_during_error_reporting == 1)
  {
    fprintf(stderr, "Stall during error reporting after releasing lock\n");
    NdbSleep_MilliSleep(12000);
  }
#endif

  if (theTraceFileName) {
    /* Attempt to stop all processing to be able to dump a consistent state. */
    globalScheduler.traceDumpPrepare(nst);

    char *traceFileEnd = theTraceFileName + strlen(theTraceFileName);
    for (Uint32 i = 0; i < threadCount; i++)
    {
      // Open the tracefile
      if (i > 0)
        sprintf(traceFileEnd, "_t%u", i);
      FILE *jamStream = fopen(theTraceFileName, "w");

      // Get jam status, buffer and index for current thread.
      dumpJam_ok = globalScheduler.traceDumpGetJam(i, dumpJam_buffer,
                                                   dumpJam_oldest);
      /**
       * dumpJam_cursor is the last jam event of the next signal to be printed.
       * For ease of use it's kept in the range
       * dumpJam_oldest <= dumpJam_cursor < dumpJam_oldest + EMULATED_JAM_SIZE
       * so JAM_MASK must be applied whenever it's used.
       */
      dumpJam_cursor = dumpJam_oldest + EMULATED_JAM_SIZE - 1;
      // Ensure that the above won't overflow.
      static_assert(((Uint32) (EMULATED_JAM_SIZE << 1)) > EMULATED_JAM_SIZE);
      fprintf(jamStream,
              "Dump of signal and jam information, NEWEST signal first.\n"
              "See legend at end of file.\n"
              );
      /**
       * Dump all known signals together with their respective jams. Note that
       * for ndbmtd, this uses FastScheduler::dumpSignalMemoryAndJam in
       * ../vm/mt.cpp, *not* in ../vm/FastScheduler.cpp.
       */
      globalScheduler.dumpSignalMemoryAndJam(i, jamStream);

      fprintf(jamStream,
              "\n"
              "Legend:\n"
              "r.*: Receiver*\n"
              "s.*: Sender*\n"
              ".bn: BlockNumber/BlockInstance \"BlockName\"\n"
              " - BlockNumber is the type of block.\n"
              " - BlockInstance is the instance Id for the block. Together with"
              " nodeId and\n   BlockNumber it will uniquely identify a block."
              " Will not be printed for packed\n   signals.\n"
              " - BlockName describes the type of block. It's a function of"
              " BlockNumber.\n"
              ".nodeId: Node ID.\n"
              ".sigId: Sequence number for signal, applied at the receiving"
              " side.\n"
              ".gsn: GlobalSignalNumber, the type of signal.\n"
              ".sn: \"SignalName\"\n"
              "  SignalName describes the type of signal. For non-packed"
              " signals it's a\n  function of GlobalSignalNumber. Packed"
              " signals don't have a\n  GlobalSignalNumber.\n"
              ".threadId: The thread Id.\n"
              "  Will only be printed for local (same-node) signals.\n"
              ".threadSigId: Sequence number for local (same-node) signals."
              " Will only be\n  incremented for JBB priority signals. Will only"
              " be printed for local\n  (same-node) signals.\n"
              "prio: Priority for signal.\n"
              "  JBA = High priority\n"
              "  JBB = Normal priority\n"
              "length: Number of Uint32 words in signal payload\n"
              "trace: Trace number between 0-63\n"
              "#sec: Number of sections in signal\n"
              "fragInfo: Details about signal fragmentation.\n"
              "  0 = Not fragmented\n"
              "  1 = First fragment\n"
              "  2 = Fragment other than first or last\n"
              "  3 = Last fragment\n"
              );
      fclose(jamStream);
    }
  }

  return 0;
}

constexpr bool less_bits(Uint32 x, Uint32 y, int b)
{
  // Our best guess for (x < y) while accounting for wraparound and that x
  // and/or y only has the b lowest bits set correctly.
  return ((x - y) & (1 << (b - 1)));
}
static_assert(!less_bits(0x3fffffff, 0xffffffff, 30));
static_assert(!less_bits(0x3fffffff, 0x3fffffff, 30));
static_assert(!less_bits(0x3fffffff, 0x7fffffff, 30));
static_assert(!less_bits(0x3fffffff, 0xbfffffff, 30));
static_assert( less_bits(0x3fffffff, 0x00000000, 30));
static_assert( less_bits(0x3fffffff, 0x40000000, 30));
static_assert( less_bits(0x3fffffff, 0x80000000, 30));
static_assert( less_bits(0x3fffffff, 0xc0000000, 30));
static_assert(!less_bits(0x00000000, 0x00000000, 30));
static_assert(!less_bits(0x00000000, 0x40000000, 30));
static_assert(!less_bits(0x00000000, 0x80000000, 30));
static_assert(!less_bits(0x00000000, 0xc0000000, 30));
static_assert(!less_bits(0x00000001, 0x00000000, 30));
static_assert(!less_bits(0x00000001, 0x40000000, 30));
static_assert(!less_bits(0x00000001, 0x80000000, 30));
static_assert(!less_bits(0x00000001, 0xc0000000, 30));
static_assert( less_bits(0x00000001, 0x00000002, 30));
static_assert( less_bits(0x00000001, 0x40000002, 30));
static_assert( less_bits(0x00000001, 0x80000002, 30));
static_assert( less_bits(0x00000001, 0xc0000002, 30));
static_assert( less_bits(0x12345678, 0x9abcdef0, 25));
static_assert(!less_bits(0x13345678, 0x9abcdef0, 25));
static_assert(!less_bits(0x12345678, 0x9bbcdef0, 25));
static_assert( less_bits(0x13345678, 0x9bbcdef0, 25));
static_assert(!less_bits(0x13345678, 0x9bbcdef0,  5));
static_assert( less_bits(0x13345678, 0x9bbcdee0,  5));
static_assert(!less_bits(0x13345668, 0x9bbcdee0,  5));

/**
 * Given the index for the last jam event for a signal, return the index for
 * the first. Both the argument and the return value is in the range
 * dumpJam_oldest <= X < dumpJam_oldest + EMULATED_JAM_SIZE
 */
Uint32
ErrorReporter::startOfJamSignal(Uint32 endIndex)
{
  for(Uint32 idx = endIndex; dumpJam_oldest <= idx; idx--)
  {
    JamEvent::JamEventType type = dumpJam_buffer[idx & JAM_MASK].getType();
    if(type == JamEvent::STARTOFSIG || type == JamEvent::STARTOFPACKEDSIG)
    {
      return idx;
    }
  }
  return dumpJam_oldest;
}

/**
 * Maybe dump a jam log table for one signal, then return whether a dump was
 * made. Dumping happens if there is still available jam data and one of:
 * - syncMethod==0.
 * - syncMethod==1 and the Signal Id for the next signal in the jam log is
     either unavailable or not less than syncValue.
 * - syncMethod==2 and the pack index for the next signal in the jam log is
     available and is not less than syncValue.
 */
bool
ErrorReporter::dumpOneJam(FILE *jamStream, int syncMethod, Uint32 syncValue,
                          const char* prefix) {
#ifndef NO_EMULATED_JAM
  if (!dumpJam_ok)
  {
    return false;
  }
  /**
   * Variables used as indices to the jam buffer are kept in the range
   * dumpJam_oldest <= X < dumpJam_oldest + EMULATED_JAM_SIZE
   * so JAM_MASK must be applied whenever they are used. This applies to
   * - dumpJam_cursor
   * - lastIndex
   * - firstIndex
   * - idx
   */
  const JamEvent nextJamMarker =
    dumpJam_buffer[startOfJamSignal(dumpJam_cursor) & JAM_MASK];
  JamEvent::JamEventType nextJamMarkerType = nextJamMarker.getType();
  const Uint32 nextId = nextJamMarker.getSignalId();
  const Uint32 nextIdx = nextJamMarker.getPackedIndex();
  if (!(syncMethod == 0
        || (syncMethod == 1
            && (nextJamMarkerType != JamEvent::STARTOFSIG
                || !less_bits(nextId, syncValue, 30))
            &&  (nextJamMarkerType != JamEvent::STARTOFPACKEDSIG
                 || !less_bits(nextId, syncValue, 25)))
        || (syncMethod == 2
            && nextJamMarkerType == JamEvent::STARTOFPACKEDSIG
            && !less_bits(nextIdx, syncValue, 5))))
  {
    return false;
  }

  Uint32 lastIndex = dumpJam_cursor;
  Uint32 firstIndex = startOfJamSignal(lastIndex);
  bool atOldestSignalInJam = firstIndex == dumpJam_oldest ||
    dumpJam_buffer[(firstIndex - 1) & JAM_MASK].getType() == JamEvent::EMPTY;
  bool hasLineOrDataEntries = false;
  if (atOldestSignalInJam)
  {
    dumpJam_ok = false;
  }
  dumpJam_cursor = firstIndex - 1;
  for (Uint32 idx = firstIndex; idx <= lastIndex; idx++)
  {
    const JamEvent thisJamEvent = dumpJam_buffer[idx & JAM_MASK];
    if(thisJamEvent.getType() == JamEvent::LINE ||
       thisJamEvent.getType() == JamEvent::DATA)
    {
      hasLineOrDataEntries = true;
      break;
    }
  }

  fprintf(jamStream, "%s", prefix);

  // print header
  const Uint32 maxCols = 6;
  const JamEvent firstJamEvent = dumpJam_buffer[firstIndex & JAM_MASK];
  if (firstJamEvent.getType() == JamEvent::STARTOFSIG)
  {
    Uint32 firstJamSignalId = firstJamEvent.getSignalId();
    fprintf(jamStream, "    ---- Signal H\'%.8x: Jam content, OLDEST first ----\n",
            firstJamSignalId);
  }
  else if (firstJamEvent.getType() == JamEvent::STARTOFPACKEDSIG)
  {
    Uint32 firstJamSignalId = firstJamEvent.getSignalId();
    Uint32 firstJamPackedIdx = firstJamEvent.getPackedIndex();
    fprintf(jamStream, "    ---- Signal H\'%.8x, Packed signal %d: Jam content, OLDEST first ----\n",
            firstJamSignalId, firstJamPackedIdx);
  }
  else
  {
    fprintf(jamStream, "    ---- Unknown signal:"
            " Jam content, OLDEST first ----\n"
            "         WARNING: Oldest jams might be lost. No start of signal marker found.\n");
  }
  if (hasLineOrDataEntries)
  {
    fprintf(jamStream, "    %-33s %s", "SOURCE FILE",
            "LINE NUMBERS ##### OR DATA d#####");
  }
  else
  {
    fprintf(jamStream, "         No jam entries other than start of signal");
  }

  // loop over all entries of the current signal
  Uint32 col = 0;
  Uint32 fileId = ~0;

  for (Uint32 idx = firstIndex; idx <= lastIndex; idx++)
  {
    globalData.incrementWatchDogCounter(4);	// watchdog not to kill us ?
    const JamEvent aJamEvent = dumpJam_buffer[idx & JAM_MASK];
    if (aJamEvent.getType() == JamEvent::LINE ||
        aJamEvent.getType() == JamEvent::DATA)
    {
      if (aJamEvent.getFileId() != fileId)
      {
        fileId = aJamEvent.getFileId();
        const char* const fileName = aJamEvent.getFileName();
        if (fileName != NULL)
        {
          fprintf(jamStream, "\n    %-33s", fileName);
        }
        else
        {
          /** 
           * Getting here indicates that there is a JAM_FILE_ID without a
           * corresponding entry in jamFileNames.
           */
          fprintf(jamStream, "\n    unknown_file_%05u               ", fileId);
        }
        col = 0;
      }
      else if (col==0)
      {
        fprintf(jamStream, "\n    %-33s", "");
      }
      fprintf(jamStream, (aJamEvent.getType() == JamEvent::LINE) ? "  %05u" : " d%05u",
              aJamEvent.getLineNoOrData());
      col = (col+1) % maxCols;
    }
  }
  if (atOldestSignalInJam)
  {
    fprintf(jamStream,
            "\n\n-------------- "
            "END OF JAM CONTENT. If there are more signals to"
            "   --------------\n-------------- "
            "print, the corresponding jam information is lost."
            "  --------------");
  }
  fprintf(jamStream, "\n");
  fflush(jamStream);
  return true;
#endif // ifndef NO_EMULATED_JAM
}
