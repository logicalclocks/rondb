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

#include <signaldata/PackedSignal.hpp>
#include <signaldata/LqhKey.hpp>
#include <signaldata/FireTrigOrd.hpp>
#include <debugger/DebuggerNames.hpp>

#include <EventLogger.hpp>

Uint32 packedSignalLength(Uint32 signalType)
{
  switch (signalType)
  {
  case ZCOMMIT:
  {
    return 5;
  }
  case ZCOMPLETE:
  {
    return 3;
  }
  case ZCOMMITTED:
  {
    return 3;
  }
  case ZCOMPLETED:
  {
    return 3;
  }
  case ZLQHKEYCONF:
  {
    return LqhKeyConf::SignalLength;
  }
  case ZREMOVE_MARKER:
  {
    return 3;
  }
  case ZFIRE_TRIG_REQ:
  {
    return FireTrigReq::SignalLength;
  }
  case ZFIRE_TRIG_CONF:
  {
    return FireTrigConf::SignalLength;
  }
  default:
  {
    return RNIL;
  }
  }
}

const char* packedSignalName(Uint32 signalType, bool failApi)
{
  switch (signalType)
  {
  case ZCOMMIT:
  {
    return "COMMIT";
  }
  case ZCOMPLETE:
  {
    return "COMPLETE";
  }
  case ZCOMMITTED:
  {
    return "COMMITTED";
  }
  case ZCOMPLETED:
  {
    return "COMPLETED";
  }
  case ZLQHKEYCONF:
  {
    return "LQHKEYCONF";
  }
  case ZREMOVE_MARKER:
  {
    return failApi ? "REMOVE_MARKER_FAIL_API" : "REMOVE_MARKER";
  }
  case ZFIRE_TRIG_REQ:
  {
    return "FIRE_TRIG_REQ";
  }
  case ZFIRE_TRIG_CONF:
  {
    return "FIRE_TRIG_CONF";
  }
  default:
  {
    return 0;
  }
  }
}

bool
printPACKED_SIGNAL(FILE * output,
                   const Uint32 * theData,
                   Uint32 len,
                   Uint16 /*receiverBlockNo*/)
{
  assert(len <= 25);
  Uint32 sigOffsets[25];
  int numberOfSignals = 0;
  for (Uint32 i = 0; i < len;)
  {
    sigOffsets[numberOfSignals] = i;
    numberOfSignals++;
    Uint32 signalType = PackedSignal::getSignalType(theData[i]);
    switch (signalType)
    {
    case ZCOMMIT:
    case ZCOMPLETE:
    case ZCOMMITTED:
    case ZCOMPLETED:
    case ZLQHKEYCONF:
    case ZREMOVE_MARKER:
    case ZFIRE_TRIG_REQ:
    case ZFIRE_TRIG_CONF:
    {
      i += packedSignalLength(signalType);
      break;
    }
    default:
    {
      // Stop processing
      i = len;
      break;
    }
    }
  }
  // Print each signal separately
  for (int i = -1; i < numberOfSignals; i++)
  {
    // Reverse the print order if we are in signal memory dump.
    int idx = globalIsInCrashlog ? (numberOfSignals - i - 2) : i;
    /**
     * Print the main signal when idx == -1 and the packed signals when
     * 0 <= idx < numberOfSignals. We handle the special case idx == -1 inside
     * the loop to make it easier to reverse the order.
     */
    if (idx == -1) // Print the main signal
    {
      printHex(output, theData, len, "Signal data:");
    }
    if (idx == (globalIsInCrashlog ? numberOfSignals - 1 : 0))
    {
      fprintf(output, "  -------- Begin Packed Signals --------\n");
    }
    if (0 <= idx) // Print a packed signal
    {
      Uint32 offset = sigOffsets[idx];
      Uint32 signalType = PackedSignal::getSignalType(theData[offset]);
      Uint32 signalLength = packedSignalLength(signalType);
      const char* signalName =
        packedSignalName(signalType, theData[offset] & 1);
      // If you change the print format, then update the legend in
      // ErrorReporter::WriteMessage to match.
      fprintf(output, "  ----------- Packed signal %d -----------\n", idx);
      fprintf(output, "  length: %u, r.sn: \"%s\"\n", signalLength, signalName);
      switch (signalType)
      {
      case ZCOMMIT:
      case ZCOMPLETE:
      case ZCOMMITTED:
      case ZCOMPLETED:
      case ZLQHKEYCONF:
      {
        printHex(output, &theData[offset], signalLength, "  Signal data:");
        break;
      }
      case ZREMOVE_MARKER:
      {
        // Skip first word!
        printHex(output, &theData[offset + 1], signalLength - 1,
                 "  Signal data:");
        break;
      }
      case ZFIRE_TRIG_REQ:
      case ZFIRE_TRIG_CONF:
      {
        break;
      }
      default:
      {
        fprintf(output, "  ----------- Packed signal %d -----------\n", idx);
        fprintf(output, "  Unknown signal type H\'%.8x.\n", signalType);
        fprintf(output,
                "  There might be more packed signals with indices higher than"
                " %d.\n  If so, they are not printed.\n", idx);
        break;
      }
      }
      if (globalIsInCrashlog)
      {
        // Print the jam table for one signal, using the packed index for
        // synchronization.
        globalDumpOneJam(output, 2, idx, "");
      }
    }
    if (idx == (globalIsInCrashlog ? 0 : numberOfSignals - 1))
    {
      fprintf(output, "  --------- End Packed Signals ---------\n");
    }
  }//for
  return true;
}

bool
PackedSignal::verify(const Uint32* data, Uint32 len, Uint32 receiverBlockNo, 
                     Uint32 typesExpected, Uint32 commitLen)
{
  Uint32 pos = 0;
  bool bad = false;

  if (unlikely(len > 25))
  {
    g_eventLogger->info("Bad PackedSignal length : %u", len);
    bad = true;
  }
  else
  {
    while ((pos < len) && ! bad)
    {
      Uint32 sigType = data[pos] >> 28;
      if (unlikely(((1 << sigType) & typesExpected) == 0))
      {
        g_eventLogger->info(
            "Unexpected sigtype in packed signal: %u at pos %u. Expected : %u",
            sigType, pos, typesExpected);
        bad = true;
        break;
      }
      switch (sigType)
      {
      case ZCOMMIT:
        assert(commitLen > 0);
        pos += commitLen;
        break;
      case ZCOMPLETE:
        pos+= 3;
        break;
      case ZCOMMITTED:
        pos+= 3;
        break;
      case ZCOMPLETED:
        pos+= 3;
        break;
      case ZLQHKEYCONF:
        pos+= LqhKeyConf::SignalLength;
        break;
      case ZREMOVE_MARKER:
        pos+= 3;
        break;
      case ZFIRE_TRIG_REQ:
        pos+= FireTrigReq::SignalLength;
        break;
      case ZFIRE_TRIG_CONF:
        pos+= FireTrigConf::SignalLength;
        break;
      default :
        g_eventLogger->info("Unrecognised signal type %u at pos %u", sigType,
                            pos);
        bad = true;
        break;
      }
    }
    
    if (likely(pos == len))
    {
      /* Looks ok */
      return true;
    }
    
    if (!bad)
    {
      g_eventLogger->info(
          "Packed signal component length (%u) != total length (%u)",
          pos, len);
    }
  }

  printPACKED_SIGNAL(stderr, data, len, receiverBlockNo);
  
  return false;
}
