/*
   Copyright (c) 2003, 2021, Oracle and/or its affiliates.
   Copyright (c) 2022, 2022, Hopsworks and/or its affiliates.

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

#include <signaldata/CommitReq.hpp>

#define JAM_FILE_ID 549

bool
printCOMMITREQ(FILE * output,
               const Uint32 * theData,
               Uint32 /*len*/,
               Uint16 /*receiverBlockNo*/)
{
  const CommitReq * const sig = (const CommitReq *) theData;
  fprintf(output,
    " ReqPtr = H\'%.8x reqBlockRef = H\'%.8x"
    " transId1 = H\'%.8x transId2 = H\'%.8x\n"
    " gci_lo = H\'%.8x gci_hi = H\'%.8x"
    " tcOprec = H\'%.8x old_blockref = H\'%.8x\n",
          sig->reqPtr,
          sig->reqBlockref,
          sig->transid1,
          sig->transid2,
          sig->gci_lo,
          sig->gci_hi,
          sig->tcOprec,
          sig->old_blockref
          );
  return true;
}

bool
printCOMMIT(FILE * output,
            const Uint32 * theData,
            Uint32 /*len*/,
            Uint16 /*receiverBlockNo*/)
{
  const Commit * const sig = (const Commit *) theData;
  fprintf(output,
    " tcConnectPtr = H\'%.8x"
    " transId1 = H\'%.8x transId2 = H\'%.8x\n"
    " gci_lo = H\'%.8x gci_hi = H\'%.8x",
          sig->tcConnectPtr,
          sig->transid1,
          sig->transid2,
          sig->gci_lo,
          sig->gci_hi
          );
  return true;
}

bool
printCOMMITTED(FILE * output,
              const Uint32 * theData,
              Uint32 /*len*/,
              Uint16 /*receiverBlockNo*/)
{
  const Committed * const sig = (const Committed *) theData;
  fprintf(output,
    " tcConnectPtr = H\'%.8x"
    " transId1 = H\'%.8x transId2 = H\'%.8x\n",
          sig->tcConnectPtr,
          sig->transid1,
          sig->transid2
          );
  return true;
}

bool
printCOMMITCONF(FILE * output,
                const Uint32 * theData,
                Uint32 /*len*/,
                Uint16 /*receiverBlockNo*/)
{
  const CommitConf * const sig = (const CommitConf *) theData;
  fprintf(output,
    " tcConnectPtr = H\'%.8x senderNodeId = H\'%.8x"
    " transId1 = H\'%.8x transId2 = H\'%.8x\n",
          sig->tcConnectPtr,
          sig->senderNodeId,
          sig->transid1,
          sig->transid2
          );
  return true;
}

bool
printCOMPLETEREQ(FILE * output,
               const Uint32 * theData,
               Uint32 /*len*/,
               Uint16 /*receiverBlockNo*/)
{
  const CompleteReq * const sig = (const CompleteReq *) theData;
  fprintf(output,
    " ReqPtr = H\'%.8x reqBlockRef = H\'%.8x\n"
    " transId1 = H\'%.8x transId2 = H\'%.8x"
    " tcOprec = H\'%.8x old_blockref = H\'%.8x\n",
          sig->reqPtr,
          sig->reqBlockref,
          sig->transid1,
          sig->transid2,
          sig->tcOprec,
          sig->old_blockref
          );
  return true;
}

bool
printCOMPLETE(FILE * output,
              const Uint32 * theData,
              Uint32 /*len*/,
              Uint16 /*receiverBlockNo*/)
{
  const Complete * const sig = (const Complete *) theData;
  fprintf(output,
    " tcConnectPtr = H\'%.8x"
    " transId1 = H\'%.8x transId2 = H\'%.8x\n",
          sig->tcConnectPtr,
          sig->transid1,
          sig->transid2
          );
  return true;
}

bool
printCOMPLETED(FILE * output,
               const Uint32 * theData,
               Uint32 /*len*/,
               Uint16 /*receiverBlockNo*/)
{
  const Completed * const sig = (const Completed *) theData;
  fprintf(output,
    " tcConnectPtr = H\'%.8x"
    " transId1 = H\'%.8x transId2 = H\'%.8x\n",
          sig->tcConnectPtr,
          sig->transid1,
          sig->transid2
          );
  return true;
}

bool
printCOMPLETECONF(FILE * output,
                  const Uint32 * theData,
                  Uint32 /*len*/,
                  Uint16 /*receiverBlockNo*/)
{
  const CompleteConf * const sig = (const CompleteConf *) theData;
  fprintf(output,
    " tcConnectPtr = H\'%.8x senderNodeId = H\'%.8x"
    " transId1 = H\'%.8x transId2 = H\'%.8x\n",
          sig->tcConnectPtr,
          sig->senderNodeId,
          sig->transid1,
          sig->transid2
          );
  return true;
}
