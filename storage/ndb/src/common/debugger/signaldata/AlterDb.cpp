/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

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

#include <signaldata/AlterDb.hpp>

#define JAM_FILE_ID 560

bool printALTER_DB_REQ(FILE *output,
                       const Uint32 *theData,
                       Uint32 len,
                       Uint16 /*receiverBlockNo*/)
{
  const AlterDbReq *const sig = (const AlterDbReq *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " databaseId: %.8u, databaseVersion: %.8u\n", 
	  sig->databaseId, sig->databaseVersion);
  fprintf(output, " requestType: H\'%.8x\n", sig->requestType);
  fprintf(output, " inMemorySizeMB: %.8u, diskSpaceSizeGB: %.8u\n",
          sig->inMemorySizeMB, sig->diskSpaceSizeGB);
  fprintf(output, " ratePerSec: %.8u\n", sig->ratePerSec);

  if (len > AlterDbReq::SignalLengthLQH) {
    fprintf(output, "maxTransactionSize: %.8u", sig->maxTransactionSize);
    fprintf(output, ", maxParallelTransactions: %.8u, maxParallelComplexQueries: %.8u\n",
            sig->maxParallelTransactions, sig->maxParallelComplexQueries);
  }
  return true;
}

bool printALTER_DB_CONF(FILE *output,
                        const Uint32 *theData,
                        Uint32 /* len */,
                        Uint16 /*receiverBlockNo*/)
{
  const AlterDbConf *const sig = (const AlterDbConf *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " databaseId: %.8u\n", 
	  sig->databaseId);
  return true;
}

bool printALTER_DB_REF(FILE *output,
                       const Uint32 *theData,
                       Uint32 /* len */,
                       Uint16 /*receiverBlockNo*/)
{
  const AlterDbRef *const sig = (const AlterDbRef *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " databaseId: %.8u\n", 
	  sig->databaseId);
  fprintf(output, " errorCode: %.8u, errorLine: %.8u\n", 
	  sig->errorCode, sig->errorLine);
  fprintf(output, " errorKey: %.8u, errorStatus: %.8u\n", 
	  sig->errorKey, sig->errorStatus);
  return true;
}
