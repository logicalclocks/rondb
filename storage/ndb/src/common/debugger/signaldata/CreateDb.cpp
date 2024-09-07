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

#include <signaldata/CreateDb.hpp>

#define JAM_FILE_ID 559

bool printCREATE_DB_REQ(FILE *output,
                        const Uint32 *theData,
                        Uint32 len,
                        Uint16 /*receiverBlockNo*/)
{
  const CreateDbReq *const sig = (const CreateDbReq *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " databaseId: %.8u, databaseVersion: %.8u\n", 
	  sig->databaseId, sig->databaseVersion);
  fprintf(output, " requestType: H\'%.8x\n", sig->requestType);
  fprintf(output, " inMemorySizeMB: %.8u, diskSpaceSizeGB: %.8u",
          sig->inMemorySizeMB, sig->diskSpaceSizeGB);
  fprintf(output, " ratePerSec: %.8u\n", sig->ratePerSec);
  if (len > CreateDbReq::SignalLengthLQH) {
    fprintf(output, "maxTransactionSize: %.8u", sig->maxTransactionSize);
    fprintf(output, " maxParallelTransactions: %.8u, maxParallelComplexQueries: %.8u\n",
            sig->maxParallelTransactions, sig->maxParallelComplexQueries);
  }
  return true;
}

bool printCREATE_DB_CONF(FILE *output,
                         const Uint32 *theData,
                         Uint32 /* len */,
                         Uint16 /*receiverBlockNo*/)
{
  const CreateDbConf *const sig = (const CreateDbConf *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " databaseId: %.8u\n", 
	  sig->databaseId);
  return true;
}

bool printCREATE_DB_REF(FILE *output,
                         const Uint32 *theData,
                         Uint32 /* len */,
                         Uint16 /*receiverBlockNo*/)
{
  const CreateDbRef *const sig = (const CreateDbRef *)theData;
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

bool printCOMMIT_DB_REQ(FILE *output,
                        const Uint32 *theData,
                        Uint32 /* len */,
                        Uint16 /*receiverBlockNo*/)
{
  const CommitDbReq *const sig = (const CommitDbReq *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " databaseId: %.8u\n", 
	  sig->databaseId);
  return true;
}

bool printCOMMIT_DB_CONF(FILE *output,
                         const Uint32 *theData,
                         Uint32 /* len */,
                         Uint16 /*receiverBlockNo*/)
{
  const CommitDbConf *const sig = (const CommitDbConf *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
          sig->senderRef, sig->senderData);
  fprintf(output, " databaseId: %.8u\n", 
          sig->databaseId);
  return true;
}

bool printCOMMIT_DB_REF(FILE *output,
                        const Uint32 *theData,
                        Uint32 /* len */,
                        Uint16 /*receiverBlockNo*/)
{
  const CommitDbRef *const sig = (const CommitDbRef *)theData;
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

bool printCONNECT_TABLE_DB_REQ(FILE *output,
                               const Uint32 *theData,
                               Uint32 /* len */,
                               Uint16 /*receiverBlockNo*/)
{
  const ConnectTableDbReq *const sig = (const ConnectTableDbReq *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " requestType: %.8u\n", sig->requestType);
  fprintf(output, " databaseId: %.8u, tableId: %.8u\n", 
          sig->databaseId, sig->tableId);
  return true;
}

bool printCONNECT_TABLE_DB_CONF(FILE *output,
                                const Uint32 *theData,
                                Uint32 /* len */,
                                Uint16 /*receiverBlockNo*/)
{
  const ConnectTableDbConf *const sig = (const ConnectTableDbConf *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
          sig->senderRef, sig->senderData);
  fprintf(output, " requestType: %.8u\n", sig->requestType);
  fprintf(output, " databaseId: %.8u, tableId: %.8u\n", 
          sig->databaseId, sig->tableId);
  return true;
}

bool printCONNECT_TABLE_DB_REF(FILE *output,
                               const Uint32 *theData,
                               Uint32 /* len */,
                               Uint16 /*receiverBlockNo*/)
{
  const ConnectTableDbRef *const sig = (const ConnectTableDbRef *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " requestType: %.8u\n", sig->requestType);
  fprintf(output, " databaseId: %.8u, tableId: %.ux\n", 
          sig->databaseId, sig->tableId);
  fprintf(output, " errorCode: %.8u, errorLine: %.8u\n", 
          sig->errorCode, sig->errorLine);
  fprintf(output, " errorKey: %.8u, errorStatus: %.8u\n", 
          sig->errorKey, sig->errorStatus);
  return true;
}

bool printDISCONNECT_TABLE_DB_REQ(FILE *output,
                                  const Uint32 *theData,
                                  Uint32 /* len */,
                                  Uint16 /*receiverBlockNo*/)
{
  const DisconnectTableDbReq *const sig =
    (const DisconnectTableDbReq *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " requestType: %.8u\n", sig->requestType);
  fprintf(output, " databaseId: %.8u, tableId: %.8u\n", 
          sig->databaseId, sig->tableId);
  return true;
}

bool printDISCONNECT_TABLE_DB_CONF(FILE *output,
                                   const Uint32 *theData,
                                   Uint32 /* len */,
                                   Uint16 /*receiverBlockNo*/)
{
  const DisconnectTableDbConf *const sig =
    (const DisconnectTableDbConf *)theData;
  fprintf(output, " senderRef: H\'%.8u, senderData: %.8u\n", 
          sig->senderRef, sig->senderData);
  fprintf(output, " requestType: %.8u\n", sig->requestType);
  fprintf(output, " databaseId: %.8u, tableId: %.8u\n", 
          sig->databaseId, sig->tableId);
  return true;
}

bool printDISCONNECT_TABLE_DB_REF(FILE *output,
                                  const Uint32 *theData,
                                  Uint32 /* len */,
                                  Uint16 /*receiverBlockNo*/)
{
  const DisconnectTableDbRef *const sig =
    (const DisconnectTableDbRef *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
          sig->senderRef, sig->senderData);
  fprintf(output, " requestType: %.8u\n", sig->requestType);
  fprintf(output, " databaseId: %.8u, tableId: %.8u\n", 
          sig->databaseId, sig->tableId);
  fprintf(output, " errorCode: %.8u, errorLine: %.8u\n", 
          sig->errorCode, sig->errorLine);
  fprintf(output, " errorKey: %.8u, errorStatus: %.8u\n", 
          sig->errorKey, sig->errorStatus);
  return true;
}
