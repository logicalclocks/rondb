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

#include <signaldata/AlterDatabase.hpp>

#define JAM_FILE_ID 557

bool printALTER_DATABASE_REQ(FILE *output,
                             const Uint32 *theData,
                             Uint32 /* len */,
                             Uint16 /*receiverBlockNo*/)
{
  const AlterDatabaseReq *const sig = (const AlterDatabaseReq *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " transId: H\'%.8x, transKey: H\'%.8x\n", 
	  sig->transId, sig->transKey);
  fprintf(output, " databaseId: %.8u, databaseVersion: %.8u\n", 
	  sig->databaseId, sig->databaseVersion);
  fprintf(output, " requestInfo: H\'%.8x, requestType: H\'%.8x\n",
          sig->requestInfo, sig->requestType);
  return true;
}

bool printALTER_DATABASE_CONF(FILE *output,
                              const Uint32 *theData,
                              Uint32 /* len */,
                              Uint16 /*receiverBlockNo*/)
{
  const AlterDatabaseConf *const sig =
    (const AlterDatabaseConf *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " databaseId: %.8u, databaseVersion: %.8u\n", 
	  sig->databaseId, sig->databaseVersion);
  fprintf(output, " transId: H\'%.8x\n", 
	  sig->transId);
  return true;
}

bool printALTER_DATABASE_REF(FILE *output,
                             const Uint32 *theData,
                             Uint32 /* len */,
                             Uint16 /*receiverBlockNo*/)
{
  const AlterDatabaseRef *const sig =
    (const AlterDatabaseRef *)theData;
  fprintf(output, " senderRef: H\'%.8x, senderData: %.8u\n", 
	  sig->senderRef, sig->senderData);
  fprintf(output, " transId: H\'%.8x\n", 
	  sig->transId);
  fprintf(output, " errorCode: %.8u, errorLine: %.8u\n", 
	  sig->errorCode, sig->errorLine);
  fprintf(output, " errorNodeId: %.8u, masterNodeId: %.8u\n", 
	  sig->errorNodeId, sig->masterNodeId);
  fprintf(output, " errorKey: %.8u, errorStatus: %.8u\n", 
	  sig->errorKey, sig->errorStatus);
  return true;
}
