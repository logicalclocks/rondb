/*
   Copyright (c) 2023, 2023, Hopsworks and/or its affiliates.

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

#include <signaldata/CopyActive.hpp>

bool
printCOPY_ACTIVEREQ(FILE * output, const Uint32 * theData, Uint32, Uint16)
{
  const CopyActiveReq* sig = (const CopyActiveReq*)theData;
  
  fprintf(output, "userPtr: %u userRef: %x",
	  sig->userPtr, sig->userRef);
  fprintf(output, " tableId: %u fragId: %u", sig->tableId, sig->fragId);
  fprintf(output, " distributionKey: %u flags: %x\n",
	  sig->distributionKey, sig->flags);
  return true;
}

bool
printCOPY_ACTIVECONF(FILE * output, const Uint32 * theData, Uint32, Uint16)
{
  const CopyActiveConf* sig = (const CopyActiveConf*)theData;
  
  fprintf(output, "userPtr: %u startingNodeId: %u",
	  sig->userPtr, sig->startingNodeId);
  fprintf(output, " tableId: %u fragId: %u", sig->tableId, sig->fragId);
  fprintf(output, " startGci: %u\n",
	  sig->startGci);
  return true;
}

bool
printCOPY_ACTIVEREF(FILE * output, const Uint32 * theData, Uint32, Uint16)
{
  const CopyActiveRef* sig = (const CopyActiveRef*)theData;
  
  fprintf(output, "userPtr: %u startingNodeId: %u",
	  sig->userPtr, sig->startingNodeId);
  fprintf(output, " tableId: %u fragId: %u", sig->tableId, sig->fragId);
  fprintf(output, " errorCode: %u\n",
	  sig->errorCode);
  return true;
}
