/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
    Use is subject to license terms.
   Copyright (c) 2021, 2021, Logical Clocks and/or its affiliates.

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

#include <signaldata/UtilDelete.hpp>

bool 
printUTIL_DELETE_REQ(FILE * out, const Uint32 * data, Uint32 l, Uint16 b){
  (void)l;  // Don't want compiler warning
  (void)b;  // Don't want compiler warning

  const UtilDeleteReq* sig = (const UtilDeleteReq*)data;
  fprintf(out, " senderData: %d prepareId: %d totalDataLen: %d\n",
	  sig->senderData,
	  sig->prepareId,
	  sig->totalDataLen);
  printHex(out, sig->attrData, 22, "");
  return true;
}

bool 
printUTIL_DELETE_CONF(FILE * out, const Uint32 * data, Uint32 l, Uint16 b){
  (void)l;  // Don't want compiler warning
  (void)b;  // Don't want compiler warning

  const UtilDeleteConf* sig = (const UtilDeleteConf*)data;
  fprintf(out, " senderData: %d\n", sig->senderData);
  return true;
}

bool 
printUTIL_DELETE_REF(FILE * out, const Uint32 * data, Uint32 l, Uint16 b){
  (void)l;  // Don't want compiler warning
  (void)b;  // Don't want compiler warning

  const UtilDeleteRef* sig = (const UtilDeleteRef*)data;
  fprintf(out, " senderData: %d\n", sig->senderData);
  fprintf(out, " errorCode: %d\n", sig->errorCode);
  return true;
}
