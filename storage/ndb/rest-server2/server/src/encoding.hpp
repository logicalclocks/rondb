/*
 * Copyright (C) 2023, 2024 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_ENCODING_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_ENCODING_HPP_

#include "encoding_helper.hpp"
#include "pk_data_structs.hpp"
#include "rdrs_dal.h"
#include "rdrs_const.h"
#include <ArenaMalloc.hpp>

#include <drogon/HttpTypes.h>
#include <drogon/drogon.h>
#include <iostream>

struct RS_BufferChain;

/* Encode one operation request into its view. Checks an upper bound of the
 * encoded size against the view first (RONDB-1144: the encoder itself has
 * no bounds checks) and answers 400 when it does not fit. consumed is the
 * number of bytes the view must advance by, including the reservation for
 * column names when the operation names none. */
RS_Status create_native_request(PKReadParams &, RS_Buffer &view,
                                Uint32 &consumed);
/* The bound create_native_request checks against; the batch controllers
 * pass it to RS_BufferChain::next_op_view so an operation that does not
 * fit behind the previous one starts in a fresh buffer. */
Uint32 native_request_upper_bound(const PKReadParams &);
/* Decode the response of operation op_index from the chain; a blob or text
 * value that spilled is handed to the JSON stage as a segment list. */
RS_Status process_pkread_response(ArenaMalloc*,
                                  const RS_BufferChain &resp_chain,
                                  Uint32 op_index,
                                  RS_Buffer *reqBuff,
                                  PKReadResponseJSON &);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_ENCODING_HPP_
