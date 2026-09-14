/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_RONSQL_OPERATION_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_RONSQL_OPERATION_HPP_

#include "src/status.hpp"
#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"

RS_Status ronsql_op(RonSQLExecParams& params);

/*
 * RONDB-1124: the HTTP status for a permanent RonSQL error of the given
 * class - 400 syntax / semantic / unsupported, 413 for a result or request
 * that exceeds a size limit (other limits 400), 503 resource, 500 internal.
 * Shared by ronsql_op and the controller's parse-only pre-check.
 */
HTTP_CODE ronsql_http_code_for(RonSQLErrorClass cls, const char* what);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_RONSQL_OPERATION_HPP_
