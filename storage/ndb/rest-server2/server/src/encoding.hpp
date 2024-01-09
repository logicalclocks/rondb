/*
 * Copyright (C) 2023 Hopsworks AB
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

#include <drogon/HttpTypes.h>
#include <drogon/drogon.h>
#include <iostream>

RS_Status create_native_request(PKReadParams &, void *, void *);

RS_Status process_pkread_response(void *, PKReadResponseJSON &);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_ENCODING_HPP_
