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

package common

/*
#include "./../../../src/error_strings.h"
*/
import "C"

func ERROR_008() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_INVALID_COLUMN_DATA))
}

func ERROR_011() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_DB_TABLE_NOT_EXIST))
}

func ERROR_012() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_COLUMN_NOT_EXIST))
}

func ERROR_013() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_WRONG_PRIMARY_KEY_COUNT))
}

func ERROR_014() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_WRONG_PRIMARY_KEY_COLUMN))
}

func ERROR_001() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_WRONG_DATA_TYPE))
}

func ERROR_015() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_WRONG_DATA_TYPE))
}

func ERROR_017() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_UNSUPPORTED_HASH_INDEX))
}

func ERROR_024() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_NO_FREE_API_SLOT))
}

func ERROR_026() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_UNSUPPORTED_BLOB_TEXT_READ))
}

func ERROR_027() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_INVALID_DATE_TIME))
}

func ERROR_036() string {
	return C.GoString(C.rdrsErrorMessage(C.ERROR_RONDB_RECONNECTION_IN_PROGRESS))
}
