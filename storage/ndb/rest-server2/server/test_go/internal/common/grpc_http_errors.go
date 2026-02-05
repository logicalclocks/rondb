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

import (
	"net/http"

	"google.golang.org/grpc/codes"
)

func HttpStatusToGrpcCode(httpStatus int) codes.Code {
	switch httpStatus {
	case http.StatusOK:
		return codes.OK
	case http.StatusUnauthorized:
		return codes.PermissionDenied
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusNotFound:
		return codes.NotFound
	default:
		return codes.Internal
	}
}

func GrpcCodeToHttpStatus(grpcCode codes.Code) int {
	switch grpcCode {
	case codes.OK:
		return http.StatusOK
	case codes.PermissionDenied:
		return http.StatusUnauthorized
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
