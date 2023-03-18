/*

 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package dal

/*
 #include <stdlib.h>
 #include "./../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import "fmt"

type DalError struct {
	HttpCode    int
	Message     string
	ErrLineNo   int
	ErrFileName string
}

func (e *DalError) Error() string {
	return e.Message
}

func (e *DalError) VerboseError() string {
	return fmt.Sprintf("%v; File: %v, Line: %v ", e.Message, e.ErrFileName, e.ErrLineNo)
}

func cToGoRet(ret *C.RS_Status) *DalError {
	return &DalError{
		HttpCode:    int(ret.http_code),
		Message:     C.GoString(&ret.message[0]),
		ErrLineNo:   int(ret.err_line_no),
		ErrFileName: C.GoString(&ret.err_file_name[0]),
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
