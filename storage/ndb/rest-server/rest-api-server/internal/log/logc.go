/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
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

package log

/*
#cgo CFLAGS: -g -Wall
#include <stdlib.h>
#include <stdbool.h>
#include "./../../../data-access-rondb/src/rdrs-dal.h"
extern void loggerCToGo(RS_LOG_MSG log);
*/
import "C"
import (
	log "github.com/sirupsen/logrus"
)

var cCallbacks C.Callbacks

func RegisterLogCallBack() {
	cCallbacks = C.Callbacks{}
	cCallbacks.logger = C.LogCallBackFn(C.loggerCToGo)
	C.register_callbacks(cCallbacks)
}

//export goLog
func goLog(logMsg C.RS_LOG_MSG) {
	level := log.Level(logMsg.level)
	msg := C.GoString(&logMsg.message[0])

	switch level {
	case log.PanicLevel:
		Panic(msg)
	case log.FatalLevel:
		Fatal(msg)
	case log.ErrorLevel:
		Error(msg)
	case log.WarnLevel:
		Warn(msg)
	case log.InfoLevel:
		Info(msg)
	case log.DebugLevel:
		Debug(msg)
	case log.TraceLevel:
		Trace(msg)
	default:
		Error("Please fix log level for this message: " + msg)
	}
}

func main() {}
