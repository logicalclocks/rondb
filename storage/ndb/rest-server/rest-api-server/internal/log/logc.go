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
		Errorf("Unknown log level '%d' for this message: %s", level, msg)
	}
}

func main() {}
