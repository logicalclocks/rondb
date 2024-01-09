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

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type LogConfig struct {
	Level      string
	FilePath   string
	MaxSizeMB  int
	MaxBackups int
	MaxAge     int
}

var currentLevel log.Level

func InitLogger(logConfig LogConfig) {
	SetLevel(logConfig.Level)

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
		DisableQuote:  true,
	})

	// setup log cutting
	if logConfig.FilePath != "" {
		log.SetOutput(&lumberjack.Logger{
			Filename:   logConfig.FilePath,
			MaxSize:    logConfig.MaxSizeMB,
			MaxBackups: logConfig.MaxBackups,
			MaxAge:     logConfig.MaxAge,
		})
	} else {
		log.SetOutput(os.Stdout)
	}

	RegisterLogCallBack()
}

func SetLevel(levelStr string) {
	lvl, err := log.ParseLevel(levelStr)
	if err != nil {
		log.Errorf("Invlid log level %s ", levelStr)
		lvl = log.ErrorLevel
	}
	log.SetLevel(lvl)

	currentLevel = lvl
}

func Tracef(format string, v ...interface{}) {
	if currentLevel >= log.TraceLevel { //  avoid unnecessary processing of fmt.Sprintf
		log.Trace(fmt.Sprintf(format, v...))
	}
}

func Debugf(format string, v ...interface{}) {
	if currentLevel >= log.DebugLevel {
		log.Debug(fmt.Sprintf(format, v...))
	}
}

func Infof(format string, v ...interface{}) {
	if currentLevel >= log.InfoLevel {
		log.Info(fmt.Sprintf(format, v...))
	}
}

func Warnf(format string, v ...interface{}) {
	if currentLevel >= log.WarnLevel {
		log.Warn(fmt.Sprintf(format, v...))
	}
}

func Errorf(format string, v ...interface{}) {
	if currentLevel >= log.ErrorLevel {
		log.Error(fmt.Sprintf(format, v...))
	}
}

func Fatalf(format string, v ...interface{}) {
	if currentLevel >= log.FatalLevel {
		log.Fatal(fmt.Sprintf(format, v...))
	}
}

func Panicf(format string, v ...interface{}) {
	if currentLevel >= log.PanicLevel {
		log.Panic(fmt.Sprintf(format, v...))
	}
}

func Trace(msg string) {
	if currentLevel >= log.TraceLevel {
		log.Trace(msg)
	}
}

func Debug(msg string) {
	if currentLevel >= log.DebugLevel {
		log.Debug(msg)
	}
}

func Info(msg string) {
	if currentLevel >= log.InfoLevel {
		log.Info(msg)
	}
}

func Warn(msg string) {
	if currentLevel >= log.WarnLevel {
		log.Warn(msg)
	}
}

func Error(msg string) {
	if currentLevel >= log.ErrorLevel {
		log.Error(msg)
	}
}

func Fatal(msg string) {
	if currentLevel >= log.FatalLevel {
		log.Fatal(msg)
	}
}

func Panic(msg string) {
	if currentLevel >= log.PanicLevel {
		log.Panic(msg)
	}
}

func IsTrace() bool {
	return currentLevel >= log.TraceLevel
}

func IsDebug() bool {
	return currentLevel >= log.DebugLevel
}

func IsInfo() bool {
	return currentLevel >= log.InfoLevel
}

func IsWarn() bool {
	return currentLevel >= log.WarnLevel
}

func IsError() bool {
	return currentLevel >= log.ErrorLevel
}

func IsFatal() bool {
	return currentLevel >= log.FatalLevel
}

func IsPanic() bool {
	return currentLevel >= log.PanicLevel

}
