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

#include <string.h>
#include <iostream>
#include "src/logger.hpp"

Callbacks my_cb_fns;

void setLogCallBackFns(const Callbacks cbs) {
  my_cb_fns.logger = cbs.logger;
}

void log(const int level, const char *msg) {
  if (my_cb_fns.logger != nullptr) {
    RS_LOG_MSG log_msg;
    log_msg.level = level;
    strncpy(log_msg.message, msg, RS_LOG_MSG_LEN - 1);
    log_msg.message[RS_LOG_MSG_LEN - 1] = 0;
    my_cb_fns.logger(log_msg);
  } else {
    std::cout << msg << std::endl;
  }
}

void LOG_PANIC(const char *msg) {
  log(PanicLevel, msg);
}

void LOG_PANIC(const std::string msg) {
  log(PanicLevel, msg.c_str());
}

void LOG_FATAL(const char *msg) {
  log(FatalLevel, msg);
}

void LOG_FATAL(const std::string msg) {
  log(FatalLevel, msg.c_str());
}

void LOG_ERROR(const char *msg) {
  log(ErrorLevel, msg);
}

void LOG_ERROR(const std::string msg) {
  log(ErrorLevel, msg.c_str());
}

void LOG_WARN(const char *msg) {
  log(WarnLevel, msg);
}

void LOG_WARN(const std::string msg) {
  log(WarnLevel, msg.c_str());
}

void LOG_INFO(const char *msg) {
  log(InfoLevel, msg);
}

void LOG_INFO(const std::string msg) {
  log(InfoLevel, msg.c_str());
}

void LOG_DEBUG(const char *msg) {
  log(DebugLevel, msg);
}

void LOG_DEBUG(const std::string msg) {
  log(DebugLevel, msg.c_str());
}

void LOG_TRACE(char *msg) {
  log(TraceLevel, msg);
}
