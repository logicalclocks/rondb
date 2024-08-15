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

#include "log.hpp"
#include "rdrs_dal.hpp"

#include <sstream>

LogConfig::LogConfig() {
  this->level      = "warn";
  this->filePath   = "";
  this->maxSizeMb  = 100;
  this->maxBackups = 10;
  this->maxAge     = 30;
}

RS_Status LogConfig::validate() {
  // TODO Implement Me
  return CRS_Status::SUCCESS.status;
}

std::string LogConfig::string() {
  std::stringstream ss;
  ss << "level: " << this->level;
  return ss.str();
}
