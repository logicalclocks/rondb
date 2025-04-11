// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

/*
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef PINK_THREAD_NAME_H
#define PINK_THREAD_NAME_H

#include <string>
#include <pthread.h>

namespace pink {

#if defined(__GLIBC__) && !defined(__APPLE__) && !defined(__ANDROID__)
# if __GLIBC_PREREQ(2, 12)
  // has pthread_setname_np(pthread_t, const char*) (2 params)
# define HAS_PTHREAD_SETNAME_NP 1
# endif
#endif

#ifdef HAS_PTHREAD_SETNAME_NP
inline bool SetThreadName(pthread_t id, const std::string& name) {
  //printf ("use pthread_setname_np(%s)\n", name.substr(0, 15).c_str());
  return 0 == pthread_setname_np(id, name.substr(0, 15).c_str());
}
#else
inline bool SetThreadName(pthread_t id, const std::string& name) {
  //printf ("no pthread_setname\n");
  (void)id;
  (void)name;
  return false;
}
#endif
}

#endif
