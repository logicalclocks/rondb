/*
   Copyright (c) 2005, 2023, Oracle and/or its affiliates.
   Copyright (c) 2021, 2023, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef NDB_ROPE_HPP
#define NDB_ROPE_HPP

#include <ndb_global.h>

#define JAM_FILE_ID 316

// Optimally ROPE_COPY_BUFFER_SIZE should be evenly divisible by 28 (4*sz)
#define ROPE_COPY_BUFFER_SIZE 5600

struct LcRopeHandle {
  LcRopeHandle()
  {
    m_hash = 0;
    m_length = 0;
    m_string = nullptr;
  }

  Uint32 m_hash;
  Uint32 m_length;
  char *m_string;

  Uint32 hashValue() const { return m_hash; }
};

class LcConstRope {
public:
  LcConstRope(const LcRopeHandle& handle)
    : src(handle)
  {
    m_string = src.m_string;
    m_length = src.m_length;
  }

  ~LcConstRope(){}

  Uint32 size() const;
  bool empty() const;

  void copy(char* buf, Uint32 size) const;
  bool copy(class LcLocalRope & dest);

  /* Returns number of bytes read, or 0 at EOF.
     Context is maintained in rope_offset.
     The caller must initialize rope_offset to 0 before the first read.
  */
  int readBuffered(char* buf, Uint32 buf_size, Uint32 & rope_offset) const;

  int compare(const char * s) const { return compare(s, (Uint32)strlen(s) + 1);}
  int compare(const char *, Uint32 len) const; 

  bool equal(const LcConstRope& r2) const;

private:
  const LcRopeHandle & src;
  Uint32 m_length;
  char *m_string;
};

class LcLocalRope {
public:
  LcLocalRope(LcRopeHandle& handle)
    : src(handle)
  {
    m_string = src.m_string;
    m_length = src.m_length;
    m_hash = src.m_hash;
  }
  
  ~LcLocalRope()
  {
    src.m_string = m_string;
    src.m_length = m_length;
    src.m_hash = m_hash;
  }

  Uint32 size() const;
  bool empty() const;

  void copy(char* buf, Uint32 size) const;
  
  int compare(const char * s) const { return compare(s, Uint32(strlen(s) + 1));}
  int compare(const char *, Uint32 len) const; 
  
  bool assign(const char * s) { return assign(s, Uint32(strlen(s) + 1));}
  bool assign(const char * s, Uint32 l) { return assign(s, l, hash(s, l));}
  bool assign(const char *, Uint32 len, Uint32 hash);

  bool appendBuffer(const char * buf, Uint32 len);

  void erase();
  
  static Uint32 hash(const char * str, Uint32 len, Uint32 starter = 0);

private:
  Uint32 m_hash;
  Uint32 m_length;
  char *m_string;
  LcRopeHandle & src;
};

inline
Uint32
LcLocalRope::size() const {
  return m_length;
}

inline
bool
LcLocalRope::empty() const {
  return m_length == 0;
}

inline
Uint32
LcConstRope::size() const {
  return m_length;
}

inline
bool
LcConstRope::empty() const {
  return m_length == 0;
}

#undef JAM_FILE_ID

#endif

