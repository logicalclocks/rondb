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

#include <record_types.hpp>
#include <ndbd_malloc.hpp>
#include "util/require.h"
#include "Rope.hpp"

#define JAM_FILE_ID 330

#ifdef TEST_ROPE
#define DEBUG_ROPE 1
#undef ROPE_COPY_BUFFER_SIZE
#define ROPE_COPY_BUFFER_SIZE 24
#else
#define DEBUG_ROPE 0
#endif

/* Returns number of bytes read, or 0 at EOF */
static int
readRope(char* buf,
         Uint32 bufSize,
         Uint32 & rope_offset,
         char *obj_str,
         Uint32 obj_len)
{
  if (obj_str == nullptr ||
      rope_offset >= obj_len)
  {
    return 0;
  }
  Uint32 left_to_copy = obj_len - rope_offset;
  Uint32 copy_size = left_to_copy;
  if (left_to_copy > bufSize)
  {
    copy_size = bufSize;
  }
  memcpy(buf, &obj_str[rope_offset], copy_size);
  rope_offset += copy_size;
  return copy_size;
}

int
LcConstRope::readBuffered(char* buf,
                          Uint32 bufSize,
                          Uint32 & rope_offset) const
{
  return readRope(buf,
                  bufSize,
                  rope_offset,
                  m_string,
                  m_length);
}

void
LcConstRope::copy(char* buf, Uint32 size) const
{
  /* Assume that buffer is big enough */
  Uint32 offset = 0;
  require(size >= m_length);
  readBuffered(buf, m_length, offset);
}

Uint32
LcLocalRope::hash(const char * p, Uint32 len, Uint32 starter)
{
  Uint32 h = starter;
  const char * data = p;
  for (; len > 0; len--)
    h = (h << 5) + h + (* data++);
  return h;
}

bool LcConstRope::copy(LcLocalRope & dest)
{
  dest.erase();
  Uint32 hash_val = dest.hash(m_string, m_length);
  if (dest.assign(m_string, m_length, hash_val))
  {
    return true;
  }
  return false;
}

static
int
compare_local(const char *in_str,
              Uint32 in_len,
              const char *obj_str,
              Uint32 obj_len)
{
  Uint32 min_size = MIN(in_len, obj_len);
  int res = memcmp(in_str, obj_str, min_size);
  if (res != 0)
  {
    return res;
  }
  if (obj_len > in_len)
  {
    return +1;
  }
  else if (obj_len < in_len)
  {
    return -1;
  }
  return 0;
}

int
LcConstRope::compare(const char * str, Uint32 len) const
{
  return compare_local(str, len, m_string, m_length);
}

void
LcLocalRope::erase()
{
  if (m_string != nullptr)
  {
    lc_ndbd_pool_free(m_string);
  }
  m_length = 0;
  m_hash = 0;
  m_string = nullptr;
}

bool
LcLocalRope::appendBuffer(const char * s, Uint32 len)
{
  Uint32 tot_len = m_length + len;
  Uint32 alloc_len = ((tot_len + 3) / 4) * 4;
  char *str = (char*)lc_ndbd_pool_malloc(alloc_len + 1,
                                         RG_SCHEMA_MEMORY,
                                         0,
                                         true);
  if (str == nullptr)
  {
    return false;
  }
  if (m_string != nullptr)
  {
    memcpy(str, m_string, m_length);
  }
  memcpy(&str[m_length], s, len);
  if (m_string != nullptr)
  {
    lc_ndbd_pool_free(m_string);
  }
  m_length = tot_len;
  m_string = str;
  m_hash = hash(m_string, tot_len, 0);
  return true;
}

bool
LcLocalRope::assign(const char * s, Uint32 len, Uint32 hash)
{
  Uint32 alloc_len = ((len + 3) / 4) * 4;
  char *str = (char*)lc_ndbd_pool_malloc(alloc_len + 1,
                                         RG_SCHEMA_MEMORY,
                                         0,
                                         true);
  if (str == nullptr)
  {
    return false;
  }
  memcpy(str, s, len);
  m_length = len;
  m_string = str;
  m_hash = hash;
  return true;
}

void
LcLocalRope::copy(char* buf, Uint32 size) const
{
  Uint32 rope_offset = 0;
  require(size >= m_length);
  readRope(buf,
           size,
           rope_offset,
           m_string,
           m_length);
}

int
LcLocalRope::compare(const char * str, Uint32 len) const
{
  return compare_local(str, len, m_string, m_length);
}

bool
LcConstRope::equal(const LcConstRope& r2) const
{
  if (m_length != r2.m_length)
  {
    return false;
  }
  if (m_length == 0)
  {
    return true;
  }
  require(m_string != nullptr && r2.m_string != nullptr);
  return (memcmp(m_string, r2.m_string, m_length) == 0);
}

/* Unit test
*/

#ifdef TEST_ROPE

int main(int argc, char ** argv) {
  ndb_init();

  init_lc_ndbd_memory_pool(12,
                           1,
                           1,
                           nullptr,
                           nullptr);

  char buffer_sml[32];
  const char * a_string = "One Two Three Four Five Six Seven Eight Nine Ten";
  LcRopeHandle h1, h2, h3, h4, h5, h6, h7;
  bool ok;

  /* Create a scope for the LocalRope */
  {
    LcLocalRope lr1(h1);
    assert(lr1.size() == 0);
    assert(lr1.empty());
    ok = lr1.assign(a_string);
    assert(ok);
    assert(lr1.size() == strlen(a_string) + 1);
    assert(! lr1.empty());
    assert(! lr1.compare(a_string));
    printf("LcLocalRope lr1 size: %d\n", lr1.size());
  }
  /* When the LocalRope goes out of scope, its head is copied back into the
     RopeHandle, which can then be used to construct a ConstRope.
  */
  LcConstRope cr1(h1);
  printf("LcConstRope cr1 size: %d\n", cr1.size());

  /* Copy a zero-length rope */
  {
    LcLocalRope lr6(h6);
  }
  {
    LcConstRope cr6(h6);
    cr6.copy(buffer_sml, sizeof(buffer_sml));
  }

  /* Assign & copy a string that is exactly the size as a rope segment */
  const char * str28 = "____V____X____V____X____VII";
  char buf28[28];
  {
    LcLocalRope lr5(h5);
    lr5.assign(str28);
    lr5.copy(buf28, sizeof(buf28));
    memset(buf28, 0, 28);
  }
  LcConstRope cr5(h5);
  cr5.copy(buf28, sizeof(buf28));

  /* Test buffered-style reading from ConstRope
  */
  assert(! cr1.compare(a_string));
  Uint32 offset = 0;
  int nread = 0;
  printf(" --> START readBuffered TEST <--\n");
  printf("LcConstRope cr1 nread: %d offset: %d\n", nread, offset);
  nread = cr1.readBuffered(buffer_sml, 32, offset);
  printf("LcConstRope cr1 nread: %d offset: %d\n", nread, offset);
  assert(! strncmp(a_string, buffer_sml, nread));
  nread = cr1.readBuffered(buffer_sml, 32, offset);
  printf("LcConstRope cr1 nread: %d offset: %d\n", nread, offset);
  assert(! strncmp(a_string + offset - nread, buffer_sml, nread));
  /* All done: */
  assert(offset == cr1.size());
  /* Read once more; should return 0: */
  nread = cr1.readBuffered(buffer_sml, 32, offset);
  assert(nread == 0);
  printf(" --> END readBuffered TEST <--\n");

  /* Test buffered-style writing to LocalRope
  */
  printf(" --> START appendBuffer TEST <--\n");
  {
    LcLocalRope lr2(h2);
    lr2.appendBuffer(a_string, 40);
    printf("lr2 size: %d\n", lr2.size());
    assert(lr2.size() == 40);
    lr2.appendBuffer(a_string, 40);
    printf("lr2 size: %d\n", lr2.size());
    assert(lr2.size() == 80);
  }
  printf("h2.hashValue() = 0x%x, h3.hashValue() = 0x%x\n",
         h2.hashValue(),
         h3.hashValue());
  /* Identical strings should have the same hash code whether they were stored
     in one part or in two.  Here is a scope for two local ropes that should
     end up with the same hash.
  */
  {
    printf("Hash test h3:\n");
    LcLocalRope lr3(h3);
    lr3.assign(a_string, 16);
    lr3.appendBuffer(a_string + 16, 16);

    printf("Hash test h4:\n");
    LcLocalRope lr4(h4);
    lr4.assign(a_string, 32);
  }
  printf("Hashes:  h3 = 0x%x, h4 = 0x%x\n", h3.hashValue(), h4.hashValue());
  assert(h3.hashValue() == h4.hashValue());
  printf(" --> END appendBuffer TEST <--\n");

  /* Test ConstRope::copy(LocalRope &)
  */
  printf(" --> START LcConstRope::copy() TEST <--\n");
  LcConstRope cr2(h2);
  printf("h2.hashValue() = 0x%x, h3.hashValue() = 0x%x\n",
         h2.hashValue(),
         h3.hashValue());
  printf("cr2 size: %d\n", cr2.size());
  assert(cr2.size() == 80);
  {
    LcLocalRope lr3(h3);
    cr2.copy(lr3);
    printf("lr3 size: %d\n", lr3.size());
    assert(lr3.size() == 80);
  }
  {
    LcLocalRope lr7(h7);
    lr7.appendBuffer(a_string, 40);
  }
  printf("h7.hashValue() = 0x%x\n", h7.hashValue());
  LcConstRope cr3(h3);
  assert(cr3.size() == 80);
  printf("h2.hashValue() = 0x%x, h3.hashValue() = 0x%x\n", h2.hashValue(), h3.hashValue());
  assert(h2.hashValue() == h3.hashValue());
  assert(cr2.equal(cr3));
  printf(" --> END LcConstRope::copy() TEST <--\n");

  /* Test that RopeHandles can be assigned */
  h6 = h3;
  assert(h6.hashValue() == h3.hashValue());
  LcConstRope cr6(h6);
  assert(cr6.size() == cr3.size());
  assert(cr3.equal(cr6));

  printf("Rope test successfully completed\n");
  ndb_end(0);
  return 0;
}

#endif
