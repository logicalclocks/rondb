/*
   Copyright (c) 2015, 2017, Oracle and/or its affiliates.
   Copyright (c) 2020, 2022, Hopsworks, and/or its affiliates.

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

#ifdef TEST_INTRUSIVE64LIST

#include <ndb_global.h>
#include <NdbTap.hpp>
#include "Intrusive64List.hpp"
#include "RWPool64.hpp"
#include "test_context.hpp"

#define JAM_FILE_ID 543

class Type
{
};

class SL64: public Type
{
public:
  Uint64 nextList;
};

class DL64: public SL64
{
public:
  Uint64 prevList;
};

class T : public DL64
{
public:
  Uint32 data;
  Uint32 m_magic;
  Uint64 key;
  Uint64 nextPool;
};

static unsigned scale = 100;

#define LIST_COMMON_TEST(pool, list, head) \
  Ptr64<T> p; \
  Ptr64<T> q; \
 \
  ok(&pool == &list.getPool(), "list.getPool()"); \
 \
  ok(list.isEmpty(), "list.isEmpty()"); \
 \
  Uint64 c_seized = 0; \
  Uint64 c_walked = 0; \
  Uint64 c_released = 0; \
  Uint64 c_moved = 0; \
  Uint64 c_half; \
 \
  while (list.seizeFirst(p)) { c_seized ++; p.p->key = c_seized; } \
  ok(!list.isEmpty(), "seizeFirst %llu items", c_seized); \
 \
  c_half = c_seized / 2;   \
 \
  if (list.first(p)) { c_walked ++; while (list.next(p)) c_walked ++; } \
  ok(c_seized == c_walked, "walk next %llu of %llu items", c_walked, c_seized); \
 \
  ok(list.first(q), "list.first(q) failed"); \
  while (q.p->key != c_half && list.hasNext(q)) list.next(q); \
  ok(q.p->key == c_half, "find half %llu (%llu)", q.p->key, c_half); \
 \
  /* list before: key= c_seized, c_seized - 1, ..., c_half, ..., 2, 1 */ \
  /* list after: key= c_half + 1, c_half + 2, ..., c_seized - 1, c_seized, c_half - 1, c_half - 2, ..., 2, 1 and c_half removed into p */ \
  while (list.removeFirst(p)) { if (p.i == q.i) break; list.insertAfter(p, q); c_moved ++;} \
  ok(p.p->key == c_half, "rearrange: removed item %llu (%llu), moved %llu items", p.p->key, c_half, c_moved); \
  ok(c_moved == (c_seized - c_half), "rearrange: moved %llu of %llu items", c_moved, (c_seized - c_half)); \
 \
  pool.release(p); \
  c_released ++; \
 \
  ok(list.first(p), "list.first(p) failed"); \
  ok(p.p->key == c_half + 1, "rearrange: first item %llu (%llu)", p.p->key, c_half + 1); \
 \
  ok(list.getPtr(p.i) == p.p, "list.getPtr(%llu) = %p (%p)", p.i, p.p, list.getPtr(p.i)); \
  ok(pool.getPtr(p.i) == p.p, "pool.getPtr(%llu) = %p (%p)", p.i, p.p, pool.getPtr(p.i)); \
 \
  q.i = p.i; \
  q.p = NULL; \
  ok(list.getPtr(q), "list.getPtr(q)"); \
 \
  q.i = RNIL64; \
  q.p = NULL; \
  ok(list.getPtr(q, p.i), "list.getPtr(q, p.i)")

#define LIST_PREV_TEST(pool, list, head) \
  ok(list.first(q), "list.first(q) failed"); \
  while (q.p->key != c_half - 1 && list.hasNext(q)) list.next(q); \
  ok(q.p->key == c_half - 1, "find %llu (%llu)", q.p->key, c_half - 1); \
  /* list before: key= c_half + 1, c_half + 2, ..., c_seized - 1, c_seized, c_half - 1, c_half - 2, ..., 2, 1 */ \
  /* list after: key= c_seized - 1, ..., c_half + 1, c_half -1, ..., 2, 1 and c_seized removed into p */ \
  while (list.removeFirst(p)) { if (p.i == q.i) break; list.insertBefore(p, q); q=p; c_moved ++;} \
  ok(p.p->key == c_seized, "rearrange: removed item %llu (%llu), moved %llu items", p.p->key, c_seized, c_moved); \
  ok(c_moved == c_seized, "rearrange: moved %llu of %llu items", c_moved, c_seized); \
 \
  pool.release(p); \
  c_released ++; \
 \
  ok(list.first(p), "list.first(p) failed"); \
  ok(p.p->key == c_seized - 1, "rearrange: first item %llu (%llu)", p.p->key, c_seized - 1); \
 \
  while (p.p->key != c_half -1 && list.next(p)); \
  ok(p.p->key == c_half -1, "found %llu (%llu)", p.p->key, c_half -1); \
 \
  q = p; \
  (void) list.next(q); \
  list.remove(q.p); \
  pool.release(q); \
  c_released ++; \
  q = p; \
  (void) list.next(q); \
  ok(q.p->key == c_half -3, "found %llu (%llu)", q.p->key, c_half - 3); \
  list.release(p); \
  c_released ++

#define LIST_LAST_TEST(pool, list, head) \
  c_seized = 0; \
  while (list.seizeLast(p)) c_seized ++; \
  ok(c_seized == c_released, "seizeLast %llu (%llu)", c_seized, c_released); \
  c_released = 0; \
  while (list.last(p)) { list.releaseFirst(); c_released ++; } \
  ok(c_seized == c_released, "released %llu (%llu)", c_released, c_seized)

#define LIST_COUNT_TEST(pool, list, head, value) \
  { \
    Uint64 c = list.getCount(); \
    ok(c == value, "count %llu (%llu)", c, value); \
  }

#define LIST_RELEASE_FIRST(list) \
  while (list.releaseFirst()) c_released ++; \
  OK(c_seized == c_released); \
 \
  OK(list.isEmpty())

#define LIST_RELEASE_LAST(list) \
  while (list.releaseLast()) c_released ++; \
  ok(c_seized == c_released, "released %llu (%llu)", c_released, c_seized); \
 \
  ok(list.isEmpty(), "list.isEmpty()")

template<typename Pool>
void testSLList(Pool& pool)
{
  diag("testSLList");
  SL64List<SL64>::Head64 head;
  LocalSL64List<Pool> list(pool, head);

  LIST_COMMON_TEST(pool, list, head);

  LIST_RELEASE_FIRST(list);
}

template<typename Pool>
void testDLList(Pool& pool)
{
  diag("testDLList");
  DL64List<DL64>::Head64 head;
  LocalDL64List<Pool> list(pool, head);

  LIST_COMMON_TEST(pool, list, head);

  LIST_PREV_TEST(pool, list, head);

  LIST_RELEASE_FIRST(list);
}

template<typename Pool>
void testSLCList(Pool& pool)
{
  diag("testSLCList");
  SLC64List<SL64>::Head64 head;
  LocalSLC64List<Pool> list(pool, head);

  LIST_COMMON_TEST(pool, list, head);

  LIST_COUNT_TEST(pool, list, head, c_seized - 1);

  LIST_RELEASE_FIRST(list);
}

template<typename Pool>
void testDLCList(Pool& pool)
{
  diag("testDLCList");
  DLC64List<DL64>::Head64 head;
  LocalDLC64List<Pool> list(pool, head);

  LIST_COMMON_TEST(pool, list, head);

  LIST_COUNT_TEST(pool, list, head, c_seized - 1);

  LIST_PREV_TEST(pool, list, head);

  LIST_COUNT_TEST(pool, list, head, c_seized - 4);

  LIST_RELEASE_FIRST(list);
}

template<typename Pool>
void testSLFifoList(Pool& pool)
{
  diag("testSLFifoList");
  SLFifo64List<SL64>::Head64 head;
  LocalSLFifo64List<Pool> list(pool, head);

  LIST_COMMON_TEST(pool, list, head);

  LIST_RELEASE_FIRST(list);

  LIST_LAST_TEST(pool, list, head);
}

template<typename Pool>
void testDLFifoList(Pool& pool)
{
  diag("testDLFifoList");
  DLFifo64List<DL64>::Head64 head;
  LocalDLFifo64List<Pool> list(pool, head);

  LIST_COMMON_TEST(pool, list, head);

  LIST_PREV_TEST(pool, list, head);

  LIST_RELEASE_LAST(list);

  LIST_LAST_TEST(pool, list, head);
}

template<typename Pool>
void testSLCFifoList(Pool& pool)
{
  diag("testSLCFifoList");
  SLCFifo64List<SL64>::Head64 head;
  LocalSLCFifo64List<Pool> list(pool, head);

  LIST_COMMON_TEST(pool, list, head);

  LIST_COUNT_TEST(pool, list, head, c_seized - 1);

  LIST_RELEASE_FIRST(list);

  LIST_COUNT_TEST(pool, list, head, Uint64(0));

  LIST_LAST_TEST(pool, list, head);

  LIST_COUNT_TEST(pool, list, head, Uint64(0));
}

template<typename Pool>
void testDLCFifoList(Pool& pool)
{
  diag("testDLCFifoList");
  DLCFifo64List<DL64>::Head64 head;
  LocalDLCFifo64List<Pool> list(pool, head);

  LIST_COMMON_TEST(pool, list, head);

  LIST_COUNT_TEST(pool, list, head, c_seized - 1);

  LIST_PREV_TEST(pool, list, head);

  LIST_COUNT_TEST(pool, list, head, c_seized - 4);

  LIST_RELEASE_LAST(list);

  LIST_COUNT_TEST(pool, list, head, Uint64(0));

  LIST_LAST_TEST(pool, list, head);

  LIST_COUNT_TEST(pool, list, head, Uint64(0));
}

template<typename Pool>
void testConcat(Pool& pool)
{
  diag("testConcat");
  SLFifo64List<SL64>::Head64 slhead;
  DLFifo64List<DL64>::Head64 dlhead;
  SLCFifo64List<SL64>::Head64 slchead;
  DLCFifo64List<DL64>::Head64 dlchead;

  Ptr64<T> p;

  Uint64 c_seized = 0;
  p.p = nullptr;
  {
    LocalSLFifo64List<Pool> list(pool, slhead);
    for (; c_seized < 1 * scale ; c_seized ++)
    {
      list.seizeFirst(p);
      p.p->key = c_seized + 1;
    }
  } /* sl: 100-1 */
  {
    LocalDLFifo64List<Pool> list(pool, dlhead);
    for (; c_seized < 2 * scale ; c_seized ++)
    {
      list.seizeFirst(p);
      p.p->key = c_seized + 1;
    }
  } /* dl: 200-101 */
  {
    LocalSLCFifo64List<Pool> list(pool, slchead);
    for (; c_seized < 3 * scale; c_seized ++)
    {
      list.seizeFirst(p);
      p.p->key = c_seized + 1;
    }
    ok(list.getCount() == 1 * scale, "slc.count %llu (%u)", list.getCount(), 1 * scale);
  } /* slc: 300-201 */
  {
    LocalDLCFifo64List<Pool> list(pool, dlchead);
    for (; c_seized < 4 * scale; c_seized ++)
    {
      list.seizeFirst(p);
      p.p->key = c_seized + 1;
    }
    ok(list.getCount() == 1 * scale, "dlc.count %llu (%u)", list.getCount(), 1 * scale);
  } /* dlc: 400-301 */
  {
    LocalSLCFifo64List<Pool> list(pool, slchead);
    list.appendList(dlchead);
    ok(list.getCount() == 2 * scale, "slc.append(dlc) %llu (%u) items", list.getCount(), 2 * scale);
  } /* slc: 300-201, 400-301 */
  {
    LocalSLFifo64List<Pool> list(pool, slhead);
    list.prependList(slchead);
    Uint32 c = 0;
    if (list.first(p))
    {
      c ++;
      while (list.next(p)) c ++;
    }
    ok(c == 3 * scale, "sl.prepend(slc) %u (%u) items", c, 3 * scale);
  } /* sl: 300-201, 400-301, 100-1 */
  {
    LocalDLCFifo64List<Pool> list(pool, dlchead);
    for (; c_seized < 5 * scale; c_seized ++)
    {
      list.seizeFirst(p);
      p.p->key = c_seized + 1;
    }
  } /* dlc: 500-401 */
  {
    LocalDLFifo64List<Pool> list(pool, dlhead);
    list.appendList(dlchead);
    Uint32 c = 0;
    if (list.first(p))
    {
      c ++;
      while (list.next(p)) c ++;
    }
    ok(c == 2 * scale, "dl.append(dlc) %u (%u) items", c, 2 * scale);
  } /* dl: 200-101, 500-401 */
  {
    LocalSLFifo64List<Pool> list(pool, slhead);
    list.prependList(dlhead);
    Uint32 c = 0;
    if (list.first(p))
    {
      c ++;
      while (list.next(p)) c ++;
    }
    ok(c == 5 * scale, "sl.prepend(dl) %u (%u) items", c, 5 * scale);
  } /* sl: 200-101, 500-401, 300-201, 400-301, 100-1 */
  ok(slchead.getCount() == 0, "slc.count %llu (0)", slchead.getCount());
  ok(dlchead.getCount() == 0, "dlc.count %llu (0)", dlchead.getCount());
  {
    LocalSLFifo64List<Pool> list(pool, slhead);
    (void)list.first(p);
    ok(p.p->key == 2 * scale, "sl#1: %llu (%u)", p.p->key, 2 * scale);
    for (unsigned i = 0; i < 1 * scale; i++) list.next(p);
    ok(p.p->key == 5 * scale, "sl#1: %llu (%u)", p.p->key, 5 * scale);
    for (unsigned i = 0; i < 1 * scale; i++) list.next(p);
    ok(p.p->key == 3 * scale, "sl#1: %llu (%u)", p.p->key, 3 * scale);
    for (unsigned i = 0; i < 1 * scale; i++) list.next(p);
    ok(p.p->key == 4 * scale, "sl#1: %llu (%u)", p.p->key, 4 * scale);
    for (unsigned i = 0; i < 1 * scale; i++) list.next(p);
    ok(p.p->key == 1 * scale, "sl#1: %llu (%u)", p.p->key, 1 * scale);
    for (unsigned i = 0; i < 1 * scale; i++) list.next(p);
    ok(p.i == RNIL64, "sl#%u %llu (RNIL64:%llu)", 5 * scale + 1, p.i, RNIL64);
  }
}

#include <stdlib.h>

int
main(int argc, char **argv)
{
  if (argc == 2)
    scale = atoi(argv[1]);

  Pool_context pc = test_context(1 * scale);
  RecordPool64<RWPool64<T> > pool;
  pool.init(1, pc);

  plan(0);

  testSLList(pool);
  testDLList(pool);
  testSLCList(pool);
  testDLCList(pool);
  testSLFifoList(pool);
  testDLFifoList(pool);
  testSLCFifoList(pool);
  testDLCFifoList(pool);

  testConcat(pool);

  return exit_status();
}

#endif

