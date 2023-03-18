/*
   Copyright (c) 2013, 2019, Oracle and/or its affiliates.
   Copyright (c) 2020, 2022, Hopsworks and/or its affiliates.

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

#ifndef NDB_INTRUSIVE_LIST64_HPP
#define NDB_INTRUSIVE_LIST64_HPP

/**
 * Intrusive64List implements an family of intrusive list.
 *
 * The following specialisation are defined:
 *
 * SL64List - single linked list with only first in head
 * DL64List - double linked list with only first in head
 * SLC64List - single linked list with first and count in head
 * DLC64List - double linked list with first and count in head
 * SLFifo64List - single linked list with both first and last in head
 * DLFifo64List - double linked list with both first and last in head
 * SLCFifo64List - single linked list with first and last and count in head
 * DLCFifo64List - double linked list with first and last and count in head
 *
 * For each XXList there are also
 * LocalXX64List
 * XXHead - XXList::Head
 *
 * Recommended use is to define list type alias:
 *   typedef LocalXX64List<NodeClass, PoolClass> YourList;
 * and declare the head as:
 *   YourList::Head64 head; or
 *   YourList::Head::POD64 head;
 * and in local scope declare list as:
 *   YourList list(pool, head);
 *
 * For all variants of lists the following methods is available:
 *   void addFirst(Ptr64<T> p);
 *   bool first(Ptr64<T>& p) const;
 *   bool hasNext(Ptr64<T> p) const;
 *   void insertAfter(Ptr64<T> p, Ptr64<T> loc);
 *   bool isEmpty() const;
 *   bool next(Ptr64<T>& p) const;
 *   bool removeFirst(Ptr64<T>& p);
 * and pool using methods
 *   Pool& getPool() const;
 *   void getPtr(Ptr64<T>& p) const;
 *   void getPtr(Ptr64<T>& p, Uint64 i) const;
 *   T* getPtr(Uint64 i) const;
 *   bool releaseFirst();
 *   bool seizeFirst(Ptr64<T>& p);
 *
 * These methods needs a prev link in node
 *   void insertBefore(Ptr64<T> p, Ptr64<T> loc);
 *   void remove(Ptr64<T> p);
 *   void remove(T* p);
 *   bool hasPrev(Ptr64<T> p) const;
 *   bool prev(Ptr64<T>& p) const;
 *
 * These methods needs a last link in head
 *   void addLast(Ptr64<T> p);
 *   bool last(Ptr64<T>& p) const;
 *   bool seizeLast(Ptr64<T>& p);
 * and the for the concatenating methods the OtherHead must
 * have the same or more features as list head have
 *   template<class OtherHead>  void prependList(OtherHead& other);
 *   template<class OtherHead>  void appendList(OtherHead& other);
 * When swapping list contents, list must have same head type.
 *   void swapList(Head64& src);
 *
 * These methods needs both prev link in node and last link in head
 *   bool removeLast(Ptr64<T>& p);
 *   bool releaseLast();
 *   void release(Uint64 i);
 *   void release(Ptr64<T> p);
 *
 * These methods needs a counter in head
 *   Uint64 getCount() const;
 **/

#include <ndb_limits.h>
#include <IntrusiveTags.hpp>
#include <Pool.hpp>

#define JAM_FILE_ID 544

template<class FirstLink64, class LastLink64, class Count64>
  class ListHeadPOD64
: public FirstLink64, public LastLink64, public Count64
{
public:
  typedef ListHeadPOD64<FirstLink64,LastLink64,Count64> POD64;
  void init()
  {
    FirstLink64::setFirst(RNIL64);
    LastLink64::setLast(RNIL64);
    Count64::setCount(0);
#if defined VM_TRACE || defined ERROR_INSERT
    in_use = false;
#endif
  }
  bool isEmpty() const
  {
    bool empty = FirstLink64::getFirst() == RNIL64;
#ifdef VM_TRACE
    Count64::checkCount(empty);
#endif
    return empty;
  }
#if defined VM_TRACE || defined ERROR_INSERT
  bool in_use;
#endif
};

template<class FirstLink64, class LastLink64, class Count64>
  class ListHead64
: public ListHeadPOD64<FirstLink64, LastLink64, Count64>
{
public:
  typedef ListHeadPOD64<FirstLink64, LastLink64, Count64> POD64;
  ListHead64() { POD64::init(); }
};

class FirstLink64
{
public:
  Uint64 getFirst() const { return m_first; }
  void setFirst(Uint64 first) { m_first = first; }
  template<class H> void copyFirst(H& h) { setFirst(h.getFirst()); }
private:
  Uint64 m_first;
};

class LastLink64
{
public:
  Uint64 getLast() const { return m_last; }
  void setLast(Uint64 last) { m_last = last; }
  template<class H> void copyLast(H& h) { setLast(h.getLast()); }
private:
  Uint64 m_last;
};

class NoLastLink64
{
public:
  void setLast(Uint64 /* last */) { }
  template<class H> void copyLast(H& /* h */) { }
};

class Count64
{
public:
  Uint64 getCount() const { return m_count; }
  void setCount(Uint64 count) { m_count = count; }
  void incrCount() { m_count ++ ; }
  void decrCount() { assert(m_count > 0); m_count -- ; }
  template<class H> void transferCount(H& h)
  {
    m_count += h.getCount();
    h.setCount(0);
  }
#ifdef VM_TRACE
  void checkCount(bool empty) const
  {
    if (empty)
      assert(getCount() == 0);
    else
      assert(getCount() > 0);
  }
#endif
private:
  Uint64 m_count;
};

class NoCount64
{
public:
  void setCount(Uint64 /* count */) { }
  void incrCount() { }
  void decrCount() { }
  template<class H> void transferCount(H& h) { h.setCount(0); }
#ifdef VM_TRACE
  void checkCount(bool /* empty */) const { }
#endif
private:
};

template <typename T, typename U = T> struct Default64SingleLinkMethods
{
  static bool hasNext(U& t) { return getNext(t) != RNIL64; }
  static Uint64 getNext(U& t) { return t.nextList; }
  static void setNext(U& t, Uint64 v) { t.nextList = v; }
  template<class T2> static void copyNext(T& t, T2& t2)
  {
    setNext(t, getNext(t2));
  }
  static void setPrev(U& /* t */, Uint64 /* v */) { }
  template<class T2> static void copyPrev(T& /* t */, T2& /* t2 */) { }
};

template <typename T, typename U = T> struct Default64DoubleLinkMethods
{
  static bool hasNext(U& t) { return getNext(t) != RNIL64; }
  static Uint64 getNext(U& t) { return t.nextList; }
  static void setNext(U& t, Uint64 v) { t.nextList = v; }
  template<class T2> static void copyNext(T& t, T2& t2)
  {
    setNext(t, getNext(t2));
  }
  static bool hasPrev(U& t) { return getPrev(t) != RNIL64; }
  static Uint64 getPrev(U& t) { return t.prevList; }
  static void setPrev(U& t, Uint64 v) { t.prevList = v; }
  template<class T2> static void copyPrev(T& t, T2& t2)
  {
    setPrev(t, getPrev(t2));
  }
};

template<typename T, Intrusive64Tags tag>
struct Tagged64SingleLinkMethods
{
  static bool hasNext(T& t) { return getNext(t) != RNIL64; }
  static Uint64 getNext(T& t) { return Intrusive64Access<tag>::getNext(t); }
  static void setNext(T& t, Uint64 v)
  {
    Intrusive64Access<tag>::getNext(t) = v;
  }
  template<class T2> static void copyNext(T& t, T2& t2)
  {
    setNext(t, T2::getNext(t2));
  }
  static void setPrev(T& t, Uint64 v) { }
  template<class T2> static void copyPrev(T& t, T2& t2)
  {
    setPrev(t, T2::getPrev(t2));
  }
};

template<typename T, Intrusive64Tags tag>
struct Tagged64DoubleLinkMethods
{
  static bool hasNext(T& t) { return getNext(t) != RNIL64; }
  static Uint64 getNext(T& t) { return Intrusive64Access<tag>::getNext(t); }
  static void setNext(T& t, Uint64 v)
  {
    Intrusive64Access<tag>::getNext(t) = v;
  }
  template<class T2> static void copyNext(T& t, T2& t2)
  {
    setNext(t, T2::getNext(t2));
  }
  static bool hasPrev(T& t) { return getPrev(t) != RNIL64; }
  static Uint64 getPrev(T& t) { return Intrusive64Access<tag>::getPrev(t); }
  static void setPrev(T& t, Uint64 v)
  {
    Intrusive64Access<tag>::getPrev(t) = v;
  }
  template<class T2> static void copyPrev(T& t, T2& t2)
  {
    setPrev(t, T2::getPrev(t2));
  }
};

template<typename T> struct remove_reference64 { typedef T type; };
template<typename T> struct remove_reference64<T&> { typedef T type; };
template<typename T> struct pod64 { typedef typename T::POD64 type; };
template<typename T> struct pod64<T&> { typedef typename T::POD64& type; };

template<class Pool, typename THead, class LM =
  Default64DoubleLinkMethods<typename Pool::Type> > class Intrusive64List
{
public:
typedef typename remove_reference64<THead>::type Head64;
typedef typename Head64::POD64 HeadPOD64;
public:
  typedef typename Pool::Type T;
  explicit Intrusive64List(Pool& pool, THead head): m_pool(pool), m_head(head)
  {
  }
  explicit Intrusive64List(Pool& pool): m_pool(pool)
  {
    m_head.init();
  }
  ~Intrusive64List() {}
private:
  Intrusive64List&  operator=(const Intrusive64List& src) {
    assert(&this->m_pool == &src.m_pool);
    this->m_head = src.m_head;
    return *this;
  }
private:
  Intrusive64List(const Intrusive64List&); // Not to be implemented
public:
  void addFirst(Ptr64<T> p);
  void addLast(Ptr64<T> p);
  void insertBefore(Ptr64<T> p, Ptr64<T> loc);
  void insertAfter(Ptr64<T> p, Ptr64<T> loc);
  bool removeFirst(Ptr64<T>& p);
  bool removeLast(Ptr64<T>& p);
  void remove(Ptr64<T> p);
  void remove(T* p);
  bool hasNext(Ptr64<T> p) const;
  bool next(Ptr64<T>& p) const;
  bool hasPrev(Ptr64<T> p) const;
  bool prev(Ptr64<T>& p) const;
  void swapList(Head64& src);
  template<class OtherList>  void prependList(OtherList& other);
  template<class OtherList>  void appendList(OtherList& other);
  bool isEmpty() const;
  Uint64 getCount() const;
  [[nodiscard]] bool first(Ptr64<T>& p) const;
  [[nodiscard]] bool last(Ptr64<T>& p) const;
public:
  Pool& getPool() const;
  [[nodiscard]] bool getPtr(Ptr64<T>& p) const
  {
    if (p.i == RNIL64)
    {
      p.p = NULL;
      return false;
    }
    else
    {
      return m_pool.getPtr(p);
    }
    return true;
  }
  [[nodiscard]] bool getPtr(Ptr64<T>& p, Uint64 i) const
  {
    p.i=i;
    return getPtr(p);
  }
  T* getPtr(Uint64 i) const
  {
    Ptr64<T> p;
    p.i = i;
    (void)getPtr(p);
    return p.p;
  }
  bool seizeFirst(Ptr64<T>& p);
  bool seizeLast(Ptr64<T>& p);
  bool releaseFirst();
  bool releaseLast();
  void release(Uint64 i);
  void release(Ptr64<T> p);
protected:
  Pool& m_pool;
  THead m_head;
};

/* Specialisations */

#define INTRUSIVE_LIST64_COMPAT(prefix, links) \
template <typename P, Intrusive64Tags tag = IA_64List, typename LM = \
  Tagged64##links##LinkMethods<typename P::Type, tag> > \
  class prefix##List : public Intrusive64List<P, prefix##Head64, LM> { \
  public: prefix##List(P& pool): \
  Intrusive64List<P, prefix##Head64, LM>(pool) { } \
}; \
 \
template <typename P, Intrusive64Tags tag = IA_64List, typename LM = \
  Tagged64##links##LinkMethods<typename P::Type, tag> > \
  class Local##prefix##List :\
  public Intrusive64List<P, prefix##Head64::POD64&, LM> { \
  public: Local##prefix##List(P& pool, prefix##Head64::POD64& head): \
  Intrusive64List<P, prefix##Head64::POD64&, LM>(pool, head) { } \
}; \
 \
template <typename P, Intrusive64Tags tag = IA_64List, typename LM = \
  Tagged64##links##LinkMethods<typename P::Type, tag> > \
class ConstLocal##prefix##List : \
  public Intrusive64List<P, const prefix##Head64::POD64&, LM> { \
  public: ConstLocal##prefix##List(P& pool, \
                                   const prefix##Head64::POD64& head): \
  Intrusive64List<P, const prefix##Head64::POD64&, LM>(pool, head) { } \
}

typedef ListHead64<FirstLink64, NoLastLink64, NoCount64> SL64Head64;
typedef ListHead64<FirstLink64, NoLastLink64, NoCount64> DL64Head64;
typedef ListHead64<FirstLink64, NoLastLink64, Count64> SLC64Head64;
typedef ListHead64<FirstLink64, NoLastLink64, Count64> DLC64Head64;
typedef ListHead64<FirstLink64, LastLink64, NoCount64> SLFifo64Head64;
typedef ListHead64<FirstLink64, LastLink64, NoCount64> DLFifo64Head64;
typedef ListHead64<FirstLink64, LastLink64, Count64> SLCFifo64Head64;
typedef ListHead64<FirstLink64, LastLink64, Count64> DLCFifo64Head64;

INTRUSIVE_LIST64_COMPAT(SL64, Single);
INTRUSIVE_LIST64_COMPAT(DL64, Double);
INTRUSIVE_LIST64_COMPAT(SLC64, Single);
INTRUSIVE_LIST64_COMPAT(DLC64, Double);
INTRUSIVE_LIST64_COMPAT(SLFifo64, Single);
INTRUSIVE_LIST64_COMPAT(DLFifo64, Double);
INTRUSIVE_LIST64_COMPAT(SLCFifo64, Single);
INTRUSIVE_LIST64_COMPAT(DLCFifo64, Double);

/**
 * Implementation Intrusive64List
 **/

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::addFirst(Ptr64<T> p)
{
  Ptr64<T> firstItem;
  if (first(firstItem))
  {
    LM::setPrev(*firstItem.p, p.i);
  }
  else
  {
    m_head.setLast(p.i);
  }
  LM::setPrev(*p.p, RNIL64);
  LM::setNext(*p.p, firstItem.i);
  m_head.setFirst(p.i);
  m_head.incrCount();
}

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::addLast(Ptr64<T> p)
{
  Ptr64<T> lastItem;
  if (last(lastItem))
  {
    LM::setNext(*lastItem.p, p.i);
  }
  else
  {
    m_head.setFirst(p.i);
  }
  LM::setPrev(*p.p, lastItem.i);
  LM::setNext(*p.p, RNIL64);
  m_head.setLast(p.i);
  m_head.incrCount();
}

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::insertBefore(Ptr64<T> p,
                                                         Ptr64<T> loc)
{
  assert(!loc.isNull());
  Ptr64<T> prevItem = loc;
  if (prev(prevItem))
  {
    LM::setNext(*prevItem.p, p.i);
  }
  else
  {
    m_head.setFirst(p.i);
  }
  LM::setPrev(*loc.p, p.i);
  LM::setPrev(*p.p, prevItem.i);
  LM::setNext(*p.p, loc.i);
  m_head.incrCount();
}

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::insertAfter(Ptr64<T> p,
                                                          Ptr64<T> loc)
{
  assert(!loc.isNull());
  Ptr64<T> nextItem = loc;
  if (next(nextItem))
  {
    LM::setPrev(*nextItem.p, p.i);
  }
  else
  {
    m_head.setLast(p.i);
  }
  LM::setNext(*loc.p, p.i);
  LM::setPrev(*p.p, loc.i);
  LM::setNext(*p.p, nextItem.i);
  m_head.incrCount();
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::removeFirst(Ptr64<T>& p)
{
  if (!first(p))
    return false;
  Ptr64<T> nextItem = p;
  if (next(nextItem))
  {
    LM::setPrev(*nextItem.p, RNIL64);
  }
  else
  {
    m_head.setLast(RNIL64);
  }
  LM::setNext(*p.p, RNIL64);
  m_head.setFirst(nextItem.i);
  m_head.decrCount();
  return true;
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::removeLast(Ptr64<T>& p)
{
  if (!last(p))
    return false;
  Ptr64<T> prevItem = p;
  if (prev(prevItem))
  {
    LM::setNext(*prevItem.p, RNIL64);
  }
  else
  {
    m_head.setFirst(RNIL64);
  }
  LM::setPrev(*p.p, RNIL64);
  m_head.setLast(prevItem.i);
  m_head.decrCount();
  return true;
}

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::remove(Ptr64<T> p)
{
  remove(p.p);
}

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::remove(T* p)
{
  Ptr64<T> prevItem;
  Ptr64<T> nextItem;
  prevItem.p = p;
  nextItem.p = p;
  prev(prevItem);
  next(nextItem);
  if (!prevItem.isNull())
  {
    LM::setNext(*prevItem.p, nextItem.i);
  }
  else
  {
    m_head.setFirst(nextItem.i);
  }
  if (!nextItem.isNull())
  {
    LM::setPrev(*nextItem.p, prevItem.i);
  }
  else
  {
    m_head.setLast(prevItem.i);
  }
  LM::setPrev(*p, RNIL64);
  LM::setNext(*p, RNIL64);
  m_head.decrCount();
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::hasNext(Ptr64<T> p) const
{
  return LM::hasNext(*p.p);
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::next(Ptr64<T>& p) const
{
  p.i = LM::getNext(*p.p);
  if (p.i == RNIL64)
    return false;
  return getPtr(p);
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::hasPrev(Ptr64<T> p) const
{
  return LM::hasPrev(*p.p);
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::prev(Ptr64<T>& p) const
{
  p.i = LM::getPrev(*p.p);
  if (p.i == RNIL64)
    return false;
  return getPtr(p);
}

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::swapList(Head64& src)
{
  Head64 tmp = m_head;
  m_head = src;
  src = tmp;
}

template<class Pool, typename THead, class LM>
template<class OtherHead>
inline void Intrusive64List<Pool, THead, LM>::prependList(OtherHead& other)
{
  if (other.isEmpty())
    return;

  Ptr64<T> firstItem;
  (void)first(firstItem);

  Ptr64<T> otherLastItem;
  otherLastItem.i = other.getLast();
  (void)getPtr(otherLastItem);

  if (firstItem.i != RNIL64)
  {
    LM::setPrev(*firstItem.p, otherLastItem.i);
  }
  else
  {
    m_head.copyLast(other);
  }
  LM::setNext(*otherLastItem.p, firstItem.i);
  m_head.copyFirst(other);
  m_head.transferCount(other);
  other.setFirst(RNIL64);
  other.setLast(RNIL64);
}

template<class Pool, typename THead, class LM>
template<class OtherHead>
inline void Intrusive64List<Pool, THead, LM>::appendList(OtherHead& other)
{
  if (other.isEmpty())
    return;

  Ptr64<T> lastItem;
  (void)last(lastItem);

  Ptr64<T> otherFirstItem;
  otherFirstItem.i = other.getFirst();
  (void)getPtr(otherFirstItem);

  if (lastItem.i != RNIL64)
  {
    LM::setNext(*lastItem.p, otherFirstItem.i);
  }
  else
  {
    m_head.copyFirst(other);
  }
  LM::setPrev(*otherFirstItem.p, lastItem.i);
  m_head.copyLast(other);
  m_head.transferCount(other);
  other.setFirst(RNIL64);
  other.setLast(RNIL64);
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::isEmpty() const
{
  return m_head.isEmpty();
}

template<class Pool, typename THead, class LM>
inline Uint64 Intrusive64List<Pool, THead, LM>::getCount() const
{
  return m_head.getCount();
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::first(Ptr64<T>& p) const
{
  p.i = m_head.getFirst();
  return getPtr(p);
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::last(Ptr64<T>& p) const
{
  p.i = m_head.getLast();
  return getPtr(p);
}

template<class Pool, typename THead, class LM>
inline Pool& Intrusive64List<Pool, THead, LM>::getPool() const
{
  return m_pool;
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::seizeFirst(Ptr64<T>& p)
{
  if (!getPool().seize(p))
    return false;
  addFirst(p);
  return true;
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::seizeLast(Ptr64<T>& p)
{
  if (!getPool().seize(p))
    return false;
  addLast(p);
  return true;
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::releaseFirst()
{
  Ptr64<T> p;
  if (!removeFirst(p))
    return false;
  getPool().release(p);
  return true;
}

template<class Pool, typename THead, class LM>
inline bool Intrusive64List<Pool, THead, LM>::releaseLast()
{
  Ptr64<T> p;
  if (!removeLast(p))
    return false;
  getPool().release(p);
  return true;
}

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::release(Ptr64<T> p)
{
  remove(p);
  getPool().release(p);
}

template<class Pool, typename THead, class LM>
inline void Intrusive64List<Pool, THead, LM>::release(Uint64 i)
{
  Ptr64<T> p;
  getPtr(p, i);
  remove(p);
  getPool().release(p);
}

#undef JAM_FILE_ID

#endif
