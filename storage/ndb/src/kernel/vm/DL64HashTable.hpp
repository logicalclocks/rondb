/*
   Copyright (c) 2003, 2017, Oracle and/or its affiliates.
   Copyright (c) 2020, 2022, Logical Clocks and/or its affiliates.

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

#ifndef DL64_HASHTABLE_HPP
#define DL64_HASHTABLE_HPP

#include <ndb_global.h>

#define JAM_FILE_ID 19


/**
 * DLM64HashTable implements a hashtable using chaining
 *   (with a double linked list)
 *
 * The entries in the (uninstansiated) meta class passed to the
 * hashtable must have the following methods:
 *
 *  -# nextHash(T&) returning a reference to the next link
 *  -# prevHash(T&) returning a reference to the prev link
 *  -# bool equal(T const&,T const&) returning equality of the objects keys
 *  -# hashValue(T) calculating the hash value
 */

template <typename T, typename U = T> struct DL64HashTableDefaultMethods {
static Uint64& nextHash(U& t) { return t.nextHash; }
static Uint64& prevHash(U& t) { return t.prevHash; }
static Uint64 hashValue(T const& t) { return t.hashValue(); }
static bool equal(T const& lhs, T const& rhs) { return lhs.equal(rhs); }
};

template <typename P, typename M = DL64HashTableDefaultMethods<typename P::Type> >
class DLM64HashTable
{
public:
  explicit DLM64HashTable(P & thePool);
  ~DLM64HashTable();
private:
  typedef typename P::Type T;
  DLM64HashTable(const DLM64HashTable&);
  DLM64HashTable&  operator=(const DLM64HashTable&);

public:
  /**
   * Set the no of bucket in the hashtable
   *
   * Note, can currently only be called once
   */
  bool setSize(Uint64 noOfElements);

  /**
   * Seize element from pool - return i
   *
   * Note *must* be added using <b>add</b> (even before hash.release)
   *             or be released using pool
   */
  bool seize(Ptr64<T> &);

  /**
   * Add an object to the hashtable
   */
  void add(Ptr64<T> &);

  /**
   * Find element key in hashtable update Ptr (i & p)
   *   (using key.equal(...))
   * @return true if found and false otherwise
   */
  bool find(Ptr64<T> &, const T & key) const;

  /**
   * Update i & p value according to <b>i</b>
   */
  void getPtr(Ptr64<T> &, Uint64 i) const;

  /**
   * Get element using ptr.i (update ptr.p)
   */
  void getPtr(Ptr64<T> &) const;

  /**
   * Get P value for i
   */
  T * getPtr(Uint64 i) const;

  /**
   * Remove element (and set Ptr to removed element)
   * Note does not return to pool
   */
  void remove(Ptr64<T> &, const T & key);

  /**
   * Remove element
   * Note does not return to pool
   */
  void remove(Uint64 i);

  /**
   * Remove element
   * Note does not return to pool
   */
  void remove(Ptr64<T> &);

  /**
   * Remove all elements, but dont return them to pool
   */
  void removeAll();

  /**
   * Remove element and return to pool
   * release releases object and places it first in free list
   * releaseLast releases object and places it last in free list
   */
  void release(Uint64 i);
  void releaseLast(Uint64 i);

  /**
   * Remove element and return to pool
   * release releases object and places it first in free list
   * releaseLast releases object and places it last in free list
   */
  void release(Ptr64<T> &);
  void releaseLast(Ptr64<T> &);

  class Iterator {
  public:
    Ptr64<T> curr;
    Uint64 bucket;
    inline bool isNull() const { return curr.isNull();}
    inline void setNull() { curr.setNull(); }
  };

  /**
   * Sets curr.p according to curr.i
   */
  void getPtr(Iterator & iter) const ;

  /**
   * First element in bucket
   */
  bool first(Iterator & iter) const;

  bool first(Ptr64<T>& p) const;

  /**
   * Next Element
   *
   * param iter - A "fully set" iterator
   */
  bool next(Iterator & iter) const;

  bool next(Ptr64<T>& p) const;

  /**
   * Get next element starting from bucket
   *
   * @param bucket - Which bucket to start from
   * @param iter - An "uninitialized" iterator
   */
  bool next(Uint64 bucket, Iterator & iter) const;

private:
  Uint64 mask;
  Uint64 * hashValues;
  P & thePool;
};

template <typename P, typename M>
inline
DLM64HashTable<P, M>::DLM64HashTable(P & _pool)
  : mask(0), hashValues(nullptr), thePool(_pool)
{}

template <typename P, typename M>
inline
DLM64HashTable<P, M>::~DLM64HashTable()
{
  if (hashValues != 0)
    delete [] hashValues;
}

template <typename P, typename M>
inline
bool
DLM64HashTable<P, M>::setSize(Uint64 size)
{
  Uint64 i = 1;
  while (i < size) i *= 2;

  if (hashValues != NULL)
  {
    /*
      If setSize() is called twice with different size values then this is 
      most likely a bug.
    */
    assert(mask == i-1); 
    // Return true if size already set to 'size', false otherwise.
    return mask == i-1;
  }

  mask = (i - 1);
  hashValues = new Uint64[i];
  for (Uint64 j = 0; j<i; j++)
    hashValues[j] = RNIL64;

  return true;
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::add(Ptr64<T> & obj)
{
  const Uint64 hv = M::hashValue(*obj.p) & mask;
  const Uint64 i  = hashValues[hv];

  if (i == RNIL64)
  {
    hashValues[hv] = obj.i;
    M::nextHash(*obj.p) = RNIL64;
    M::prevHash(*obj.p) = RNIL64;
  }
  else
  {
    T * tmp = thePool.getPtr(i);
    M::prevHash(*tmp) = obj.i;
    M::nextHash(*obj.p) = i;
    M::prevHash(*obj.p) = RNIL64;

    hashValues[hv] = obj.i;
  }
}

/**
 * First element
 */
template <typename P, typename M>
inline
bool
DLM64HashTable<P, M>::first(Iterator & iter) const
{
  Uint64 i = 0;
  while (i <= mask && hashValues[i] == RNIL64) i++;
  if (i <= mask)
  {
    iter.bucket = i;
    iter.curr.i = hashValues[i];
    iter.curr.p = thePool.getPtr(iter.curr.i);
    return true;
  }
  else
  {
    iter.curr.i = RNIL64;
  }
  return false;
}

template <typename P, typename M>
inline
bool
DLM64HashTable<P, M>::first(Ptr64<T>& p) const
{
  for (Uint64 bucket = 0; bucket <= mask; bucket++)
  {
    if (hashValues[bucket] != RNIL64)
    {
      p.i = hashValues[bucket];
      p.p = thePool.getPtr(p.i);
      return true;
    }
  }
  p.i = RNIL64;
  return false;
}

template <typename P, typename M>
inline
bool
DLM64HashTable<P, M>::next(Iterator & iter) const
{
  if (M::nextHash(*iter.curr.p) == RNIL64)
  {
    Uint64 i = iter.bucket + 1;
    while (i <= mask && hashValues[i] == RNIL64) i++;
    if (i <= mask)
    {
      iter.bucket = i;
      iter.curr.i = hashValues[i];
      iter.curr.p = thePool.getPtr(iter.curr.i);
      return true;
    }
    else
    {
      iter.curr.setNull();
      return false;
    }
  }

  iter.curr.i = M::nextHash(*iter.curr.p);
  iter.curr.p = thePool.getPtr(iter.curr.i);
  return true;
}

template <typename P, typename M>
inline
bool
DLM64HashTable<P, M>::next(Ptr64<T>& p) const
{
  p.i = M::nextHash(*p.p);
  if (p.i == RNIL64)
  {
    Uint64 bucket = M::hashValue(*p.p) & mask;
    bucket++;
    while (bucket <= mask && hashValues[bucket] == RNIL64)
    {
      bucket++;
    }
    if (bucket > mask)
    {
      return false;
    }
    p.i = hashValues[bucket];
  }
  p.p = thePool.getPtr(p.i);
  return true;
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::remove(Ptr64<T> & ptr, const T & key)
{
  const Uint64 hv = M::hashValue(key) & mask;

  Uint64 i;
  T * p;
  Ptr64<T> prev;
  prev.i = RNIL64;

  i = hashValues[hv];
  while (i != RNIL64)
  {
    p = thePool.getPtr(i);
    if (M::equal(key, * p))
    {
      const Uint64 next = M::nextHash(*p);
      if (prev.i == RNIL64)
      {
        hashValues[hv] = next;
      }
      else
      {
        M::nextHash(*prev.p) = next;
      }

      if (next != RNIL64)
      {
        T * nextP = thePool.getPtr(next);
        M::prevHash(*nextP) = prev.i;
      }

      ptr.i = i;
      ptr.p = p;
      return;
    }
    prev.p = p;
    prev.i = i;
    i = M::nextHash(*p);
  }
  ptr.i = RNIL64;
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::remove(Uint64 i)
{
  Ptr64<T> tmp;
  tmp.i = i;
  tmp.p = thePool.getPtr(i);
  remove(tmp);
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::release(Uint64 i)
{
  Ptr64<T> tmp;
  tmp.i = i;
  tmp.p = thePool.getPtr(i);
  release(tmp);
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::releaseLast(Uint64 i)
{
  Ptr64<T> tmp;
  tmp.i = i;
  tmp.p = thePool.getPtr(i);
  releaseLast(tmp);
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::releaseLast(Ptr64<T> & ptr)
{
  remove(ptr);
  thePool.releaseLast(ptr);
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::remove(Ptr64<T> & ptr)
{
  const Uint64 next = M::nextHash(*ptr.p);
  const Uint64 prev = M::prevHash(*ptr.p);

  if (prev != RNIL64)
  {
    T * prevP = thePool.getPtr(prev);
    M::nextHash(*prevP) = next;
  }
  else
  {
    const Uint64 hv = M::hashValue(*ptr.p) & mask;
    if (hashValues[hv] == ptr.i)
    {
      hashValues[hv] = next;
    }
    else
    {
      // Will add assert in 5.1
      assert(false);
    }
  }

  if (next != RNIL64)
  {
    T * nextP = thePool.getPtr(next);
    M::prevHash(*nextP) = prev;
  }
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::release(Ptr64<T> & ptr)
{
  const Uint64 next = M::nextHash(*ptr.p);
  const Uint64 prev = M::prevHash(*ptr.p);

  if (prev != RNIL64)
  {
    T * prevP = thePool.getPtr(prev);
    M::nextHash(*prevP) = next;
  }
  else
  {
    const Uint64 hv = M::hashValue(*ptr.p) & mask;
    if (hashValues[hv] == ptr.i)
    {
      hashValues[hv] = next;
    }
    else
    {
      assert(false);
      // Will add assert in 5.1
    }
  }

  if (next != RNIL64)
  {
    T * nextP = thePool.getPtr(next);
    M::prevHash(*nextP) = prev;
  }

  thePool.release(ptr);
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::removeAll()
{
  for (Uint64 i = 0; i <= mask; i++)
    hashValues[i] = RNIL64;
}

template <typename P, typename M>
inline
bool
DLM64HashTable<P, M>::next(Uint64 bucket, Iterator & iter) const
{
  while (bucket <= mask && hashValues[bucket] == RNIL64)
    bucket++;

  if (bucket > mask)
  {
    iter.bucket = bucket;
    iter.curr.setNull();
    return false;
  }

  iter.bucket = bucket;
  iter.curr.i = hashValues[bucket];
  iter.curr.p = thePool.getPtr(iter.curr.i);
  return true;
}

template <typename P, typename M>
inline
bool
DLM64HashTable<P, M>::seize(Ptr64<T> & ptr)
{
  if (thePool.seize(ptr))
  {
    M::nextHash(*ptr.p) = M::prevHash(*ptr.p) = RNIL64;
    return true;
  }
  return false;
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::getPtr(Ptr64<T> & ptr, Uint64 i) const
{
  ptr.i = i;
  ptr.p = thePool.getPtr(i);
}

template <typename P, typename M>
inline
void
DLM64HashTable<P, M>::getPtr(Ptr64<T> & ptr) const
{
  thePool.getPtr(ptr);
}

template <typename P, typename M>
inline
typename P::Type *
DLM64HashTable<P, M>::getPtr(Uint64 i) const
{
  return thePool.getPtr(i);
}

template <typename P, typename M>
inline
bool
DLM64HashTable<P, M>::find(Ptr64<T> & ptr, const T & key) const
{
  const Uint64 hv = M::hashValue(key) & mask;

  Uint64 i;
  T * p;

  i = hashValues[hv];
  while (i != RNIL64)
  {
    p = thePool.getPtr(i);
    if (M::equal(key, * p))
    {
      ptr.i = i;
      ptr.p = p;
      return true;
    }
    i = M::nextHash(*p);
  }
  ptr.i = RNIL64;
  ptr.p = NULL;
  return false;
}

// Specializations

template <typename P, typename U = typename P::Type >
class DL64HashTable: public DLM64HashTable<P, DL64HashTableDefaultMethods<typename P::Type, U> >
{
public:
  explicit DL64HashTable(P & p): DLM64HashTable<P, DL64HashTableDefaultMethods<typename P::Type, U> >(p) { }
private:
  DL64HashTable(const DL64HashTable&);
  DL64HashTable&  operator=(const DL64HashTable&);
};

#undef JAM_FILE_ID

#endif
