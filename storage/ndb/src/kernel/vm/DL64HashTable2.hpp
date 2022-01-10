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

#ifndef DL64_HASHTABLE2_HPP
#define DL64_HASHTABLE2_HPP

#include <ndb_global.h>

#define JAM_FILE_ID 13


/**
 * DL64HashTable2 is a DL64HashTable variant meant for cases where different
 * DL64HashTable instances share a common pool (based on a union U).
 *
 * Calls T constructor after seize from pool and T destructor before
 * release (in all forms) into pool.
 */
template <class P, class T = typename P::Type>
class DL64HashTable2
{
  typedef typename P::Type U;
public:
  DL64HashTable2(P & thePool);
  ~DL64HashTable2();
  
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
   * Remove element (and set Ptr to removed element)
   * And return element to pool
   */
  void release(Ptr64<T> &, const T & key);

  /**
   * Remove element and return to pool
   */
  void release(Uint64 i);

  /**
   * Remove element and return to pool
   */
  void release(Ptr64<T> &);
  
  class Iterator
  {
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
  
  /**
   * Next Element
   *
   * param iter - A "fully set" iterator
   */
  bool next(Iterator & iter) const;

  /**
   * Get next element starting from bucket
   *
   * @param bucket - Which bucket to start from
   * @param iter - An "uninitialized" iterator
   */
  bool next(Uint64 bucket, Iterator & iter) const;

  inline bool isEmpty() const { Iterator iter; return ! first(iter); }
  
private:
  Uint64 mask;
  Uint64 * hashValues;
  P & thePool;
};

template<class P, class T>
inline
DL64HashTable2<P, T>::DL64HashTable2(P & _pool)
  : thePool(_pool)
{
  mask = 0;
  hashValues = 0;
}

template<class P, class T>
inline
DL64HashTable2<P, T>::~DL64HashTable2()
{
  if (hashValues != 0)
    delete [] hashValues;
}

template<class P, class T>
inline
bool
DL64HashTable2<P, T>::setSize(Uint64 size)
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

template<class P, class T>
inline
void
DL64HashTable2<P, T>::add(Ptr64<T> & obj)
{
  const Uint64 hv = obj.p->hashValue() & mask;
  const Uint64 i  = hashValues[hv];
  
  if (i == RNIL64)
  {
    hashValues[hv] = obj.i;
    obj.p->nextHash = RNIL64;
    obj.p->prevHash = RNIL64;
  }
  else
  {
    T * tmp = (T*)thePool.getPtr(i);    // cast
    tmp->prevHash = obj.i;
    obj.p->nextHash = i;
    obj.p->prevHash = RNIL64;
    
    hashValues[hv] = obj.i;
  }
}

/**
 * First element
 */
template<class P, class T>
inline
bool
DL64HashTable2<P, T>::first(Iterator & iter) const
{
  Uint64 i = 0;
  while (i <= mask && hashValues[i] == RNIL64) i++;
  if (i <= mask)
  {
    iter.bucket = i;
    iter.curr.i = hashValues[i];
    iter.curr.p = (T*)thePool.getPtr(iter.curr.i);      // cast
    return true;
  }
  else
  {
    iter.curr.i = RNIL64;
  }
  return false;
}

template<class P, class T>
inline
bool
DL64HashTable2<P, T>::next(Iterator & iter) const
{
  if (iter.curr.p->nextHash == RNIL64)
  {
    Uint64 i = iter.bucket + 1;
    while (i <= mask && hashValues[i] == RNIL64) i++;
    if (i <= mask)
    {
      iter.bucket = i;
      iter.curr.i = hashValues[i];
      iter.curr.p = (T*)thePool.getPtr(iter.curr.i);    // cast
      return true;
    }
    else
    {
      iter.curr.i = RNIL64;
      return false;
    }
  }
  
  iter.curr.i = iter.curr.p->nextHash;
  iter.curr.p = (T*)thePool.getPtr(iter.curr.i);        // cast
  return true;
}

template<class P, class T>
inline
void
DL64HashTable2<P, T>::remove(Ptr64<T> & ptr, const T & key)
{
  const Uint64 hv = key.hashValue() & mask;  
  
  Uint64 i;
  T * p;
  Ptr64<T> prev;
  prev.i = RNIL64;

  i = hashValues[hv];
  while (i != RNIL64)
  {
    p = (T*)thePool.getPtr(i);  // cast
    if (key.equal(* p))
    {
      const Uint64 next = p->nextHash;
      if (prev.i == RNIL64)
      {
	hashValues[hv] = next;
      }
      else
      {
	prev.p->nextHash = next;
      }
      
      if (next != RNIL64)
      {
	T * nextP = (T*)thePool.getPtr(next);   // cast
	nextP->prevHash = prev.i;
      }

      ptr.i = i;
      ptr.p = p;
      return;
    }
    prev.p = p;
    prev.i = i;
    i = p->nextHash;
  }
  ptr.i = RNIL64;
}

template<class P, class T>
inline
void
DL64HashTable2<P, T>::release(Ptr64<T> & ptr, const T & key)
{
  const Uint64 hv = key.hashValue() & mask;  
  
  Uint64 i;
  T * p;
  Ptr64<T> prev;
  prev.i = RNIL64;

  i = hashValues[hv];
  while (i != RNIL64)
  {
    p = (T*)thePool.getPtr(i);  // cast
    if (key.equal(* p))
    {
      const Uint64 next = p->nextHash;
      if (prev.i == RNIL64)
      {
	hashValues[hv] = next;
      }
      else
      {
	prev.p->nextHash = next;
      }
      
      if (next != RNIL64)
      {
	T * nextP = (T*)thePool.getPtr(next);   // cast
	nextP->prevHash = prev.i;
      }

      p->~T();  // dtor
      thePool.release(i);
      ptr.i = i;
      ptr.p = p;        // invalid
      return;
    }
    prev.p = p;
    prev.i = i;
    i = p->nextHash;
  }
  ptr.i = RNIL64;
}

template<class P, class T>
inline
void
DL64HashTable2<P, T>::remove(Uint64 i)
{
  Ptr64<T> tmp;
  tmp.i = i;
  tmp.p = (T*)thePool.getPtr(i);        // cast
  remove(tmp);
}

template<class P, class T>
inline
void
DL64HashTable2<P, T>::release(Uint64 i)
{
  Ptr64<T> tmp;
  tmp.i = i;
  tmp.p = (T*)thePool.getPtr(i);        // cast
  release(tmp);
}

template<class P, class T>
inline
void 
DL64HashTable2<P, T>::remove(Ptr64<T> & ptr)
{
  const Uint64 next = ptr.p->nextHash;
  const Uint64 prev = ptr.p->prevHash;

  if (prev != RNIL64)
  {
    T * prevP = (T*)thePool.getPtr(prev);       // cast
    prevP->nextHash = next;
  }
  else
  {
    const Uint64 hv = ptr.p->hashValue() & mask;  
    if (hashValues[hv] == ptr.i)
    {
      hashValues[hv] = next;
    }
    else
    {
      // Will add assert in 5.1
    }
  }
  
  if (next != RNIL64)
  {
    T * nextP = (T*)thePool.getPtr(next);       // cast
    nextP->prevHash = prev;
  }
}

template<class P, class T>
inline
void 
DL64HashTable2<P, T>::release(Ptr64<T> & ptr)
{
  const Uint64 next = ptr.p->nextHash;
  const Uint64 prev = ptr.p->prevHash;

  if (prev != RNIL64)
  {
    T * prevP = (T*)thePool.getPtr(prev);       // cast
    prevP->nextHash = next;
  }
  else
  {
    const Uint64 hv = ptr.p->hashValue() & mask;  
    if (hashValues[hv] == ptr.i)
    {
      hashValues[hv] = next;
    }
    else
    {
      // Will add assert in 5.1
    }
  }
  
  if (next != RNIL64)
  {
    T * nextP = (T*)thePool.getPtr(next);       // cast
    nextP->prevHash = prev;
  }
  
  thePool.release(ptr.i);
}

template<class P, class T>
inline
void 
DL64HashTable2<P, T>::removeAll()
{
  for (Uint64 i = 0; i<=mask; i++)
    hashValues[i] = RNIL64;
}

template<class P, class T>
inline
bool
DL64HashTable2<P, T>::next(Uint64 bucket, Iterator & iter) const
{
  while (bucket <= mask && hashValues[bucket] == RNIL64) 
    bucket++; 
  
  if (bucket > mask)
  {
    iter.bucket = bucket;
    iter.curr.i = RNIL64;
    return false;
  }
  
  iter.bucket = bucket;
  iter.curr.i = hashValues[bucket];
  iter.curr.p = (T*)thePool.getPtr(iter.curr.i);        // cast
  return true;
}

template<class P, class T>
inline
bool
DL64HashTable2<P, T>::seize(Ptr64<T> & ptr)
{
  Ptr64<U> ptr2;
  thePool.seize(ptr2);
  ptr.i = ptr2.i;
  ptr.p = (T*)ptr2.p;   // cast
  if (ptr.p != NULL)
  {
    ptr.p->nextHash = RNIL64;
    ptr.p->prevHash = RNIL64;
    new (ptr.p) T;      // ctor
  }
  return !ptr.isNull();
}

template<class P, class T>
inline
void
DL64HashTable2<P, T>::getPtr(Ptr64<T> & ptr, Uint64 i) const
{
  ptr.i = i;
  ptr.p = (T*)thePool.getPtr(i);        // cast
}

template<class P, class T>
inline
void
DL64HashTable2<P, T>::getPtr(Ptr64<T> & ptr) const
{
  Ptr64<U> ptr2;
  thePool.getPtr(ptr2);
  ptr.i = ptr2.i;
  ptr.p = (T*)ptr2.p;   // cast
}

template<class P, class T>
inline
T * 
DL64HashTable2<P, T>::getPtr(Uint64 i) const
{
  return (T*)thePool.getPtr(i); // cast
}

template<class P, class T>
inline
bool
DL64HashTable2<P, T>::find(Ptr64<T> & ptr, const T & key) const
{
  const Uint64 hv = key.hashValue() & mask;  
  
  Uint64 i;
  T * p;

  i = hashValues[hv];
  while (i != RNIL64)
  {
    p = (T*)thePool.getPtr(i);  // cast
    if (key.equal(* p))
    {
      ptr.i = i;
      ptr.p = p;
      return true;
    }
    i = p->nextHash;
  }
  ptr.i = RNIL64;
  ptr.p = NULL;
  return false;
}

#undef JAM_FILE_ID

#endif
