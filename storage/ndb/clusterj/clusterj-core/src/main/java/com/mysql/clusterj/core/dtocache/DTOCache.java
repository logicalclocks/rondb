/*
   Copyright (c) 2020, 2021, Logical Clocks and/or its affiliates.

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

package com.mysql.clusterj.core.dtocache;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.HashMap;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.core.SessionImpl;


public class DTOCache {

    private class CacheObject {
        public CacheObject() {
            next_global = null;
            prev_global = null;
            next_class = null;
            prev_class = null;
            elem = null;
            age = 0;
            type = Object.class;
        }
        CacheObject next_global;
        CacheObject prev_global;
        CacheObject next_class;
        CacheObject prev_class;
        Object elem;
        Class<?> type;
        long age;
    }

    private class CacheEntry {
        CacheObject theFirst;
        CacheObject theLast;

        public CacheEntry() {
            theFirst = null;
            theLast = null;
        }

        public CacheObject get() {
            CacheObject first = theFirst;
            if (first != null)
                remove(first);
            return first;
        }

        public void add(CacheObject cache_object) {
            cache_object.next_class = theFirst;
            cache_object.prev_class = null;
            if (theFirst == null) {
                theLast = cache_object;
            } else {
                theFirst.prev_class = cache_object;
            }
            theFirst = cache_object;
        }

        public void remove(CacheObject cache_object) {
            if (theFirst == cache_object) {
                if (theLast == cache_object) {
                    theFirst = null;
                    theLast = null;
                } else {
                    theFirst = cache_object.next_class;
                    theFirst.prev_class = null;
                }
            } else if (theLast == cache_object) {
                theLast = theLast.prev_class;
                theLast.next_class = null;
            } else {
                cache_object.prev_class.next_class = cache_object.next_class;
                cache_object.next_class.prev_class = cache_object.prev_class;
            }
            cache_object.prev_class = null;
            cache_object.next_class = null;
        }
    }

    private final Map<Class, CacheEntry> cacheMap =
            new HashMap<Class, CacheEntry>();

    CacheEntry unusedCacheObjects;

    CacheObject theFirst, theLast;

    Session session;

    long theOldestAge;
    long theCurrentAge;
    int theCacheSize;

    public DTOCache(Session session, int cacheSize) {
        this.session = session;
        theFirst = null;
        theLast = null;
        theOldestAge = 0;
        theCurrentAge = 0;
        unusedCacheObjects = new CacheEntry();
        for (int i = 0; i < cacheSize; i++) {
            CacheObject cache_object = new CacheObject();
            unusedCacheObjects.add(cache_object);
        }
    }

    private void removeGlobal(CacheObject cache_object) {
        if (theFirst == cache_object) {
            if (theLast == cache_object) {
                theFirst = null;
                theLast = null;
            } else {
                theFirst = cache_object.next_global;
                theFirst.prev_global = null;
            }
        } else if (theLast == cache_object) {
            theLast = theLast.prev_global;
            theLast.next_global = null;
            theOldestAge = theLast.age;
        } else {
            cache_object.prev_global.next_global = cache_object.next_global;
            cache_object.next_global.prev_global = cache_object.prev_global;
        }
        cache_object.prev_global = null;
        cache_object.next_global = null;
    }

    private CacheObject removeLastGlobal() {
        CacheObject last_object = theLast;
        removeGlobal(last_object);
        return last_object;
    }

    private void insertGlobal(CacheObject cache_object) {
        cache_object.next_global = theFirst;
        cache_object.prev_global = null;
        if (theFirst == null) {
            theLast = cache_object;
            theOldestAge = theCurrentAge;
        } else {
            theFirst.prev_global = cache_object;
        }
        theFirst = cache_object;
    }
 
    public <T> void put(T element, Class<?> cls) {
        /**
         *  First get hold of the proper CacheEntry class for this element
         *  type.
         */
        CacheEntry cacheEntry = cacheMap.get(cls);
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            cacheMap.put(cls, cacheEntry);
        }

        /**
         * Get an unused cache object if there are still unused ones.
         */
        CacheObject cache_object = unusedCacheObjects.get();
        if (cache_object == null) {
            /**
             * No unused objects around, find the oldest CacheObject in the
             * global list and replace this object.
             */
            if ((theCurrentAge - theOldestAge) < 4 * theCacheSize) {
                session.release(element);
                return;
            }
            CacheObject remove_object = removeLastGlobal();
            CacheEntry remove_cache_entry = cacheMap.get(remove_object.type);
            remove_cache_entry.remove(remove_object);
            session.release(remove_object.elem);
            remove_object.elem = null;
            remove_object.type = Object.class;
            cache_object = remove_object;
        }
        cache_object.age = theCurrentAge;
        cache_object.elem = (Object)element;
        cache_object.type = cls;
        insertGlobal(cache_object);
        cacheEntry.add(cache_object);
        theCurrentAge++;
    }

    public <T> T get(Class<T> type) {
        // Get the CacheEntry class for this type
        CacheEntry cacheEntry = cacheMap.get(type);
        if (cacheEntry == null) {
            return null;
        }
        // Get the latest used object of this type
        CacheObject cache_elem = cacheEntry.get();
        if (cache_elem == null) {
            return null;
        }
        /**
         * Now remove the CacheObject from global list and place it in the
         * unused list for a put operation to use.
         */
        T ret_elem = (T)cache_elem.elem;
        removeGlobal(cache_elem);
        unusedCacheObjects.add(cache_elem);
        cache_elem.age = 0;
        return ret_elem;
    }

    private void removeAll(CacheEntry cacheEntry) {
        while (true) {
            CacheObject cache_object = cacheEntry.get();
            if (cache_object == null)
                break;
            session.release(cache_object.elem);
            removeGlobal(cache_object);
            unusedCacheObjects.add(cache_object);
            cache_object.age = 0;
        }
    }
        

    public <T> void drop(Class<?> type) {
        // Get the CacheEntry class for this type
        CacheEntry cacheEntry = cacheMap.get(type);
        if (cacheEntry == null) {
            return;
        }
        removeAll(cacheEntry);
        cacheMap.remove(type);
    }

    public void drop() {
        Set<Entry<Class, CacheEntry>> setOfEntries = cacheMap.entrySet();
        Iterator<Entry<Class, CacheEntry>> iterator = setOfEntries.iterator();
        while (iterator.hasNext()) {
            Entry<Class, CacheEntry> entry = iterator.next();
            CacheEntry cacheEntry = entry.getValue();
            if (cacheEntry == null) {
                return;
            }
            removeAll(cacheEntry);
            iterator.remove();
        }
    }
}
