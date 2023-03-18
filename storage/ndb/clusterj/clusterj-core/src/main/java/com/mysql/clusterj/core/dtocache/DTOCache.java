/*
   Copyright (c) 2020,2021 LogicalClocks and/or its affiliates.

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

import java.util.*;
import java.util.Map.Entry;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.core.SessionImpl;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.core.util.I18NHelper;


public class DTOCache {
    /** My message translator */
    protected static final I18NHelper local = I18NHelper.getInstance(DTOCache.class);

    /**
     * The DTOCache maintains a cache of DynamicObject's.
     *
     * DynamicObject's can be of several types. When requesting a new object one must
     * return an object of the same type. To ensure this we maintain a list of
     * cached objects of a certain type. This list is maintained by the class
     * CacheEntry. To find the correct list to fetch from we have a HashMap to find
     * the proper CacheEntry object. This map is called cacheEntryMap.
     *
     * In addition we need to maintain a global order of which objects that have
     * been cached for the longest duration. This is maintained as a global list of
     * CacheObjects. This list is maintained by the methods removeGlobal,
     * removeLastGlobal, insertGlobal. When we remove an object from the cache for
     * use we need to use removeGlobal to remove it. When we put it back in the
     * cache we use insertGlobal. When we need an old entry to remove we use the
     * removeLastGlobal method.
     *
     * We also need a method to discover if someone is trying to release an object
     * for a second time. If one calls session.release twice on the same object,
     * the second call should throw an error. However for this we need to have
     * a quick check that the object isn't cached. If an object is cached it cannot
     * be released again. Only the cache is now able to release the object.
     *
     * When the application drops a table, alters a table with copy algorithm or
     * truncate the table, in all those cases the table is dropped and possibly
     * a new one is created. The application will discover this through errors
     * when trying to use the table. In this case the application should call
     * unloadSchema to remove all objects of this table. Since a table isn't
     * necessarily mapped to only one class we actually drop the entire
     * cache in those situations. This is handled by the drop call.
     *
     * However when we drop we could still have elements that are maintained by
     * the cache that was in use when the drop call was made. When those elements
     * are released we need to ensure that those elements are not put back in the
     * cache, those objects must be properly released.
     *
     * In order to handle this we put objects that are currently in use into a
     * HashMap called inUseMap. When a drop call is made we will traverse this
     * list and ensure that all objects in use are marked as invalid to ensure
     * they are removed when they are released.
     *
     * Thus cached objects are always to be found in 3 places, the global list
     * of cached objects, the local list of cached objects of a certain type
     * (maintained by CacheEntry) and finally in the inCacheMap.
     *
     * Objects that are currently in use is found in the inUseMap and in no
     * other place.
     *
     * In addition we have a list of free CacheObjects that is maintained in
     * the unusedCacheObjects object (this is a CacheEntry object). This list
     * should normally be empty if enough objects are used in the cache. But
     * it will be full after creation, after a drop and after releasing
     * objects that require release.
     *
     * The public interface to DTOCache is that "get()" is called to retrieve
     * an object from the cache.
     *
     * The "put" method is used to put an object back into the cache when the
     * application called releaseCache.
     *
     * The "remove" method is called right before releasing an object when the
     * normal release method is called. This release method is sometimes
     * called from this DTOCache object and thus it knows which SessionImpl
     * object it is part of.
     *
     * Finally the "insert" method is called when a new object is created that
     * should be handled by the cache.
     *
     * The cache doesn't currently handle all variants of DynamicObjects. Thus
     * an object that is present neither in inUseMap nor in inCacheMap is
     * not maintained by the cache and will always be immediately released.
     *
     * There is also two "drop" calls to remove cached objects of a certain
     * type from the cache.
     *
     * The cache will at most contain a maximum number of objects. This
     * includes both objects that are cached and objects that are in use.
     * We can temporarily increase the size of the number of objects to
     * to ensure that this doesn't limit the number of objects that can be
     * in use in parallel. Those objects will be released again when the
     * release calls are made.
     */
    private class CacheObject {
        public CacheObject() {
            next_global = null;
            prev_global = null;
            next_class = null;
            prev_class = null;
            elem = null;
            type = Object.class;
            valid_object = true;
        }
        CacheObject next_global;
        CacheObject prev_global;
        CacheObject next_class;
        CacheObject prev_class;
        Object elem;
        Class<?> type;
        boolean valid_object;
    }

    private class CacheEntry {
        CacheObject theFirst;
        CacheObject theLast;

        public CacheEntry() {
            theFirst = null;
            theLast = null;
        }

        public CacheObject get_and_remove() {
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

    private final Map<Class, CacheEntry> cacheEntryMap =
            new HashMap<Class, CacheEntry>();

    private final Map<Object, CacheObject> inCacheMap =
            new IdentityHashMap<>();

    private final Map<Object, CacheObject> inUseMap =
            new IdentityHashMap<>();

    CacheEntry unusedCacheObjects;

    CacheObject theFirst, theLast;

    Session session;

    int theCacheSize;
    int theCurrentCacheSize;

    public DTOCache(Session session, int cacheSize) {
        this.session = session;
        theFirst = null;
        theLast = null;
        theCacheSize = cacheSize;
        theCurrentCacheSize = cacheSize;
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
        } else {
            cache_object.prev_global.next_global = cache_object.next_global;
            cache_object.next_global.prev_global = cache_object.prev_global;
        }
        cache_object.prev_global = null;
        cache_object.next_global = null;
    }

    private CacheObject removeLastGlobal() {
        CacheObject last_object = theLast;
        if (last_object == null) {
            return null;
        }
        removeGlobal(last_object);
        return last_object;
    }

    private void insertGlobal(CacheObject cache_object) {
        cache_object.next_global = theFirst;
        cache_object.prev_global = null;
        if (theFirst == null) {
            theLast = cache_object;
        } else {
            theFirst.prev_global = cache_object;
        }
        theFirst = cache_object;
    }

    public <T> void remove(T element) {
        if (theCacheSize == 0) {
            return;
        }
        CacheObject cache_object = inUseMap.remove(element);
        if (cache_object == null) {
            cache_object = inCacheMap.get(element);
            if (cache_object != null) {
                throw new ClusterJUserException(local.message("ERR_Cannot_Access_Object_After_Release"));
            }
            /**
             * The object isn't handled by cache, simply continue the release
             * process.
             */
            return;
        }
        cache_object.elem = null;
        if (theCurrentCacheSize > theCacheSize) {
            theCurrentCacheSize--;
            /**
             * We forget the object, this should ensure Java GC removes it.
             * The element is dropped with the above setting of elem to
             * null and since we don't insert the cache_object into any
             * new map it should be forgotten.
             */
            return;
        }
        cache_object.valid_object = true;
        cache_object.type = Object.class;
        unusedCacheObjects.add(cache_object);
        return;
    }
    public <T> void insert(T element, Class<?> cls) {
        /**
         * Get an unused cache object if there are still unused ones.
         * If no unused one around, remove the oldest one around, if
         * neither that is around then increase the cache size
         * temporarily to ensure that we can track all outstanding
         * objects.
         */
        if (theCacheSize == 0) {
            return;
        }
        if (element == null) {
            return;
        }
        CacheObject cache_object = unusedCacheObjects.get_and_remove();
        if (cache_object == null) {
            cache_object = removeLastGlobal();
            if (cache_object == null) {
              cache_object = new CacheObject();
              theCurrentCacheSize++;
            } else {
              // Remove an object from cache to house this new one
              cache_object = inCacheMap.remove(cache_object.elem);
              CacheEntry cacheEntry = cacheEntryMap.get(cache_object.type);
              cacheEntry.remove(cache_object);
              session.release(cache_object.elem);
              cache_object.elem = null;
            }
        }
        cache_object.elem = element;
        cache_object.type = cls;
        cache_object.valid_object = true;
        inUseMap.put(element, cache_object);
    }

    public <T> void put(T element, Class<?> cls) {
        /**
         *  First get hold of the proper CacheEntry class for this element
         *  type.
         */
        CacheObject cache_object = inUseMap.remove(element);
        if (cache_object == null) {
            cache_object = inCacheMap.get(element);
            if (cache_object != null) {
                throw new ClusterJUserException(local.message("ERR_Cannot_Access_Object_After_Release"));
            }
            /**
             * The object was neither in inUseMap nor in inCacheMap, thus the
             * object is not maintained in the cache and we should simply
             * release it. The same checks will be applied in the remove
             * method that is called from the release method (the release
             * method sets is_cache_allowed to false).
             */
            session.release(element);
            return;
        }
        if (!cache_object.valid_object) {
            // The object will be treated by remove above as not cached at all.
            session.release(element);
            return;
        }
        CacheEntry cacheEntry = cacheEntryMap.get(cls);
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            cacheEntryMap.put(cls, cacheEntry);
        }
        if (theCurrentCacheSize > theCacheSize) {
            /**
             * We have grown the cache beyond its configured size,
             * we need to remove one cache object here, either the
             * one that we found in inUseMap or the oldest one in
             * the cacheEntryMap. Since removing the CacheObject from
             * the inUseMap is the last occurrence it will
             * disappear unless we put it into a new data structure.
             */
            theCurrentCacheSize--;
            CacheObject remove_object = removeLastGlobal();
            if (remove_object == null) {
                /**
                 * The cache is empty, too many outstanding cache objects.
                 * We will release this element and the CacheObject will
                 * be handled by the Java GC since we haven't put it into
                 * any new data structure.
                 */
                session.release(element);
                return;
            }
            //  Remove object from cache to house a new one.
            remove_object = inCacheMap.remove(remove_object.elem);
            CacheEntry removeCacheEntry = cacheEntryMap.get(remove_object.type);
            removeCacheEntry.remove(remove_object);
            session.release(remove_object.elem);
            /**
             * We've removed the object from the global list, the object should
             * not be accessible anymore, so will be garbage collected by
             * the Java GC. Same goes for the element.
             */
        }
        cache_object.elem = (Object)element;
        cache_object.type = cls;
        insertGlobal(cache_object);
        cacheEntry.add(cache_object);
        inCacheMap.put(element, cache_object);
    }

    public <T> T get(Class<T> type) {
        // Get the CacheEntry class for this type
        if (theCacheSize == 0) {
            return null;
        }
        CacheEntry cacheEntry = cacheEntryMap.get(type);
        if (cacheEntry == null) {
            return null;
        }
        /**
         * Get the latest used object of this type
         * This method also removes the object from
         * the local list of objects for this class.
         */
        CacheObject cache_object = cacheEntry.get_and_remove();
        if (cache_object == null) {
            return null;
        }
        /**
         * Now remove the CacheObject from global list and place it in the
         * inUseMap such that we can invalidate it if the table is changed.
         * Also remove from inCacheMap.
         */
        T ret_elem = (T)cache_object.elem;
        cache_object = inCacheMap.remove(ret_elem);
        removeGlobal(cache_object);
        inUseMap.put(ret_elem, cache_object);
        cache_object.valid_object = true;
        return ret_elem;
    }

    private void removeAll(CacheEntry cacheEntry) {
        while (true) {
            CacheObject cache_object = cacheEntry.get_and_remove();
            if (cache_object == null)
                break;
            cache_object = inCacheMap.remove(cache_object.elem);
            removeGlobal(cache_object);
            session.release(cache_object.elem);
            cache_object.elem = null;
            cache_object.valid_object = true;
            unusedCacheObjects.add(cache_object);
        }
    }

    private void dropInUseMap() {
        Set<Entry<Object, CacheObject>> setOfEntries = inUseMap.entrySet();
        Iterator<Entry<Object, CacheObject>> iterator = setOfEntries.iterator();
        while (iterator.hasNext()) {
            Entry<Object, CacheObject> entry = iterator.next();
            CacheObject cache_object = entry.getValue();
            cache_object.valid_object = false;
        }
    }
        
    public <T> void drop(Class<?> type) {
        // Get the CacheEntry class for this type
        CacheEntry cacheEntry = cacheEntryMap.get(type);
        if (cacheEntry != null) {
             removeAll(cacheEntry);
             cacheEntryMap.remove(type);
        }
        dropInUseMap();
        return;
    }

    public void drop() {
        Set<Entry<Class, CacheEntry>> setOfEntries = cacheEntryMap.entrySet();
        Iterator<Entry<Class, CacheEntry>> iterator = setOfEntries.iterator();
        while (iterator.hasNext()) {
            Entry<Class, CacheEntry> entry = iterator.next();
            CacheEntry cacheEntry = entry.getValue();
            removeAll(cacheEntry);
            iterator.remove();
        }
        dropInUseMap();
    }
}
