package com.mysql.clusterj.core;

import com.mysql.clusterj.Constants;
import com.mysql.clusterj.Session;

import java.util.*;

public class SessionCache {
  private Map<String, Queue<Session>> cachedSessions = new IdentityHashMap<String,
          Queue<Session>>();
  private int totalCachedSessions;
  private final int MAX_CACHED_SESSIONS;

  // LRU
  private SessionImpl firstLRUList;
  private SessionImpl lastLRUList;

  public SessionCache(int max_cached_sessions) {
    if (max_cached_sessions < 0) {
      throw new IllegalArgumentException(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS +
              " can not be less than 0");
    }
    this.MAX_CACHED_SESSIONS = max_cached_sessions;
    this.totalCachedSessions = 0;
    this.firstLRUList = null;
    this.lastLRUList = null;
  }

  public synchronized void dropSessionCache() {
    if (MAX_CACHED_SESSIONS == 0) {
      return;
    }

    while (cachedSessions.keySet().size() > 0) {
      String databaseName = (String)cachedSessions.keySet().toArray()[0];
      while (true) {
        Session db_session = getCachedSession(databaseName);
        if (db_session == null) {
          break;
        }
        SessionImpl db_ses = (SessionImpl) db_session;
        db_ses.setCached(false);
        db_session.close();
      }
    }
  }

  public synchronized Session getCachedSession(String databaseName) {
    if (MAX_CACHED_SESSIONS == 0 || totalCachedSessions == 0) {
      return null;
    }

    Queue<Session> db_queue = cachedSessions.get(databaseName);
    if (db_queue == null) {
      return null;
    }

    Session cached_session = db_queue.poll();
    if (cached_session == null) {
      cachedSessions.remove(databaseName);
      return null;
    }

    totalCachedSessions--;

    if (totalCachedSessions == 0){
      cachedSessions.remove(databaseName);
    }

    SessionImpl ses = (SessionImpl) cached_session;
    ses.setCached(false);
    removeFromLRUList(ses);
    return cached_session;
  }

  public synchronized void storeCachedSession(Session session, String databaseName) {
    if (MAX_CACHED_SESSIONS == 0) {
      return;
    }

    SessionImpl sesImpl = (SessionImpl) session;
    totalCachedSessions++;
    sesImpl.setCached(true);
    Queue<Session> db_queue = cachedSessions.get(databaseName);
    if (db_queue == null) {
      db_queue = new LinkedList();
      cachedSessions.put(databaseName, db_queue);
    }
    db_queue.add(session);
    addFirstToLRUList(sesImpl);
    validateCacheSize();
  }

  private synchronized void validateCacheSize() {
    if (totalCachedSessions > MAX_CACHED_SESSIONS) {
      SessionImpl sesImpl = removeLastFromLRUList();
      sesImpl.setCached(false);
      sesImpl.close();
    }
  }

  public synchronized void removeCachedSessions(String databaseName) {
    if (MAX_CACHED_SESSIONS == 0) {
      return;
    }
    synchronized (this) {
      Queue<Session> db_queue = cachedSessions.get(databaseName);
      if (db_queue != null) {
        int size = db_queue.size();
        while (!db_queue.isEmpty()) {
          Session session = db_queue.poll();
          session.close();
        }
        cachedSessions.remove(databaseName);
        totalCachedSessions -= size;
      }
    }
  }

  private synchronized void addFirstToLRUList(SessionImpl session) {
    session.setNextLruList(firstLRUList);
    session.setPrevLruList(null);
    if (firstLRUList == null) {
      lastLRUList = session;
    } else {
      firstLRUList.setPrevLruList(session);
    }
    firstLRUList = session;
  }
  private synchronized void removeFromLRUList(SessionImpl session) {
    SessionImpl next = session.getNextLruList();
    SessionImpl prev = session.getPrevLruList();
    if (prev == null) {
      firstLRUList = next;
    } else {
      prev.setNextLruList(next);
    }
    if (next == null) {
      lastLRUList = prev;
    } else {
      next.setPrevLruList(prev);
    }
  }

  private synchronized SessionImpl removeLastFromLRUList() {
    assert (lastLRUList != null);
    return (SessionImpl) getCachedSession(lastLRUList.getDatabaseName());
  }

  public synchronized int size() {
    return totalCachedSessions;
  }

  public synchronized int size(String db) {
    Queue<Session> db_queue = cachedSessions.get(db);
    if (db_queue == null) {
      return 0;
    } else {
      return db_queue.size();
    }
  }
}
