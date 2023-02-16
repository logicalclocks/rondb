/*
   Copyright (c) 2010, 2023, Oracle and/or its affiliates.
   Copyright (c) 2020, 2023, Hopsworks and/or its affiliates.

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

package testsuite.clusterj;

import com.mysql.clusterj.Constants;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.core.SessionCache;
import com.mysql.clusterj.core.SessionFactoryImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/*
Fixes for recreating a table with the same name while using session cache.
 */
public class SessionCacheTest extends AbstractClusterJModelTest {

  private final int SESSION_CACHE_SIZE = 100;

  @Override
  protected Properties modifyProperties() {
    props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_INSTANCES, Integer.toString(SESSION_CACHE_SIZE));
    props.setProperty(Constants.PROPERTY_CLUSTER_WARMUP_CACHED_SESSIONS, Integer.toString(SESSION_CACHE_SIZE));
    props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, Integer.toString(SESSION_CACHE_SIZE));
    return props;
  }

  @Override
  public void localSetUp() {
    createSessionFactory();
  }


  Session getSession(String db) {
    if (db == null) {
      return sessionFactory.getSession();
    } else {
      return sessionFactory.getSession(db);
    }
  }

  void returnSession(Session s, boolean useCache) {
    if (useCache) {
      s.closeCache();
    } else {
      s.close();
    }
  }

  public void testWithCache() throws Exception {
    sessionTest(true);
    failOnError();
  }

  public void testWithoutCache() throws Exception {
    sessionTest(false);
    failOnError();
  }

  public void sessionTest(boolean useCache) throws Exception {

    closeSession();
    closeAllExistingSessionFactories();
    sessionFactory = null;
    createSessionFactory();

    SessionFactoryImpl sessionFactoryImpl = (SessionFactoryImpl) sessionFactory;
    SessionCache sessionCache = sessionFactoryImpl.getSessionCache();
    sessionCache.dropSessionCache();

    if (sessionCache.size() != 0) {
      this.error("FAIL. Expecting session size to be: " + 0);
      return;
    }

    // Mix databases session cache test
    List<Session> sessionList = new ArrayList<>();
    int itr = 10;
    for (int i = 0; i < itr; i++) {
      sessionList.add(getSession("test"));
      sessionList.add(getSession("test2"));
      sessionList.add(getSession("test3"));
    }

    // so far nothing has been returned to the cache
    if (sessionCache.size() != 0) {
      this.error("FAIL. Expecting session size to be: " + 0);
      return;
    }

    for (Session session : sessionList) {
      if (useCache) {
        session.closeCache();
      } else {
        session.close();
      }
    }
    sessionList.clear();

    if (useCache) {
      if (sessionCache.size() != itr * 3) {
        this.error("FAIL. Expecting session size to be: " + itr * 3 + ". Got: " + sessionCache.size());
        return;
      }

      String dbs[] = {"test", "test2", "test3"};
      for (String db : dbs) {
        if (sessionCache.size(db) != itr) {
          this.error("FAIL. Expecting session size to be: " + itr + ". Got: " + sessionCache.size(db));
          return;
        }
      }
    } else {
      if (sessionCache.size() != 0) {
        this.error("FAIL. Expecting session size to be: 0.  Got: " + sessionCache.size());
        return;
      }
    }


    int newSessionsCount = SESSION_CACHE_SIZE * 2;
    for (int i = 0; i < newSessionsCount; i++) {
      sessionList.add(getSession("test"));
    }

    for (int i = 0;i  < newSessionsCount;i++) {
      if(useCache) {
        sessionList.get(i).closeCache();
      }
      else {
        sessionList.get(i).close();
      }
    }
    sessionList.clear();


    if (useCache) {
      if (sessionCache.size() != SESSION_CACHE_SIZE) {
        this.error("FAIL. Expecting session size to be: " + SESSION_CACHE_SIZE + ". Got: " + sessionCache.size());
        return;
      }
    } else {
      if (sessionCache.size() != 0) {
        this.error("FAIL. Expecting session size to be: " + 0 + ". Got: " + sessionCache.size());
        return;
      }
    }

    if (useCache) {
      if (sessionCache.size("test") != SESSION_CACHE_SIZE) {
        this.error("FAIL. Expecting session size to be: " + SESSION_CACHE_SIZE + ".  Got:" +
                " " + sessionCache.size("test"));
        return;
      }
    } else {
      if (sessionCache.size() != 0) {
        this.error("FAIL. Expecting session size to be: 0.  Got: " + sessionCache.size());
        return;
      }
    }

    sessionCache.dropSessionCache();
    if (sessionCache.size() != 0) {
      this.error("FAIL. Expecting session size to be: " + 0 + " Got: " + sessionCache.size());
      return;
    }
  }
}
