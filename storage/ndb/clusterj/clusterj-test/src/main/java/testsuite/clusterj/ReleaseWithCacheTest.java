/*
   Copyright (c) 2015, 2022, Oracle and/or its affiliates.
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

package testsuite.clusterj;


import com.mysql.clusterj.Constants;
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/*
Using DynamicObjects. Created separate class for each table
 */
public class ReleaseWithCacheTest extends AbstractClusterJModelTest {

  private static final int NUMBER_TO_INSERT = 4096;
  private static String defaultDB;

  boolean useCache = false;

  public static class EmpBasic1 extends DynamicObject {
    @Override
    public String table() {
      return "t_basic";
    }
  }

  public static class EmpBasic2 extends DynamicObject {
    @Override
    public String table() {
      return "t_basic2";
    }
  }

  public static class EmpBasic3 extends DynamicObject {
    @Override
    public String table() {
      return "t_basic3";
    }
  }

  @Override
  protected Properties modifyProperties() {
    // Modify JDBC properties to enable caching
    Properties modifiedProps = new Properties();
    modifiedProps.putAll(props);

    modifiedProps.put(Constants.PROPERTY_CLUSTER_WARMUP_CACHED_SESSIONS, 10);
    modifiedProps.put(Constants.PROPERTY_CLUSTER_MAX_CACHED_INSTANCES, 10);
    modifiedProps.put(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, 10);

    return modifiedProps;
  }

  @Override
  public void localSetUp() {
    createSessionFactory();
    defaultDB = props.getProperty(Constants.PROPERTY_CLUSTER_DATABASE);
  }

  public void cleanUp() {
    cleanUpInt(defaultDB, EmpBasic1.class);
    cleanUpInt("test2", EmpBasic2.class);
    cleanUpInt("test3", EmpBasic3.class);
  }

  public void cleanUpInt(String db, Class c) {
    Session s = getSession(db);
    s.deletePersistentAll(c);
    returnSession(s);
  }

  public void testSimple() {
    useCache = false;
    cleanUp();

    List<Thread> threads = new ArrayList(3);
    threads.add(new Thread(new Worker(this, defaultDB, EmpBasic1.class)));
    threads.add(new Thread(new Worker(this, "test2", EmpBasic2.class)));
    threads.add(new Thread(new Worker(this, "test3", EmpBasic3.class)));

    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        error(e.getMessage());
      }
    }
  }

  public void testSimpleWithCache() {
    useCache = true;
    cleanUp();

    List<Thread> threads = new ArrayList(3);
    threads.add(new Thread(new Worker(this, defaultDB, EmpBasic1.class)));
    threads.add(new Thread(new Worker(this, "test2", EmpBasic2.class)));
    threads.add(new Thread(new Worker(this, "test3", EmpBasic3.class)));

    for (Thread t : threads) {
      t.start();
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        error(e.getMessage());
      }
    }
  }

  public void runTest(String db, Class cls) {
    //System.out.println("Adding rows to DB: " + db + " table: " + cls);
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(db);
      DynamicObject e = (DynamicObject) s.newInstance(cls);
      MultiDBHelper.setEmployeeFields(this, e, i);
      s.savePersistent(e);
      closeDTO(s, e, cls);
      returnSession(s);
    }

    // now verify data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(db);
      DynamicObject e = (DynamicObject) s.find(cls, i);
      MultiDBHelper.verifyEmployeeFields(this, e, i);
      closeDTO(s, e, cls);
      returnSession(s);
    }

    // now delete data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(db);
      DynamicObject e = (DynamicObject) s.find(cls, i);
      s.deletePersistent(e);
      closeDTO(s, e, cls);
      returnSession(s);
    }

    failOnError();
  }

  Session getSession(String db) {
    if (db == null) {
      return sessionFactory.getSession();
    } else {
      return sessionFactory.getSession(db);
    }
  }

  void returnSession(Session s) {
    if (useCache) {
      s.closeCache();
    } else {
      s.close();
    }
  }

  void closeDTO(Session s, DynamicObject dto, Class dtoClass) {
    if (useCache) {
      s.releaseCache(dto, dtoClass);
    } else {
      s.release(dto);
    }
  }

  class Worker implements Runnable {

    Class cls;
    String db;
    AbstractClusterJModelTest test;

    public Worker(AbstractClusterJModelTest test, String db, Class cls) {
      this.db = db;
      this.cls = cls;
      this.test = test;

    }

    @Override
    public void run() {
      //recreating session to each operation is inefficient but
      //here we just want to test how the
      //cache works after creating many sessions
      //and dynamic objects

      //System.out.println("Adding rows to DB: " + db + " table: " + cls);
      for (int i = 0; i < NUMBER_TO_INSERT; i++) {
        Session s = getSession(db);
        DynamicObject e = (DynamicObject) s.newInstance(cls);
        MultiDBHelper.setEmployeeFields(test, e, i);
        s.savePersistent(e);
        closeDTO(s, e, cls);
        returnSession(s);
      }

      // now verify data
      for (int i = 0; i < NUMBER_TO_INSERT; i++) {
        Session s = getSession(db);
        DynamicObject e = (DynamicObject) s.find(cls, i);
        MultiDBHelper.verifyEmployeeFields(test, e, i);
        closeDTO(s, e, cls);
        returnSession(s);
      }

      // now delete data
      for (int i = 0; i < NUMBER_TO_INSERT; i++) {
        Session s = getSession(db);
        DynamicObject e = (DynamicObject) s.find(cls, i);
        s.deletePersistent(e);
        closeDTO(s, e, cls);
        returnSession(s);
      }
    }
  }
}
