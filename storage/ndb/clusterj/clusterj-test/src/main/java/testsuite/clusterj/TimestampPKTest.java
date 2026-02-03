/*
   Copyright (c) 2026 Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

public class TimestampPKTest extends AbstractClusterJModelTest {

  private static final int NUMBER_TO_INSERT = 128;
  private static String defaultDB;

  boolean useCache = false;

  public static class TimestampPK extends DynamicObject {
    @Override
    public String table() {
      return "timestamppk";
    }
  }

  public static class TimestampPK3 extends DynamicObject {
    @Override
    public String table() {
      return "timestamppk3";
    }
  }

  public static class TimestampPK6 extends DynamicObject {
    @Override
    public String table() {
      return "timestamppk6";
    }
  }

  @Override
  protected Properties modifyProperties() {
    props.put(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, 10);
    return props;
  }

  @Override
  public void localSetUp() {
    createSessionFactory();
    defaultDB = props.getProperty(Constants.PROPERTY_CLUSTER_DATABASE);
  }

  public void cleanUp() {
    cleanUpInt("test3", TimestampPK.class);
    cleanUpInt("test3", TimestampPK3.class);
    cleanUpInt("test3", TimestampPK6.class);
  }

  public void cleanUpInt(String db, Class c) {
    Session s = getSession(db);
    s.deletePersistentAll(c);
    returnSession(s);
  }

  public void testSimple() {
    useCache = false;
    cleanUp();
    runTest("test3", TimestampPK.class);
    runTest("test3", TimestampPK3.class);
    runTest("test3", TimestampPK6.class);
  }

//  public void testSimpleWithCache() {
//    useCache = true;
//    cleanUp();
//    runTest("test3", TimestampPK.class);
//  }

  public void runTest(String db, Class cls) {
    // Insert rows
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(db);
      DynamicObject e = (DynamicObject) s.newInstance(cls);
      setFields(this, e, i);
      s.savePersistent(e);
      closeDTO(s, e, cls);
      returnSession(s);
    }

    // now verify data
    Session s = getSession(db);
    s.currentTransaction().begin();
    ArrayList<DynamicObject> list = new ArrayList<DynamicObject>(NUMBER_TO_INSERT);
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      // Use timestamp as the primary key
      Timestamp key = getTimestampKey(i);
      DynamicObject e = (DynamicObject) s.newInstance(cls, key);
      list.add(e);
      s.load(e);
    }
    s.flush();

    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      verifyFields(this, list.get(i), i);
      closeDTO(s, list.get(i), cls);
    }
    list.clear();
    s.currentTransaction().commit();
    returnSession(s);

    // now delete data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      s = getSession(db);
      Timestamp key = getTimestampKey(i);
      DynamicObject e = (DynamicObject) s.find(cls, key);
      if (e != null) {
        s.deletePersistent(e);
        closeDTO(s, e, cls);
      } else {
        error("Failed to find row with key: " + key);
      }
      returnSession(s);
    }

    failOnError();
  }

  /** Create a timestamp key for the given index */
  private Timestamp getTimestampKey(int num) {
    long baseTime = Timestamp.valueOf("2024-01-01 00:00:00").getTime();
    return new Timestamp(baseTime + (num * 1000L)); // add num seconds
  }

  /** Verify the fields of a loaded object */
  private void verifyFields(AbstractClusterJModelTest test, DynamicObject e, int num) {
    for (int i = 0; i < e.columnMetadata().length; i++) {
      String fieldName = e.columnMetadata()[i].name();
      if (fieldName.equals("id")) {
        Timestamp expected = getTimestampKey(num);
        Timestamp actual = (Timestamp) e.get(i);
        if (actual == null || actual.getTime() != expected.getTime()) {
          test.error("Mismatch for id: expected " + expected + " but got " + actual);
        }
      } else if (fieldName.equals("data")) {
        Integer expected = num;
        Integer actual = (Integer) e.get(i);
        if (!expected.equals(actual)) {
          test.error("Mismatch for data: expected " + expected + " but got " + actual);
        }
      } else {
        test.error("Unexpected Column: " + fieldName);
      }
    }
  }

  private boolean setFields(AbstractClusterJModelTest test, DynamicObject e,
                                          int num) {
    for (int i = 0; i < e.columnMetadata().length; i++) {
      String fieldName = e.columnMetadata()[i].name();
      if (fieldName.equals("id")) {
        e.set(i, getTimestampKey(num));
      } else if (fieldName.equals("data")) {
        e.set(i, num);
      } else {
        test.error("Unexpected Column");
        return false;
      }
    }
    return true;
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
}
