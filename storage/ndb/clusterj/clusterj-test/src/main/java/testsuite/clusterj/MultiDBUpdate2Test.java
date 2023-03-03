/*
   Copyright (c) 2010, 2022, Oracle and/or its affiliates.
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
import com.mysql.clusterj.Session;
import testsuite.clusterj.model.Employee;
import testsuite.clusterj.model.Employee2;
import testsuite.clusterj.model.Employee3;

import java.util.Properties;

/*
Using table models
 */
public class MultiDBUpdate2Test extends AbstractClusterJModelTest {

  private static final int NUMBER_TO_INSERT = 1024;
  private static String defaultDB;
  boolean useCache = false;

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
    cleanUpInt(defaultDB, Employee.class);
    cleanUpInt("test2", Employee2.class);
    cleanUpInt("test3", Employee3.class);
  }

  public void cleanUpInt(String db, Class c) {
    Session s = getSession(db);
    s.deletePersistentAll(c);
    returnSession(s);
  }

  public void testSimpleWithoutCache() {
    useCache = false;
    cleanUp();
    runTest1();
    runTest2();
    runTest3();
  }

  public void testSimpleWithCache() {
    useCache = true;
    cleanUp();
    runTest1();
    runTest2();
    runTest3();
  }

  public void runTest1() {
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(null);
      Employee e = s.newInstance(Employee.class);
      e.setId(i);
      e.setAge(i);
      e.setMagic(i);
      e.setName(Integer.toString(i));
      s.savePersistent(e);
      closeDTO(s, e, Employee.class);
      returnSession(s);
    }

    // now verify data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(null);
      Employee e = s.find(Employee.class, i);
      if (e.getId() != i) {
        error("Failed update: for employee " + i
          + " expected age " + i
          + " actual age " + e.getId());
      }
      closeDTO(s, e, Employee.class);
      returnSession(s);
    }

    // now delete the data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(null);
      Employee e = s.find(Employee.class, i);
      s.deletePersistent(e);
      closeDTO(s, e, Employee.class);
      returnSession(s);
    }
    failOnError();
  }

  public void runTest2() {
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession("test2");
      Employee2 e = s.newInstance(Employee2.class);
      e.setId(i);
      e.setAge(i);
      e.setMagic(i);
      e.setName(Integer.toString(i));
      s.savePersistent(e);
      closeDTO(s, e, Employee2.class);
      returnSession(s);
    }

    // now verify data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession("test2");
      Employee2 e = s.find(Employee2.class, i);
      if (e.getId() != i) {
        error("Failed update: for employee " + i
          + " expected age " + i
          + " actual age " + e.getId());
      }
      closeDTO(s, e, Employee2.class);
      returnSession(s);
    }

    // now delete the data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession("test2");
      Employee2 e = s.find(Employee2.class, i);
      s.deletePersistent(e);
      closeDTO(s, e, Employee2.class);
      returnSession(s);
    }
    failOnError();
  }

  public void runTest3() {
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession("test3");
      Employee3 e = s.newInstance(Employee3.class);
      e.setId(i);
      e.setAge(i);
      e.setMagic(i);
      e.setName(Integer.toString(i));
      s.savePersistent(e);
      closeDTO(s, e, Employee3.class);
      returnSession(s);
    }

    // now verify data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession("test3");
      Employee3 e = s.find(Employee3.class, i);
      if (e.getId() != i) {
        error("Failed update: for employee " + i
          + " expected age " + i
          + " actual age " + e.getId());
      }
      closeDTO(s, e, Employee3.class);
      returnSession(s);
    }

    // now delete the data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession("test3");
      Employee3 e = s.find(Employee3.class, i);
      s.deletePersistent(e);
      closeDTO(s, e, Employee3.class);
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

  void closeDTO(Session s, Object dto, Class dtoClass) {
    if (useCache) {
      s.releaseCache(dto, dtoClass);
    } else {
      s.release(dto);
    }
  }
}
