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
import com.mysql.clusterj.DynamicObject;
import com.mysql.clusterj.Session;
import testsuite.clusterj.model.Employee;
import testsuite.clusterj.model.Employee2;
import testsuite.clusterj.model.Employee3;


/*
One class for different tables with static table name
 */
public class MultiDBUpdate4Test extends AbstractClusterJModelTest {

  private static final int NUMBER_TO_INSERT = 1024;
  private static String defaultDB;

  boolean useCache = false;

  //NOTE: Not sure if this is a good idea to use DynamicObject class like this
  public static class EmpBasic extends DynamicObject {
    private static String tabName;

    @Override
    public String table() {
      return tabName;
    }

    public static void setTabName(String name) {
      tabName = name;
    }
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

  public void testSimple() {
    useCache = false;
    cleanUp();
    EmpBasic.setTabName("t_basic");
    runTest(defaultDB);

    EmpBasic.setTabName("t_basic2");
    runTest("test2");

    EmpBasic.setTabName("t_basic3");
    runTest("test3");
  }

  public void testSimpleWithCache() {
    useCache = true;
    cleanUp();
    EmpBasic.setTabName("t_basic");
    runTest(defaultDB);

    EmpBasic.setTabName("t_basic2");
    runTest("test2");

    EmpBasic.setTabName("t_basic3");
    runTest("test3");
  }

  public void runTest(String db) {
    System.out.println("Adding rows to DB: " + db + " table: " + EmpBasic.tabName);
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(db);
      EmpBasic e = s.newInstance(EmpBasic.class);
      MultiDBHelper.verifyEmployeeFields(this, e, i);
      s.savePersistent(e);
      closeDTO(s, e, EmpBasic.class);
      returnSession(s);
    }

    // now verify data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(db);
      EmpBasic e = s.find(EmpBasic.class, i);
      MultiDBHelper.verifyEmployeeFields(this, e, i);
      closeDTO(s, e, EmpBasic.class);
      returnSession(s);
    }

    // now delete data
    for (int i = 0; i < NUMBER_TO_INSERT; i++) {
      Session s = getSession(db);
      EmpBasic e = s.find(EmpBasic.class, i);
      s.deletePersistent(e);
      closeDTO(s, e, EmpBasic.class);
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
}
