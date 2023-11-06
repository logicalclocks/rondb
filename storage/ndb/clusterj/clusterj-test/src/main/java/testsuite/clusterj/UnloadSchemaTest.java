/*
   Copyright (c) 2022 Hopsworks and/or its affiliates.

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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class UnloadSchemaTest extends AbstractClusterJModelTest {
  private static final String TABLE = "fgtest";
  private static String DROP_TABLE_CMD = "drop table if exists " + TABLE;
  private static String CREATE_TABLE_CMD = "CREATE TABLE " + TABLE + " ( id int NOT NULL, col_1 " +
          "int DEFAULT NULL, col_2 varchar(1000) COLLATE utf8_unicode_ci DEFAULT NULL, PRIMARY " +
          "KEY (id)) ENGINE=ndbcluster";
  private static String ADD_COL_3_COPY =
          "alter table " + TABLE + " add column col_3 bigint NOT NULL DEFAULT '0', ALGORITHM=COPY";
  private static String ADD_COL_3_INPLACE =
          "alter table " + TABLE + " add column (col_3 bigint DEFAULT NULL), ALGORITHM=INPLACE";
  private static String ADD_COL_4_COPY =
          "alter table " + TABLE + " add column col_4 varchar(100) COLLATE utf8_unicode_ci NOT " +
                  "NULL DEFAULT 'abc_default', ALGORITHM=COPY";
  private static String ADD_COL_4_INPLACE =
          "alter table " + TABLE + " add column col_4 varchar(100) COLLATE utf8_unicode_ci, algorithm=INPLACE";
  private static String TRUNCATE_TABLE =
          "truncate table " + TABLE;

  private static String defaultDB = "test";
  private static final int NUM_THREADS = 10;
  private int SLEEP_TIME = 3000;

  private static boolean USE_COPY_ALGO = false;

  //unloadSchema can not be used with caching
  boolean useCache = true;

  @Override
  protected Properties modifyProperties() {
    if (useCache) {
      props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_INSTANCES, Integer.toString(NUM_THREADS));
      props.setProperty(Constants.PROPERTY_CLUSTER_WARMUP_CACHED_SESSIONS, Integer.toString(NUM_THREADS));
      props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, Integer.toString(NUM_THREADS));
    } else {
      props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_INSTANCES, "0");
      props.setProperty(Constants.PROPERTY_CLUSTER_WARMUP_CACHED_SESSIONS, "0");
      props.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, "0");
    }
    return props;
  }

  @Override
  public void localSetUp() {
    createSessionFactory();
    defaultDB = props.getProperty(Constants.PROPERTY_CLUSTER_DATABASE);
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

  public static class FGTest extends DynamicObject {
    @Override
    public String table() {
      return TABLE;
    }
  }

  public void runSQLCMD(AbstractClusterJModelTest test, String cmd) {
    PreparedStatement preparedStatement = null;

    try {
      preparedStatement = connection.prepareStatement(cmd);
      preparedStatement.executeUpdate();
      // System.out.println(cmd);
    } catch (SQLException e) {
      System.err.println("Failed to run SQL command. "+e);
      test.error("Failed to drop table. Error: " + e.getMessage());
      throw new RuntimeException("Failed to command: ", e);
    }
  }


  class DataInsertWorker extends Thread {
    private boolean run = true;
    private int startIndex = 0;

    private int maxRowsToWrite = 0;
    private int insertsCounter = 0;
    private int failCounter = 0;

    DataInsertWorker(int startIndex, int maxRowsToWrite) {
      this.startIndex = startIndex;
      this.maxRowsToWrite = maxRowsToWrite;
    }

    @Override
    public void run() {

      int currentIndex = startIndex;
      while (run) {
        Session session = getSession(defaultDB);
        DynamicObject e = null;
        boolean rowInserted = false;
        try {
          e = (DynamicObject) session.newInstance(FGTest.class);
          setFields(e, currentIndex++);
          session.savePersistent(e);
          closeDTO(session, e, FGTest.class);
          insertsCounter++;
          rowInserted = true;
          if (currentIndex > (startIndex + maxRowsToWrite)) {
            currentIndex = startIndex;
          }
        } catch (Exception ex) {
          //ex.printStackTrace();
          //System.out.println(ex.getMessage());
          failCounter++;
        } finally {
          if (!rowInserted) {
            session.unloadSchema(FGTest.class);
            session.close();
            try {
              Thread.sleep(5);
            } catch (InterruptedException ex) {
              throw new RuntimeException(ex);
            }
          } else {
            returnSession(session);
          }
        }
      }
    }

    public void stopDataInsertion() {
      run = false;
    }

    public int getInsertsCounter() {
      return insertsCounter;
    }

    public int getFailCounter() {
      return failCounter;
    }

    public void setFields(DynamicObject e, int num) {
      for (int i = 0; i < e.columnMetadata().length; i++) {
        String fieldName = e.columnMetadata()[i].name();
        if (fieldName.equals("id")) {
          e.set(i, num);
        } else if (fieldName.equals("col_1")) {
          e.set(i, num);
        } else if (fieldName.equals("col_2")) {
          e.set(i, Long.toString(num));
        } else if (fieldName.equals("col_3")) {
          long num_long = num;
          e.set(i, num_long);
        } else if (fieldName.equals("col_4")) {
          e.set(i, Long.toString(num));
        } else {
          throw new IllegalArgumentException("Unexpected Column");
        }
      }
    }
  }

  public void testUnloadSchemaUsingCache() {
    unloadSchema(true);
  }

  public void testUnloadSchemaNoCache() {
    unloadSchema(false);
  }
  public void unloadSchema(boolean useCache) {
    try {
      this.useCache = useCache;
      closeSession();
      closeAllExistingSessionFactories();
      sessionFactory = null;
      createSessionFactory();

      runSQLCMD(this, DROP_TABLE_CMD);
      runSQLCMD(this, CREATE_TABLE_CMD);

      List<DataInsertWorker> threads = new ArrayList<>(NUM_THREADS);
      for (int i = 0; i < NUM_THREADS; i++) {
        DataInsertWorker t = new DataInsertWorker(i * 1000000, 1000);
        threads.add(t);
        t.start();
      }

      Thread.sleep(SLEEP_TIME);

      if (USE_COPY_ALGO) {
        runSQLCMD(this, ADD_COL_3_COPY);
      } else {
        runSQLCMD(this, ADD_COL_3_INPLACE);
      }

      Thread.sleep(SLEEP_TIME);

      if (USE_COPY_ALGO) {
        runSQLCMD(this, ADD_COL_4_COPY);
      } else {
        runSQLCMD(this, ADD_COL_4_INPLACE);
      }

      Thread.sleep(SLEEP_TIME);

      for (int i = 0; i < NUM_THREADS; i++) {
        threads.get(i).stopDataInsertion();
      }

      int totalInsertions = 0;
      int totalFailures = 0;
      for (int i = 0; i < NUM_THREADS; i++) {
        threads.get(i).join();
        totalInsertions += threads.get(i).getInsertsCounter();
        totalFailures += threads.get(i).getFailCounter();
      }
      //System.out.println("PASS: Total Insertions " + totalInsertions+
      //    " Failed Inserts: "+ totalFailures);
    } catch (Exception e) {
      this.error("FAILED . Error: " + e.getMessage());
    }
  }
}
