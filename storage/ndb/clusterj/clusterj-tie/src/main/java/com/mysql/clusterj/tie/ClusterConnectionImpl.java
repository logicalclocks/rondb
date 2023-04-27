/*
 *  Copyright (c) 2010, 2023, Oracle and/or its affiliates.
 *  Copyright (c) 2020, 2023, Hopsworks and/or its affiliates.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, version 2.0,
 *  as published by the Free Software Foundation.
 *
 *  This program is also distributed with certain software (including
 *  but not limited to OpenSSL) that is licensed under separate terms,
 *  as designated in a particular file or component or in included license
 *  documentation.  The authors of MySQL hereby grant you an additional
 *  permission to link the program and your derivative works with the
 *  separately licensed software that they have included with MySQL.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License, version 2.0, for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.clusterj.tie;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Collections;

import com.mysql.ndbjtie.ndbapi.Ndb;
import com.mysql.ndbjtie.ndbapi.Ndb_cluster_connection;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.Dictionary;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJFatalInternalException;
import com.mysql.clusterj.ClusterJFatalUserException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.ClusterJUserException;

import com.mysql.clusterj.core.spi.ValueHandlerFactory;
import com.mysql.clusterj.core.store.Db;
import com.mysql.clusterj.core.store.Index;
import com.mysql.clusterj.core.store.Table;

import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;

/**
 *
 */
public class ClusterConnectionImpl
        implements com.mysql.clusterj.core.store.ClusterConnection {

    /** My message translator */
    static final I18NHelper local = I18NHelper.getInstance(ClusterConnectionImpl.class);

    /** My logger */
    static final Logger logger = LoggerFactoryService.getFactory()
            .getInstance(ClusterConnectionImpl.class);

    /** Ndb_cluster_connection is wrapped by ClusterConnection */
    protected Ndb_cluster_connection clusterConnection;

    /** The connection string for this connection */
    final String connectString;

    /** The node id requested for this connection; 0 for default */
    final int nodeId;

    /** The timeout value to connect to mgm */
    final int connectTimeoutMgm;

    /** All regular dbs (not defaultDbForNdbRecord) given out by this cluster connection */
    private Map<DbImpl, Object> dbs = Collections.synchronizedMap(new IdentityHashMap<DbImpl, Object>());

    /** The DbImplForNdbRecord for default database */
    DbImplForNdbRecord defaultDbForNdbRecord;

    /** DbImpleForNdbRecord for specific database */
    private ConcurrentMap<String, DbImplForNdbRecord> databaseForNdbRecord = new ConcurrentHashMap<String, DbImplForNdbRecord>();

    /** The map of table name to NdbRecordImpl */
    private ConcurrentMap<String, NdbRecordImpl> ndbRecordImplMap = new ConcurrentHashMap<String, NdbRecordImpl>();

    /** The sizes of the byte buffer pool. Set from SessionFactoryImpl after construction, before connect. */
    private int[] byteBufferPoolSizes;

    /** The byte buffer pool */
    protected VariableByteBufferPoolImpl byteBufferPool;

    /** A "big enough" size for error information */
    private int errorBufferSize = 300;

    /** The byte buffer pool for DbImpl error buffers */
    protected FixedByteBufferPoolImpl byteBufferPoolForDBImplError;

    /** The size of the coordinated transaction identifier buffer */
    private final static int COORDINATED_TRANSACTION_ID_SIZE = 44;

    /** The byte buffer pool for coordinated transaction id */
    protected FixedByteBufferPoolImpl byteBufferPoolForCoordinatedTransactionId;

    /** The size of the partition key scratch buffer */
    private final static int PARTITION_KEY_BUFFER_SIZE = 10000;

    /** The byte buffer pool for DbImpl error buffers */
    protected FixedByteBufferPoolImpl byteBufferPoolForPartitionKey;

    /** The dictionary used to create NdbRecords */
    Dictionary dictionaryForNdbRecord = null;

    private ConcurrentMap<String, Dictionary> dbDictionaryForNdbRecord = new ConcurrentHashMap<String, Dictionary>();

    private boolean isClosing = false;

    private long[] autoIncrement;

    private static final String USE_SMART_VALUE_HANDLER_NAME = "com.mysql.clusterj.UseSmartValueHandler";

    private static final boolean USE_SMART_VALUE_HANDLER =
            ClusterJHelper.getBooleanProperty(USE_SMART_VALUE_HANDLER_NAME, "true");

    protected static boolean queryObjectsInitialized = false;

    /** Connect to the MySQL Cluster
     * 
     * @param connectString the connect string
     * @param nodeId the node id; node id of zero means "any node"
     */
    public ClusterConnectionImpl(String connectString, int nodeId, int connectTimeoutMgm) {
        this.connectString = connectString;
        this.nodeId = nodeId;
        this.connectTimeoutMgm = connectTimeoutMgm;
        byteBufferPoolForDBImplError =
                new FixedByteBufferPoolImpl(errorBufferSize, "DBImplErrorBufferPool");
        byteBufferPoolForCoordinatedTransactionId =
                new FixedByteBufferPoolImpl(COORDINATED_TRANSACTION_ID_SIZE, "CoordinatedTransactionIdBufferPool");
        byteBufferPoolForPartitionKey =
                new FixedByteBufferPoolImpl(PARTITION_KEY_BUFFER_SIZE, "PartitionKeyBufferPool");
        clusterConnection = Ndb_cluster_connection.create(connectString, nodeId);
        handleError(clusterConnection, connectString, nodeId);
        int timeoutError = clusterConnection.set_timeout(connectTimeoutMgm);
        handleError(timeoutError, connectString, nodeId, connectTimeoutMgm);
        logger.info(local.message("INFO_Create_Cluster_Connection", connectString, nodeId, connectTimeoutMgm));
    }

    public void connect(int connectRetries, int connectDelay, boolean verbose) {
        byteBufferPool = new VariableByteBufferPoolImpl(byteBufferPoolSizes);
        checkConnection();
        int returnCode = clusterConnection.connect(connectRetries, connectDelay, verbose?1:0);
        handleError(returnCode, clusterConnection, connectString, nodeId);
    }

    public Db createDb(String databaseName, boolean defaultDatabase, int maxTransactions) {
        checkConnection();
        Ndb ndb = null;
        /**
         * We create one NdbDictionary for each database we use in this cluster connection
         * Using this NdbDictionary we will then create one NdbRecord for each
         * table in that database. These NdbRecord objects will be stored in a separate
         * map for each database. We handle the default database as a special case since
         * most applications have only one database per cluster connection.
         *
         * Synchronize because create is not guaranteed thread-safe
         */
        synchronized(this) {
            ndb = Ndb.create(clusterConnection, databaseName, "def");
            handleError(ndb, clusterConnection, connectString, nodeId);
            if (defaultDatabase) {
                if (dictionaryForNdbRecord == null) {
                    // create a dictionary for NdbRecord
                    Ndb ndbForNdbRecord = Ndb.create(clusterConnection, databaseName, "def");
                    handleError(ndbForNdbRecord, clusterConnection, connectString, nodeId);
                    defaultDbForNdbRecord = new DbImplForNdbRecord(this,
                                                            ndbForNdbRecord,
                                                            databaseName,
                                                            defaultDatabase);
                    // get an instance of stand-alone query objects to avoid synchronizing later
                    defaultDbForNdbRecord.initializeQueryObjects();
                    dictionaryForNdbRecord = defaultDbForNdbRecord.getNdbDictionary();
                }
            } else {
                Dictionary dbDictionary = dbDictionaryForNdbRecord.get(databaseName);
                if (dbDictionary == null) {
                    // create a dictionary for NdbRecord for this database
                    Ndb dbNdbForNdbRecord = Ndb.create(clusterConnection, databaseName, "def");
                    handleError(dbNdbForNdbRecord, clusterConnection, connectString, nodeId);
                    DbImplForNdbRecord dbForNdbRecord = new DbImplForNdbRecord(this,
                                                                               dbNdbForNdbRecord,
                                                                               databaseName,
                                                                               defaultDatabase);
                    dbForNdbRecord.initializeQueryObjects();
                    dbDictionary = dbForNdbRecord.getNdbDictionary();
                    dbDictionaryForNdbRecord.put(databaseName, dbDictionary);
                    databaseForNdbRecord.put(databaseName, dbForNdbRecord);
                }
            }
        }
        DbImpl result = new DbImpl(this,
                                   ndb,
                                   maxTransactions,
                                   databaseName,
                                   defaultDatabase);
        result.initializeAutoIncrement(autoIncrement);
        dbs.put(result, null);
        return result;
    }

    public void waitUntilReady(int connectTimeoutBefore, int connectTimeoutAfter) {
        checkConnection();
        int returnCode = clusterConnection.wait_until_ready(connectTimeoutBefore, connectTimeoutAfter);
        handleError(returnCode, clusterConnection, connectString, nodeId);
    }

    private void checkConnection() {
        if (clusterConnection == null) {
            throw new ClusterJFatalInternalException(local.message("ERR_Cluster_Connection_Must_Not_Be_Null"));
        }
    }

    protected static void handleError(int timeoutError, String connectString, int nodeId, int connectTimeoutMgm) {
        if (timeoutError != 0) {
            String message = local.message("ERR_Set_Timeout_Mgm", connectString, nodeId, connectTimeoutMgm, timeoutError);
            logger.error(message);
            throw new ClusterJDatastoreException(message);
        }
    }

    protected static void handleError(int returnCode, Ndb_cluster_connection clusterConnection,
            String connectString, int nodeId) {
        if (returnCode >= 0) {
            return;
        } else {
            try {
                throwError(returnCode, clusterConnection, connectString, nodeId);
            } finally {
                // all errors on Ndb_cluster_connection are fatal
                Ndb_cluster_connection.delete(clusterConnection);
            }
        }
    }

    protected static void handleError(Object object, Ndb_cluster_connection clusterConnection,
            String connectString, int nodeId) {
        if (object != null) {
            return;
        } else {
            throwError(null, clusterConnection, connectString, nodeId);
        }
    }

    protected static void handleError(Ndb_cluster_connection clusterConnection, String connectString, int nodeId) {
        if (clusterConnection == null) {
            String message = local.message("ERR_Connect", connectString, nodeId);
            logger.error(message);
            throw new ClusterJDatastoreException(message);
        }
    }

    protected static void throwError(Object returnCode, Ndb_cluster_connection clusterConnection,
            String connectString, int nodeId) {
        String message = clusterConnection.get_latest_error_msg();
        int errorCode = clusterConnection.get_latest_error();
        String msg = local.message("ERR_NdbError", returnCode, errorCode, message, connectString, nodeId);
        throw new ClusterJDatastoreException(msg);
    }

    public void closing() {
        this.isClosing = true;
        if (clusterConnection != null) {
            logger.info(local.message("INFO_Close_Cluster_Connection", connectString, nodeId));
            if (dbs.size() > 0) {
                for (DbImpl db: dbs.keySet()) {
                    // mark all dbs as closing so no more operations will start
                    db.closing();
                }
            }
            if (defaultDbForNdbRecord != null) {
                defaultDbForNdbRecord.closing();
            }
            Iterator<Map.Entry<String, DbImplForNdbRecord>> iterator =
                databaseForNdbRecord.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, DbImplForNdbRecord> entry = iterator.next();
                DbImplForNdbRecord dbForNdbRecord = entry.getValue();
                dbForNdbRecord.closing();
            }
        }
    }

    public void close() {
        if (clusterConnection != null) {
            if (!this.isClosing) {
                this.closing();
                // sleep for 1000 milliseconds to allow operations in other threads to terminate
                sleep(1000);
            }
            if (dbs.size() != 0) {
                Map<Db, Object> dbsToClose = new IdentityHashMap<Db, Object>(dbs);
                for (Db db: dbsToClose.keySet()) {
                    db.close();
                }
            }
            for (NdbRecordImpl ndbRecord: ndbRecordImplMap.values()) {
                ndbRecord.releaseNdbRecord();
            }
            if (defaultDbForNdbRecord != null) {
                defaultDbForNdbRecord.close();
                defaultDbForNdbRecord = null;
            }
            Iterator<Map.Entry<String, DbImplForNdbRecord>> iterator =
                databaseForNdbRecord.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, DbImplForNdbRecord> entry = iterator.next();
                DbImplForNdbRecord dbForNdbRecord = entry.getValue();
                dbForNdbRecord.close();
                iterator.remove();
            }
            databaseForNdbRecord.clear();
            ndbRecordImplMap.clear();
            synchronized (this) {
                Ndb_cluster_connection.delete(clusterConnection);
                clusterConnection = null;
            }
        }
    }

    public void release(NdbRecordImpl record){
        synchronized (this){
            if (clusterConnection != null){
                record.releaseNdbRecord();
            }
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close(Db db) {
        dbs.remove(db);
    }

    public int dbCount() {
        // defaultDbForNdbRecord is not included in the dbs list
        return dbs.size();
    }

    /** 
     * Get the cached NdbRecord implementation for the table
     * used with this cluster connection. All columns are included
     * in the NdbRecord.
     * Use a ConcurrentHashMap for best multithread performance.
     * There are three possibilities:
     * <ul><li>Case 1: return the already-cached NdbRecord
     * </li><li>Case 2: return a new instance created by this method
     * </li><li>Case 3: return the winner of a race with another thread
     * </li></ul>
     * @param db         The representation of the Ndb object, contains database name
     * @param storeTable the store table
     * @return the NdbRecordImpl for the table
     */
    protected NdbRecordImpl getCachedNdbRecordImpl(DbImpl db, Table storeTable) {
        defaultDbForNdbRecord.assertNotClosed("ClusterConnectionImpl.getCachedNdbRecordImpl for table");
        // tableKey is table name plus projection indicator
        String tableName = db.getName() + "+" + storeTable.getKey();
        // find the NdbRecordImpl in the global cache
        NdbRecordImpl result = ndbRecordImplMap.get(tableName);
        if (result != null) {
            // case 1: The quick case, there is a NdbRecord ready to use
            if (logger.isDebugEnabled())logger.debug("NdbRecordImpl found for " + tableName);
            return result;
        } else {
            // dictionary is single thread
            NdbRecordImpl newNdbRecordImpl;
            synchronized (this) {
                // try again; another thread might have beat us
                result = ndbRecordImplMap.get(tableName);
                if (result != null) {
                    return result;
                }
                Dictionary dictionary = dictionaryForNdbRecord;
                if (!db.isDefaultDatabase()) {
                    dictionary = dbDictionaryForNdbRecord.get(db.getName());
                }
                newNdbRecordImpl = new NdbRecordImpl(storeTable, dictionary, this);
                ndbRecordImplMap.put(tableName, newNdbRecordImpl);
            }
            return newNdbRecordImpl;
        }
    }

    /** 
     * Get the cached NdbRecord implementation for the index and table
     * used with this cluster connection.
     * The NdbRecordImpl is cached under the name tableName+indexName.
     * Only the key columns are included in the NdbRecord.
     * Use a ConcurrentHashMap for best multithread performance.
     * There are three possibilities:
     * <ul><li>Case 1: return the already-cached NdbRecord
     * </li><li>Case 2: return a new instance created by this method
     * </li><li>Case 3: return the winner of a race with another thread
     * </li></ul>
     * @param db         The representation of the Ndb object, contains database name
     * @param storeTable the store table
     * @param storeIndex the store index
     * @return the NdbRecordImpl for the index
     */
    protected NdbRecordImpl getCachedNdbRecordImpl(DbImpl db, Index storeIndex, Table storeTable) {
        defaultDbForNdbRecord.assertNotClosed("ClusterConnectionImpl.getCachedNdbRecordImpl for index");
        String recordName = db.getName() + "+" +
                            storeTable.getName() + "+" +
                            storeIndex.getInternalName();
        // find the NdbRecordImpl in the global cache
        NdbRecordImpl result = ndbRecordImplMap.get(recordName);
        if (result != null) {
            // case 1: The quick case, there is a NdbRecord ready to use
            if (logger.isDebugEnabled())logger.debug("NdbRecordImpl found for " + recordName);
            return result;
        } else {
            // dictionary is single thread
            NdbRecordImpl newNdbRecordImpl;
            synchronized (this) {
                // try again; another thread might have beat us
                result = ndbRecordImplMap.get(recordName);
                if (result != null) {
                    return result;
                }
                Dictionary dictionary = dictionaryForNdbRecord;
                if (!db.isDefaultDatabase()) {
                    dictionary = dbDictionaryForNdbRecord.get(db.getName());
                }
                newNdbRecordImpl = new NdbRecordImpl(storeIndex, storeTable, dictionary, this);
                ndbRecordImplMap.put(recordName, newNdbRecordImpl);
            }
            return newNdbRecordImpl;
        }
    }

    /** Remove the cached NdbRecord(s) associated with this table. This allows schema change to work.
     * All NdbRecords including any index NdbRecords will be removed. Index NdbRecords are named
     * tableName+indexName. Cached schema objects in NdbDictionary are also removed.
     * This method should be called by the application after receiving an exception that indicates
     * that the table definition has changed since the metadata was loaded. Such changes as
     * truncate table or dropping indexes, columns, or tables may cause errors reported
     * to the application, including code 241 "Invalid schema object version" and
     * code 284 "Unknown table error in transaction coordinator".
     * @param tableName the name of the table
     */
    public void unloadSchema(String databaseName, String tableName, boolean defaultDatabase) {
        // synchronize to avoid multiple threads unloading schema simultaneously
        // it is possible although unlikely that another thread is adding an entry while 
        // we are removing entries; if this occurs an error will be signaled here
        boolean haveCachedTable = false;
        synchronized(ndbRecordImplMap) {
            Dictionary dictionary = dictionaryForNdbRecord;
            if (!defaultDatabase) {
                dictionary = dbDictionaryForNdbRecord.get(databaseName);
                assert(dictionary != null);
            }
            Iterator<Map.Entry<String, NdbRecordImpl>> iterator =
                ndbRecordImplMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, NdbRecordImpl> entry = iterator.next();
                String key = entry.getKey();
                String searchName = databaseName + "+" + tableName;
                if (key.startsWith(searchName)) {
                    haveCachedTable = true;
                    // entries are of the form:
                    //   databaseName+tableName or
                    //   databaseName+tableName+indexName
                    // split tableName[+indexName] into one or two parts
                    // the "\" character is escaped once for Java and again for regular expression to escape +
                    String[] tablePlusIndex = key.split("\\+");
                    if (tablePlusIndex.length > 2) {
                        String indexName = tablePlusIndex[2];
                        if (logger.isDebugEnabled())logger.debug(
                            "Removing dictionary entry for cached index " +
                            "db:" + databaseName + " " + tableName + " " + indexName);
                        dictionary.invalidateIndex(indexName, tableName);
                    }
                    // remove all records whose key begins with the table name; this will remove index records also
                    if (logger.isDebugEnabled())logger.debug("Removing cached NdbRecord for " + key);
                    NdbRecordImpl record = entry.getValue();
                    iterator.remove();
                    //NdbRecordImpl is shared with multiple dynamic objects.
                    //Calling releaseNdbRecord will also release associated native NDB objects.
                    //Dynamic objects that hold reference to this record will encounter
                    //seg faults when they access this released NdbRecordImpl object
                    //Note: unloadSchema can not be used with dynamic object caching
                    //Although we clear the cache when the user calls unloadSchema, the user
                    //might have other active dynamic objects a reference to this
                    //bad NdbRecordImpl object, and after using these objects the
                    //user puts the dynamic object in the cache.

                    //This piece of code has been commented out as we want to delay
                    //releasing the NdbRecordImpl object as long as any dynamic object has
                    //a reference to it. If there are no references to this object then
                    //the GC will call the finalize method of this object which will call the
                    //releaseNdbRecord() method
                    //if (record != null) {
                    //    record.releaseNdbRecord();
                    //}
                }
            }
            // invalidate cached dictionary table after invalidate cached indexes
            if (haveCachedTable) {
                if (logger.isDebugEnabled())logger.debug(
                    "Removing dictionary entry for cached table " +
                    "db:" + databaseName + " " + tableName);
                dictionary.invalidateTable(tableName);
            }
        }
    }

    public ValueHandlerFactory getSmartValueHandlerFactory() {
        ValueHandlerFactory result = null;
        if (USE_SMART_VALUE_HANDLER) {
            result = new NdbRecordSmartValueHandlerFactoryImpl();
        }
        return result;
    }

    public NdbRecordOperationImpl newNdbRecordOperationImpl(DbImpl db, Table storeTable) {
        return new NdbRecordOperationImpl(this, db, storeTable);
    }

    public void initializeAutoIncrement(long[] autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public VariableByteBufferPoolImpl getByteBufferPool() {
        return byteBufferPool;
    }

    public void setByteBufferPoolSizes(int[] poolSizes) {
        this.byteBufferPoolSizes = poolSizes;
    }

    public void setRecvThreadCPUid(short cpuid) {
        int ret = 0;
        if (cpuid < 0) {
            // Invalid cpu id
            throw new ClusterJUserException(
                    local.message("ERR_Invalid_CPU_Id", cpuid));
        }
        ret = clusterConnection.set_recv_thread_cpu(cpuid);
        if (ret != 0) {
            // Error in binding cpu
            switch (ret) {
            case 22:    /* EINVAL - Invalid CPU id error in Linux/FreeBSD */
            case 31994: /* CPU_ID_MISSING_ERROR - Invalid CPU id error in Windows */
                throw new ClusterJUserException(
                        local.message("ERR_Invalid_CPU_Id", cpuid));
            case 31999: /* BIND_CPU_NOT_SUPPORTED_ERROR */
                throw new ClusterJFatalUserException(
                        local.message("ERR_Bind_CPU_Not_Supported"));
            default:
                // Unknown error code. Print it to user.
                throw new ClusterJFatalInternalException(
                        local.message("ERR_Binding_Recv_Thread_To_CPU", cpuid, ret));
            }
        }
    }

    public void unsetRecvThreadCPUid() {
        int ret = clusterConnection.unset_recv_thread_cpu();
        if (ret == 31999) {
            // BIND_CPU_NOT_SUPPORTED_ERROR
            throw new ClusterJFatalUserException(
                    local.message("ERR_Bind_CPU_Not_Supported"));
        } else if (ret != 0) {
            throw new ClusterJFatalInternalException(
                    local.message("ERR_Unbinding_Recv_Thread_From_CPU", ret));
        }
    }

    public void setRecvThreadActivationThreshold(int threshold) {
        if (clusterConnection.set_recv_thread_activation_threshold(threshold) == -1) {
            throw new ClusterJFatalInternalException(
                    local.message("ERR_Setting_Activation_Threshold"));
        }
    }
}
