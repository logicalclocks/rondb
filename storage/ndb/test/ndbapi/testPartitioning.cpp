/*
   Copyright (c) 2004, 2026, Oracle and/or its affiliates.

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

#include <NDBT_ReturnCodes.h>
#include <HugoTransactions.hpp>
#include <NDBT_Test.hpp>
#include <NdbRestarter.hpp>
#include <UtilTransactions.hpp>

static Uint32 max_dks = 0;
static const Uint32 MAX_FRAGS =
    48 * 8 * 4;  // e.g. 48 nodes, 8 frags/node, 4 replicas
static Uint32 frag_ng_mappings[MAX_FRAGS];
static const char *DistTabName = "DistTest";
static const char *DistTabDKeyCol = "DKey";
static const char *DistTabPKey2Col = "PKey2";
static const char *DistTabResultCol = "Result";
static const char *DistIdxName = "ResultIndex";

static int run_drop_table(NDBT_Context *ctx, NDBT_Step *step) {
  NdbDictionary::Dictionary *dict = GETNDB(step)->getDictionary();
  dict->dropTable(ctx->getTab()->getName());
  return 0;
}

static int setNativePartitioning(Ndb *ndb, NdbDictionary::Table &tab, int when,
                                 void *arg) {
  switch (when) {
    case 0:  // Before
      break;
    case 1:  // After
      return 0;
    default:
      return 0;
  }

  /* Use rand to choose one of the native partitioning schemes */
  const Uint32 rType = rand() % 3;
  Uint32 fragType = -1;
  switch (rType) {
    case 0:
      fragType = NdbDictionary::Object::DistrKeyHash;
      break;
    case 1:
      fragType = NdbDictionary::Object::DistrKeyLin;
      break;
    case 2:
      fragType = NdbDictionary::Object::HashMapPartition;
      break;
  }

  ndbout << "Setting fragment type to " << fragType << endl;
  tab.setFragmentType((NdbDictionary::Object::FragmentType)fragType);
  return 0;
}

static int add_distribution_key(Ndb *ndb, NdbDictionary::Table &tab, int when,
                                void *arg) {
  switch (when) {
    case 0:  // Before
      break;
    case 1:  // After
      return 0;
    default:
      return 0;
  }

  /* Choose a partitioning type */
  setNativePartitioning(ndb, tab, when, arg);

  int keys = tab.getNoOfPrimaryKeys();
  Uint32 dks = (2 * keys + 2) / 3;
  dks = (dks > max_dks ? max_dks : dks);

  for (int i = 0; i < tab.getNoOfColumns(); i++)
    if (tab.getColumn(i)->getPrimaryKey() &&
        tab.getColumn(i)->getCharset() != 0)
      keys--;

  Uint32 max = NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY - tab.getNoOfPrimaryKeys();

  if (max_dks < max) max = max_dks;

  if (keys <= 1 && max > 0) {
    dks = 1 + (rand() % max);
    ndbout_c("%s pks: %d dks: %d", tab.getName(), keys, dks);
    while (dks--) {
      NdbDictionary::Column col;
      BaseString name;
      name.assfmt("PK_DK_%d", dks);
      col.setName(name.c_str());
      if ((rand() % 100) > 50) {
        col.setType(NdbDictionary::Column::Unsigned);
        col.setLength(1);
      } else {
        col.setType(NdbDictionary::Column::Varbinary);
        col.setLength(1 + (rand() % 25));
      }
      col.setNullable(false);
      col.setPrimaryKey(true);
      col.setDistributionKey(true);
      tab.addColumn(col);
    }
  } else {
    for (int i = 0; i < tab.getNoOfColumns(); i++) {
      NdbDictionary::Column *col = tab.getColumn(i);
      if (col->getPrimaryKey() && col->getCharset() == 0) {
        if ((int)dks >= keys || (rand() % 100) > 50) {
          col->setDistributionKey(true);
          dks--;
        }
        keys--;
      }
    }
  }

  ndbout << (NDBT_Table &)tab << endl;

  return 0;
}

static int setupUDPartitioning(Ndb *ndb, NdbDictionary::Table &tab) {
  NdbRestarter restarter;
  Vector<int> node_groups;
  int max_alive_replicas;
  if (restarter.getNodeGroups(node_groups, &max_alive_replicas) == -1) {
    return -1;
  }

  const Uint32 numNgs = node_groups.size();

  // Assume at least one node group had all replicas alive.
  const Uint32 numReplicas = max_alive_replicas;

  /**
   * The maximum number of partitions that may be defined explicitly
   * for any NDB table is =
   * 8 * [number of LDM threads] * [number of node groups]
   * In this case, we consider the number of LDM threads to be 1
   * (min. no of LDMs). This calculated number of partitions works for
   * higher number of LDMs as well.
   */
  const Uint32 numFragsPerNode = (rand() % (8 / numReplicas)) + 1;
  const Uint32 numPartitions = numReplicas * numNgs * numFragsPerNode;

  tab.setFragmentType(NdbDictionary::Table::UserDefined);
  tab.setFragmentCount(numPartitions);
  tab.setPartitionBalance(NdbDictionary::Object::PartitionBalance_Specific);
  for (Uint32 i = 0; i < numPartitions; i++) {
    frag_ng_mappings[i] = node_groups[i % numNgs];
  }
  tab.setFragmentData(frag_ng_mappings, numPartitions);

  return 0;
}

static int setUserDefPartitioning(Ndb *ndb, NdbDictionary::Table &tab, int when,
                                  void *arg) {
  switch (when) {
    case 0:  // Before
      break;
    case 1:  // After
      return 0;
    default:
      return 0;
  }

  setupUDPartitioning(ndb, tab);

  ndbout << (NDBT_Table &)tab << endl;

  return 0;
}

static int one_distribution_key(Ndb *ndb, NdbDictionary::Table &tab, int when,
                                void *arg) {
  switch (when) {
    case 0:  // Before
      break;
    case 1:  // After
      return 0;
    default:
      return 0;
  }

  setNativePartitioning(ndb, tab, when, arg);

  int keys = tab.getNoOfPrimaryKeys();
  int dist_key_no = rand() % keys;

  for (int i = 0; i < tab.getNoOfColumns(); i++) {
    if (tab.getColumn(i)->getPrimaryKey()) {
      if (dist_key_no-- == 0) {
        tab.getColumn(i)->setDistributionKey(true);
      } else {
        tab.getColumn(i)->setDistributionKey(false);
      }
    }
  }
  ndbout << (NDBT_Table &)tab << endl;

  return 0;
}

static const NdbDictionary::Table *create_dist_table(Ndb *pNdb,
                                                     bool userDefined) {
  NdbDictionary::Dictionary *dict = pNdb->getDictionary();

  do {
    NdbDictionary::Table tab;
    tab.setName(DistTabName);

    if (userDefined) {
      setupUDPartitioning(pNdb, tab);
    } else {
      setNativePartitioning(pNdb, tab, 0, 0);
    }

    NdbDictionary::Column dk;
    dk.setName(DistTabDKeyCol);
    dk.setType(NdbDictionary::Column::Unsigned);
    dk.setLength(1);
    dk.setNullable(false);
    dk.setPrimaryKey(true);
    dk.setPartitionKey(true);
    tab.addColumn(dk);

    NdbDictionary::Column pk2;
    pk2.setName(DistTabPKey2Col);
    pk2.setType(NdbDictionary::Column::Unsigned);
    pk2.setLength(1);
    pk2.setNullable(false);
    pk2.setPrimaryKey(true);
    pk2.setPartitionKey(false);
    tab.addColumn(pk2);

    NdbDictionary::Column result;
    result.setName(DistTabResultCol);
    result.setType(NdbDictionary::Column::Unsigned);
    result.setLength(1);
    result.setNullable(true);
    result.setPrimaryKey(false);
    tab.addColumn(result);

    dict->dropTable(tab.getName());
    if (dict->createTable(tab) == 0) {
      ndbout << (NDBT_Table &)tab << endl;

      do {
        /* Primary key index */
        NdbDictionary::Index idx;
        idx.setType(NdbDictionary::Index::OrderedIndex);
        idx.setLogging(false);
        idx.setTable(DistTabName);
        idx.setName("PRIMARY");
        idx.addColumnName(DistTabDKeyCol);
        idx.addColumnName(DistTabPKey2Col);

        dict->dropIndex("PRIMARY", tab.getName());

        if (dict->createIndex(idx) == 0) {
          ndbout << "Primary Index created successfully" << endl;
          break;
        }
        ndbout << "Primary Index create failed with "
               << dict->getNdbError().code << " retrying " << endl;
      } while (0);

      do {
        /* Now the index on the result column */
        NdbDictionary::Index idx;
        idx.setType(NdbDictionary::Index::OrderedIndex);
        idx.setLogging(false);
        idx.setTable(DistTabName);
        idx.setName(DistIdxName);
        idx.addColumnName(DistTabResultCol);

        dict->dropIndex(idx.getName(), tab.getName());

        if (dict->createIndex(idx) == 0) {
          ndbout << "Index on Result created successfully" << endl;
          return dict->getTable(tab.getName());
        }
        ndbout << "Index create failed with " << dict->getNdbError().code
               << endl;
      } while (0);
    }
  } while (0);
  return 0;
}

static int run_create_table(NDBT_Context *ctx, NDBT_Step *step) {
  /* Create table, optionally with extra distribution keys
   * or UserDefined partitioning
   */
  max_dks = ctx->getProperty("distributionkey", (unsigned)0);
  bool userDefined = ctx->getProperty("UserDefined", (unsigned)0);

  if (NDBT_Tables::createTable(
          GETNDB(step), ctx->getTab()->getName(), false, false,
          max_dks       ? add_distribution_key
          : userDefined ? setUserDefPartitioning
                        : setNativePartitioning) == NDBT_OK) {
    return NDBT_OK;
  }

  if (GETNDB(step)->getDictionary()->getNdbError().code == 745) return NDBT_OK;

  return NDBT_FAILED;
}

static int run_create_table_smart_scan(NDBT_Context *ctx, NDBT_Step *step) {
  if (NDBT_Tables::createTable(GETNDB(step), ctx->getTab()->getName(), false,
                               false, one_distribution_key) == NDBT_OK) {
    return NDBT_OK;
  }

  if (GETNDB(step)->getDictionary()->getNdbError().code == 745) return NDBT_OK;

  return NDBT_FAILED;
}

static int run_create_pk_index(NDBT_Context *ctx, NDBT_Step *step) {
  bool orderedIndex = ctx->getProperty("OrderedIndex", (unsigned)0);

  Ndb *pNdb = GETNDB(step);
  const NdbDictionary::Table *pTab =
      pNdb->getDictionary()->getTable(ctx->getTab()->getName());

  if (!pTab) return NDBT_OK;

  bool logged = ctx->getProperty("LoggedIndexes", orderedIndex ? 0 : 1);

  BaseString name;
  name.assfmt("IND_%s_PK_%c", pTab->getName(), orderedIndex ? 'O' : 'U');

  // Create index
  if (orderedIndex)
    ndbout << "Creating " << ((logged) ? "logged " : "temporary ")
           << "ordered index " << name.c_str() << " (";
  else
    ndbout << "Creating " << ((logged) ? "logged " : "temporary ")
           << "unique index " << name.c_str() << " (";

  NdbDictionary::Index pIdx(name.c_str());
  pIdx.setTable(pTab->getName());
  if (orderedIndex)
    pIdx.setType(NdbDictionary::Index::OrderedIndex);
  else
    pIdx.setType(NdbDictionary::Index::UniqueHashIndex);
  for (int c = 0; c < pTab->getNoOfColumns(); c++) {
    const NdbDictionary::Column *col = pTab->getColumn(c);
    if (col->getPrimaryKey()) {
      pIdx.addIndexColumn(col->getName());
      ndbout << col->getName() << " ";
    }
  }

  pIdx.setStoredIndex(logged);
  ndbout << ") ";
  if (pNdb->getDictionary()->createIndex(pIdx) != 0) {
    ndbout << "FAILED!" << endl;
    const NdbError err = pNdb->getDictionary()->getNdbError();
    NDB_ERR(err);
    return NDBT_FAILED;
  }

  ndbout << "OK!" << endl;
  return NDBT_OK;
}

static int run_create_pk_index_drop(NDBT_Context *ctx, NDBT_Step *step) {
  bool orderedIndex = ctx->getProperty("OrderedIndex", (unsigned)0);

  Ndb *pNdb = GETNDB(step);
  const NdbDictionary::Table *pTab =
      pNdb->getDictionary()->getTable(ctx->getTab()->getName());

  if (!pTab) return NDBT_OK;

  BaseString name;
  name.assfmt("IND_%s_PK_%c", pTab->getName(), orderedIndex ? 'O' : 'U');

  ndbout << "Dropping index " << name.c_str() << " ";
  if (pNdb->getDictionary()->dropIndex(name.c_str(), pTab->getName()) != 0) {
    ndbout << "FAILED!" << endl;
    NDB_ERR(pNdb->getDictionary()->getNdbError());
    return NDBT_FAILED;
  } else {
    ndbout << "OK!" << endl;
  }

  return NDBT_OK;
}

static int run_create_dist_table(NDBT_Context *ctx, NDBT_Step *step) {
  bool userDefined = ctx->getProperty("UserDefined", (unsigned)0);
  if (create_dist_table(GETNDB(step), userDefined)) return NDBT_OK;

  return NDBT_FAILED;
}

static int run_drop_dist_table(NDBT_Context *ctx, NDBT_Step *step) {
  GETNDB(step)->getDictionary()->dropTable(DistTabName);
  return NDBT_OK;
}

static int run_tests(Ndb *p_ndb, HugoTransactions &hugoTrans, int records,
                     Uint32 batchSize = 1) {
  if (hugoTrans.loadTable(p_ndb, records, batchSize) != 0) {
    return NDBT_FAILED;
  }

  if (hugoTrans.pkReadRecords(p_ndb, records, batchSize) != 0) {
    return NDBT_FAILED;
  }

  if (hugoTrans.pkUpdateRecords(p_ndb, records, batchSize) != 0) {
    return NDBT_FAILED;
  }

  if (hugoTrans.pkDelRecords(p_ndb, records, batchSize) != 0) {
    return NDBT_FAILED;
  }

  if (hugoTrans.loadTable(p_ndb, records, batchSize) != 0) {
    return NDBT_FAILED;
  }

  if (hugoTrans.scanUpdateRecords(p_ndb, records) != 0) {
    return NDBT_FAILED;
  }

  Uint32 abort = 23;
  for (Uint32 j = 0; j < 5; j++) {
    Uint32 parallelism = (j == 1 ? 1 : j * 3);
    ndbout_c("parallelism: %d", parallelism);
    if (hugoTrans.scanReadRecords(p_ndb, records, abort, parallelism,
                                  NdbOperation::LM_Read) != 0) {
      return NDBT_FAILED;
    }
    if (hugoTrans.scanReadRecords(p_ndb, records, abort, parallelism,
                                  NdbOperation::LM_Exclusive) != 0) {
      return NDBT_FAILED;
    }
    if (hugoTrans.scanReadRecords(p_ndb, records, abort, parallelism,
                                  NdbOperation::LM_CommittedRead) != 0) {
      return NDBT_FAILED;
    }
  }

  if (hugoTrans.clearTable(p_ndb, records) != 0) {
    return NDBT_FAILED;
  }

  return 0;
}

static int run_pk_dk(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *p_ndb = GETNDB(step);
  int records = ctx->getNumRecords();
  const NdbDictionary::Table *tab =
      p_ndb->getDictionary()->getTable(ctx->getTab()->getName());

  if (!tab) return NDBT_OK;

  HugoTransactions hugoTrans(*tab);

  Uint32 batchSize = ctx->getProperty("BatchSize", (unsigned)1);

  return run_tests(p_ndb, hugoTrans, records, batchSize);
}

int run_index_dk(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *p_ndb = GETNDB(step);
  int records = ctx->getNumRecords();
  const NdbDictionary::Table *pTab =
      p_ndb->getDictionary()->getTable(ctx->getTab()->getName());

  if (!pTab) return NDBT_OK;

  bool orderedIndex = ctx->getProperty("OrderedIndex", (unsigned)0);

  BaseString name;
  name.assfmt("IND_%s_PK_%c", pTab->getName(), orderedIndex ? 'O' : 'U');

  const NdbDictionary::Index *idx =
      p_ndb->getDictionary()->getIndex(name.c_str(), pTab->getName());

  if (!idx) {
    ndbout << "Failed to retreive index: " << name.c_str() << endl;
    return NDBT_FAILED;
  }
  Uint32 batchSize = ctx->getProperty("BatchSize", (unsigned)1);

  HugoTransactions hugoTrans(*pTab, idx);

  return run_tests(p_ndb, hugoTrans, records, batchSize);
}

static int run_startHint(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *p_ndb = GETNDB(step);
  int records = ctx->getNumRecords();
  const NdbDictionary::Table *tab =
      p_ndb->getDictionary()->getTable(ctx->getTab()->getName());

  if (!tab) return NDBT_OK;

  HugoTransactions hugoTrans(*tab);
  if (hugoTrans.loadTable(p_ndb, records) != 0) {
    return NDBT_FAILED;
  }

  NdbRestarter restarter;
  if (restarter.insertErrorInAllNodes(8050) != 0) return NDBT_FAILED;

  HugoCalculator dummy(*tab);
  int result = NDBT_OK;
  for (int i = 0; i < records && result == NDBT_OK; i++) {
    char buffer[NDB_MAX_TUPLE_SIZE];
    char *start = buffer + (rand() & 7);
    char *pos = start;

    int k = 0;
    Ndb::Key_part_ptr ptrs[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY + 1];
    for (int j = 0; j < tab->getNoOfColumns(); j++) {
      if (tab->getColumn(j)->getPartitionKey()) {
        // ndbout_c(tab->getColumn(j)->getName());
        int sz = tab->getColumn(j)->getSizeInBytes();
        Uint32 real_size;
        dummy.calcValue(i, j, 0, pos, sz, &real_size);
        ptrs[k].ptr = pos;
        ptrs[k++].len = real_size;
        pos += (real_size + 3) & ~3;
      }
    }
    ptrs[k].ptr = 0;

    // Now we have the pk
    NdbTransaction *pTrans = p_ndb->startTransaction(tab, ptrs);
    HugoOperations ops(*tab);
    ops.setTransaction(pTrans);
    if (ops.pkReadRecord(p_ndb, i, 1) != NDBT_OK) {
      result = NDBT_FAILED;
      break;
    }

    if (ops.execute_Commit(p_ndb) != 0) {
      result = NDBT_FAILED;
      break;
    }

    ops.closeTransaction(p_ndb);
  }
  restarter.insertErrorInAllNodes(0);
  return result;
}

static int run_startHint_ordered_index(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *p_ndb = GETNDB(step);
  int records = ctx->getNumRecords();
  const NdbDictionary::Table *tab =
      p_ndb->getDictionary()->getTable(ctx->getTab()->getName());

  if (!tab) return NDBT_OK;

  BaseString name;
  name.assfmt("IND_%s_PK_O", tab->getName());

  const NdbDictionary::Index *idx =
      p_ndb->getDictionary()->getIndex(name.c_str(), tab->getName());

  if (!idx) {
    ndbout << "Failed to retreive index: " << name.c_str() << endl;
    return NDBT_FAILED;
  }

  HugoTransactions hugoTrans(*tab, idx);
  if (hugoTrans.loadTable(p_ndb, records) != 0) {
    return NDBT_FAILED;
  }

  const Uint32 errorInsert = ctx->getProperty("errorinsertion", (unsigned)8050);

  NdbRestarter restarter;
  if (restarter.insertErrorInAllNodes(errorInsert) != 0) return NDBT_FAILED;

  HugoCalculator dummy(*tab);
  int result = NDBT_OK;
  for (int i = 0; i < records && result == NDBT_OK; i++) {
    char buffer[NDB_MAX_TUPLE_SIZE];
    NdbTransaction *pTrans = NULL;

    char *start = buffer + (rand() & 7);
    char *pos = start;

    int k = 0;
    Ndb::Key_part_ptr ptrs[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY + 1];
    for (int j = 0; j < tab->getNoOfColumns(); j++) {
      if (tab->getColumn(j)->getPartitionKey()) {
        // ndbout_c(tab->getColumn(j)->getName());
        int sz = tab->getColumn(j)->getSizeInBytes();
        Uint32 real_size;
        dummy.calcValue(i, j, 0, pos, sz, &real_size);
        ptrs[k].ptr = pos;
        ptrs[k++].len = real_size;
        pos += (real_size + 3) & ~3;
      }
    }
    ptrs[k].ptr = 0;

    // Now we have the pk, start a hinted transaction
    pTrans = p_ndb->startTransaction(tab, ptrs);

    // Because we pass an Ordered index here, pkReadRecord will
    // use an index scan on the Ordered index
    HugoOperations ops(*tab, idx);
    ops.setTransaction(pTrans);
    /* Despite it's name, it will actually perform index scans
     * as there is an index.
     * Error 8050 will cause an NDBD assertion failure in
     * Dbtc::execDIGETPRIMCONF() if TC needs to scan a fragment
     * which is not on the TC node
     * So for this TC to pass with no failures we need transaction
     * hinting and scan partition pruning on equal() to work
     * correctly.
     * TODO : Get coverage of Index scan which is equal on dist
     * key cols, but has an inequality on some other column.
     */
    if (ops.pkReadRecord(p_ndb, i, 1) != NDBT_OK) {
      result = NDBT_FAILED;
      break;
    }

    if (ops.execute_Commit(p_ndb) != 0) {
      result = NDBT_FAILED;
      break;
    }

    ops.closeTransaction(p_ndb);
  }
  restarter.insertErrorInAllNodes(0);
  return result;
}

#define CHECK(x, y)                                             \
  {                                                             \
    int res = (x);                                              \
    if (res != 0) {                                             \
      ndbout << "Assert failed at " << __LINE__ << endl         \
             << res << endl                                     \
             << " error : " << (y)->getNdbError().code << endl; \
      return NDBT_FAILED;                                       \
    }                                                           \
  }

#define CHECKNOTNULL(x, y)                                   \
  {                                                          \
    if ((x) == NULL) {                                       \
      ndbout << "Assert failed at line " << __LINE__ << endl \
             << " with " << (y)->getNdbError().code << endl; \
      return NDBT_FAILED;                                    \
    }                                                        \
  }

static int load_dist_table(Ndb *pNdb, int records, int parts) {
  const NdbDictionary::Table *tab =
      pNdb->getDictionary()->getTable(DistTabName);
  bool userDefined =
      (tab->getFragmentType() == NdbDictionary::Object::UserDefined);

  const NdbRecord *distRecord = tab->getDefaultRecord();
  CHECKNOTNULL(distRecord, pNdb);

  char *buf = (char *)malloc(NdbDictionary::getRecordRowLength(distRecord));

  CHECKNOTNULL(buf, pNdb);

  /* We insert a number of records with a constrained number of
   * values for the distribution key column
   */
  for (int r = 0; r < records; r++) {
    NdbTransaction *trans = pNdb->startTransaction();
    CHECKNOTNULL(trans, pNdb);

    {
      const int dKeyVal = r % parts;
      const Uint32 dKeyAttrid = tab->getColumn(DistTabDKeyCol)->getAttrId();
      memcpy(NdbDictionary::getValuePtr(distRecord, buf, dKeyAttrid), &dKeyVal,
             sizeof(dKeyVal));
    }

    {
      const int pKey2Val = r;
      const Uint32 pKey2Attrid = tab->getColumn(DistTabPKey2Col)->getAttrId();
      memcpy(NdbDictionary::getValuePtr(distRecord, buf, pKey2Attrid),
             &pKey2Val, sizeof(pKey2Val));
    }

    {
      const int resultVal = r * r;
      const Uint32 resultValAttrid =
          tab->getColumn(DistTabResultCol)->getAttrId();
      memcpy(NdbDictionary::getValuePtr(distRecord, buf, resultValAttrid),
             &resultVal, sizeof(resultVal));

      // set not NULL
      NdbDictionary::setNull(distRecord, buf, resultValAttrid, false);
    }

    NdbOperation::OperationOptions opts;
    opts.optionsPresent = 0;

    if (userDefined) {
      /* For user-defined partitioning, we set the partition id
       * to be the distribution key value modulo the number
       * of partitions in the table
       */
      opts.optionsPresent = NdbOperation::OperationOptions::OO_PARTITION_ID;
      opts.partitionId = (r % parts) % tab->getFragmentCount();
    }

    CHECKNOTNULL(trans->insertTuple(distRecord, buf, NULL, &opts, sizeof(opts)),
                 trans);

    if (trans->execute(NdbTransaction::Commit) != 0) {
      NdbError err = trans->getNdbError();
      if (err.status == NdbError::TemporaryError) {
        ndbout << err << endl;
        NdbSleep_MilliSleep(50);
        r--;  // just retry
      } else {
        CHECK(-1, trans);
      }
    }
    trans->close();
  }

  free(buf);

  return NDBT_OK;
}

struct PartInfo {
  NdbTransaction *trans;
  NdbIndexScanOperation *op;
  int dKeyVal;
  int valCount;
};

class Ap {
 public:
  void *ptr;

  Ap(void *_ptr) : ptr(_ptr) {}
  ~Ap() {
    if (ptr != 0) {
      free(ptr);
      ptr = 0;
    }
  }
};

static int dist_scan_body(Ndb *pNdb, int records, int parts, PartInfo *partInfo,
                          bool usePrimary) {
  const NdbDictionary::Table *tab =
      pNdb->getDictionary()->getTable(DistTabName);
  CHECKNOTNULL(tab, pNdb->getDictionary());
  const char *indexName = usePrimary ? "PRIMARY" : DistIdxName;
  const NdbDictionary::Index *idx =
      pNdb->getDictionary()->getIndex(indexName, DistTabName);
  CHECKNOTNULL(idx, pNdb->getDictionary());
  const NdbRecord *tabRecord = tab->getDefaultRecord();
  const NdbRecord *idxRecord = idx->getDefaultRecord();
  bool userDefined =
      (tab->getFragmentType() == NdbDictionary::Object::UserDefined);

  char *boundBuf = (char *)malloc(
      NdbDictionary::getRecordRowLength(idx->getDefaultRecord()));

  if (usePrimary)
    ndbout << "Checking MRR indexscan distribution awareness when distribution "
              "key part of bounds"
           << endl;
  else
    ndbout << "Checking MRR indexscan distribution awareness when distribution "
              "key provided explicitly"
           << endl;

  if (userDefined)
    ndbout << "User Defined Partitioning scheme" << endl;
  else
    ndbout << "Native Partitioning scheme" << endl;

  Ap boundAp(boundBuf);

  for (int r = 0; r < records; r++) {
    int partValue = r % parts;
    PartInfo &pInfo = partInfo[partValue];

    if (pInfo.trans == NULL) {
      /* Provide the partition key as a hint for this transaction */
      if (!userDefined) {
        Ndb::Key_part_ptr keyParts[2];
        keyParts[0].ptr = &partValue;
        keyParts[0].len = sizeof(partValue);
        keyParts[1].ptr = NULL;
        keyParts[1].len = 0;

        /* To test that bad hinting causes failure, uncomment */
        // int badPartVal= partValue+1;
        // keyParts[0].ptr= &badPartVal;

        CHECKNOTNULL(pInfo.trans = pNdb->startTransaction(tab, keyParts), pNdb);
      } else {
        /* User Defined partitioning */
        Uint32 partId = partValue % tab->getFragmentCount();
        CHECKNOTNULL(pInfo.trans = pNdb->startTransaction(tab, partId), pNdb);
      }
      pInfo.valCount = 0;
      pInfo.dKeyVal = partValue;

      NdbScanOperation::ScanOptions opts;
      opts.optionsPresent = NdbScanOperation::ScanOptions::SO_SCANFLAGS;
      opts.scan_flags = NdbScanOperation::SF_MultiRange;

      // Define the scan operation for this partition.
      CHECKNOTNULL(pInfo.op = pInfo.trans->scanIndex(
                       idx->getDefaultRecord(), tab->getDefaultRecord(),
                       NdbOperation::LM_Read, NULL, NULL, &opts, sizeof(opts)),
                   pInfo.trans);
    }

    NdbIndexScanOperation *op = pInfo.op;

    if (usePrimary) {
      {
        int dKeyVal = partValue;
        int pKey2Val = r;
        /* Scanning the primary index, set bound on the pk */
        memcpy(NdbDictionary::getValuePtr(
                   idxRecord, boundBuf,
                   tab->getColumn(DistTabDKeyCol)->getAttrId()),
               &dKeyVal, sizeof(dKeyVal));
        memcpy(NdbDictionary::getValuePtr(
                   idxRecord, boundBuf,
                   tab->getColumn(DistTabPKey2Col)->getAttrId()),
               &pKey2Val, sizeof(pKey2Val));
      }

      NdbIndexScanOperation::IndexBound ib;
      ib.low_key = boundBuf;
      ib.low_key_count = 2;
      ib.low_inclusive = true;
      ib.high_key = ib.low_key;
      ib.high_key_count = ib.low_key_count;
      ib.high_inclusive = true;
      ib.range_no = pInfo.valCount++;

      /* No partitioning info for native, PK index scan
       * NDBAPI can determine it from PK */
      Ndb::PartitionSpec pSpec;
      pSpec.type = Ndb::PartitionSpec::PS_NONE;

      if (userDefined) {
        /* We'll provide partition info */
        pSpec.type = Ndb::PartitionSpec::PS_USER_DEFINED;
        pSpec.UserDefined.partitionId = partValue % tab->getFragmentCount();
      }

      CHECK(op->setBound(idxRecord, ib, &pSpec, sizeof(pSpec)), op);
    } else {
      Uint32 resultValAttrId = tab->getColumn(DistTabResultCol)->getAttrId();
      /* Scanning the secondary index, set bound on the result */
      {
        int resultVal = r * r;
        memcpy(NdbDictionary::getValuePtr(idxRecord, boundBuf, resultValAttrId),
               &resultVal, sizeof(resultVal));
      }

      NdbDictionary::setNull(idxRecord, boundBuf, resultValAttrId, false);

      NdbIndexScanOperation::IndexBound ib;
      ib.low_key = boundBuf;
      ib.low_key_count = 1;
      ib.low_inclusive = true;
      ib.high_key = ib.low_key;
      ib.high_key_count = ib.low_key_count;
      ib.high_inclusive = true;
      ib.range_no = pInfo.valCount++;

      Ndb::Key_part_ptr keyParts[2];
      keyParts[0].ptr = &partValue;
      keyParts[0].len = sizeof(partValue);
      keyParts[1].ptr = NULL;
      keyParts[1].len = 0;

      /* To test that bad hinting causes failure, uncomment */
      // int badPartVal= partValue+1;
      // keyParts[0].ptr= &badPartVal;

      Ndb::PartitionSpec pSpec;
      char *tabRow = NULL;

      if (userDefined) {
        /* We'll provide partition info */
        pSpec.type = Ndb::PartitionSpec::PS_USER_DEFINED;
        pSpec.UserDefined.partitionId = partValue % tab->getFragmentCount();
      } else {
        /* Can set either using an array of Key parts, or a KeyRecord
         * structure.  Let's test both
         */
        if (rand() % 2) {
          // ndbout << "Using Key Parts to set range partition info" << endl;
          pSpec.type = Ndb::PartitionSpec::PS_DISTR_KEY_PART_PTR;
          pSpec.KeyPartPtr.tableKeyParts = keyParts;
          pSpec.KeyPartPtr.xfrmbuf = NULL;
          pSpec.KeyPartPtr.xfrmbuflen = 0;
        } else {
          // ndbout << "Using KeyRecord to set range partition info" << endl;

          /* Setup a row in NdbRecord format with the distkey value set */
          tabRow = (char *)malloc(NdbDictionary::getRecordRowLength(tabRecord));
          int &dKeyVal = *((int *)NdbDictionary::getValuePtr(
              tabRecord, tabRow, tab->getColumn(DistTabDKeyCol)->getAttrId()));
          dKeyVal = partValue;
          // dKeyVal= partValue + 1; // Test failure case

          pSpec.type = Ndb::PartitionSpec::PS_DISTR_KEY_RECORD;
          pSpec.KeyRecord.keyRecord = tabRecord;
          pSpec.KeyRecord.keyRow = tabRow;
          pSpec.KeyRecord.xfrmbuf = 0;
          pSpec.KeyRecord.xfrmbuflen = 0;
        }
      }

      CHECK(op->setBound(idxRecord, ib, &pSpec, sizeof(pSpec)), op);

      if (tabRow) free(tabRow);
      tabRow = NULL;
    }
  }

  for (int p = 0; p < parts; p++) {
    PartInfo &pInfo = partInfo[p];
    // ndbout << "D-key val " << p << " has " << pInfo.valCount
    //       << " ranges specified. " << endl;
    // ndbout << "Is Pruned? " << pInfo.op->getPruned() << endl;
    if (!pInfo.op->getPruned()) {
      ndbout << "MRR Scan Operation should have been pruned, but was not."
             << endl;
      return NDBT_FAILED;
    }

    CHECK(pInfo.trans->execute(NdbTransaction::NoCommit), pInfo.trans);

    int resultCount = 0;

    const char *resultPtr;
    int rc = 0;

    while ((rc = pInfo.op->nextResult(&resultPtr, true, true)) == 0) {
      int dKeyVal;
      memcpy(&dKeyVal,
             NdbDictionary::getValuePtr(
                 tabRecord, resultPtr,
                 tab->getColumn(DistTabDKeyCol)->getAttrId()),
             sizeof(dKeyVal));

      int pKey2Val;
      memcpy(&pKey2Val,
             NdbDictionary::getValuePtr(
                 tabRecord, resultPtr,
                 tab->getColumn(DistTabPKey2Col)->getAttrId()),
             sizeof(pKey2Val));

      int resultVal;
      memcpy(&resultVal,
             NdbDictionary::getValuePtr(
                 tabRecord, resultPtr,
                 tab->getColumn(DistTabResultCol)->getAttrId()),
             sizeof(resultVal));

      if ((dKeyVal != pInfo.dKeyVal) || (resultVal != (pKey2Val * pKey2Val))) {
        ndbout << "Got bad values.  Dkey : " << dKeyVal
               << " Pkey2 : " << pKey2Val << " Result : " << resultVal << endl;
        return NDBT_FAILED;
      }
      resultCount++;
    }

    if (rc != 1) {
      ndbout << "Got bad scan rc " << rc << endl;
      ndbout << "Error : " << pInfo.op->getNdbError().code << endl;
      ndbout << "Trans Error : " << pInfo.trans->getNdbError().code << endl;
      return NDBT_FAILED;
    }

    if (resultCount != pInfo.valCount) {
      ndbout << "Error resultCount was " << resultCount << endl;
      return NDBT_FAILED;
    }
    CHECK(pInfo.trans->execute(NdbTransaction::Commit), pInfo.trans);
    pInfo.trans->close();
  };

  ndbout << "Success" << endl;

  return NDBT_OK;
}

static int dist_scan(Ndb *pNdb, int records, int parts, bool usePk) {
  PartInfo *partInfo = new PartInfo[parts];

  NdbRestarter restarter;
  if (restarter.insertErrorInAllNodes(8050) != 0) {
    delete[] partInfo;
    return NDBT_FAILED;
  }

  for (int p = 0; p < parts; p++) {
    partInfo[p].trans = NULL;
    partInfo[p].op = NULL;
    partInfo[p].dKeyVal = 0;
    partInfo[p].valCount = 0;
  }

  int result = dist_scan_body(pNdb, records, parts, partInfo, usePk);

  restarter.insertErrorInAllNodes(0);
  delete[] partInfo;

  return result;
}

static int run_dist_test(NDBT_Context *ctx, NDBT_Step *step) {
  int records = ctx->getNumRecords();

  /* Choose an interesting number of discrete
   * distribution key values to work with
   */
  int numTabPartitions =
      GETNDB(step)->getDictionary()->getTable(DistTabName)->getFragmentCount();
  int numDkeyValues = 2 * numTabPartitions + (rand() % 6);
  if (numDkeyValues > records) {
    // limit number of distributions keys to number of records
    numDkeyValues = records;
  }

  ndbout << "Table has " << numTabPartitions << " physical partitions" << endl;
  ndbout << "Testing with " << numDkeyValues
         << " discrete distribution key values " << endl;

  if (load_dist_table(GETNDB(step), records, numDkeyValues) != NDBT_OK)
    return NDBT_FAILED;

  /* Test access via PK ordered index (including Dkey) */
  if (dist_scan(GETNDB(step), records, numDkeyValues, true) != NDBT_OK)
    return NDBT_FAILED;

  /* Test access via secondary ordered index (not including Dkey) */
  if (dist_scan(GETNDB(step), records, numDkeyValues, false) != NDBT_OK)
    return NDBT_FAILED;

  return NDBT_OK;
}

/**
 * RONDB-1074: PARTITION_HASH fanout tests
 *
 * A fanout table splits its primary key into base keys [0,x) and detail
 * keys [x,x+y) with routing hash ((base_hash/z)*z) + (detail_hash%z), so
 * all rows sharing a base key are stored in an interval of z fragments.
 *
 * The tests use a dedicated table with PK (BaseKey, DetailKey) and
 * PARTITION_HASH x:y:z = 1:1:<fanout>:
 * - metadata validation through the direct NDB API (errors 1243/1244)
 * - hinted PK operations agree with DBTC routing (error insert 8050)
 * - ordered index scans with base-key equality prune to the interval
 * - explicit hash-valued scan pruning is rejected with error 2203
 * - fanout = 1 keeps legacy behavior
 *
 * NOTE: error insert 8050 crashes a data node if any scan fragment is
 * not local to the TC node. Interval scans legitimately span nodes, so
 * 8050 must never be active during fanout interval scans.
 */

static const char *FanoutTabName = "FanoutTest";
static const char *FanoutBaseCol = "BaseKey";
static const char *FanoutDetailCol = "DetailKey";
static const char *FanoutResultCol = "Result";
static const char *FanoutIdxName = "PRIMARY";

static const Uint32 FANOUT_NUM_BASE = 16;    // distinct base key values
static const Uint32 FANOUT_NUM_DETAIL = 32;  // detail rows per base key
static const Uint32 FANOUT_FRAG_COUNT = 8;   // multiple of every fanout used

static Uint32 fanout_result_value(Uint32 base, Uint32 detail) {
  return base * 10000 + detail;
}

static void define_fanout_table(NdbDictionary::Table &tab, const char *name,
                                Uint32 base_keys, Uint32 detail_keys,
                                Uint32 fanout, Uint32 fragCount) {
  tab.setName(name);
  tab.setFragmentType(NdbDictionary::Object::HashMapPartition);
  if (fragCount > 0) {
    tab.setFragmentCount(fragCount);
    tab.setPartitionBalance(NdbDictionary::Object::PartitionBalance_Specific);
  }

  NdbDictionary::Column bk;
  bk.setName(FanoutBaseCol);
  bk.setType(NdbDictionary::Column::Unsigned);
  bk.setLength(1);
  bk.setNullable(false);
  bk.setPrimaryKey(true);
  tab.addColumn(bk);

  NdbDictionary::Column dk;
  dk.setName(FanoutDetailCol);
  dk.setType(NdbDictionary::Column::Unsigned);
  dk.setLength(1);
  dk.setNullable(false);
  dk.setPrimaryKey(true);
  tab.addColumn(dk);

  NdbDictionary::Column res;
  res.setName(FanoutResultCol);
  res.setType(NdbDictionary::Column::Unsigned);
  res.setLength(1);
  res.setNullable(true);
  res.setPrimaryKey(false);
  tab.addColumn(res);

  tab.setPartitionHash(base_keys, detail_keys, fanout);
}

/* Attempt a create through the direct NDB API, expecting success
 * (expectedError == 0, table is dropped again) or a specific error code.
 */
static int fanout_create_check(Ndb *pNdb, Uint32 base_keys, Uint32 detail_keys,
                               Uint32 fanout, Uint32 fragCount,
                               int expectedError) {
  NdbDictionary::Dictionary *dict = pNdb->getDictionary();
  NdbDictionary::Table tab;
  define_fanout_table(tab, "FanoutDDL", base_keys, detail_keys, fanout,
                      fragCount);

  const int ret = dict->createTable(tab);
  const int errCode = (ret == 0) ? 0 : dict->getNdbError().code;

  if (expectedError == 0) {
    if (ret != 0) {
      ndbout << "Create " << base_keys << ":" << detail_keys << ":" << fanout
             << " frags " << fragCount << " failed unexpectedly with "
             << errCode << endl;
      return NDBT_FAILED;
    }
    dict->dropTable("FanoutDDL");
    return NDBT_OK;
  }

  if (ret == 0) {
    ndbout << "Create " << base_keys << ":" << detail_keys << ":" << fanout
           << " frags " << fragCount << " succeeded but should have failed"
           << endl;
    dict->dropTable("FanoutDDL");
    return NDBT_FAILED;
  }
  if (errCode != expectedError) {
    ndbout << "Create " << base_keys << ":" << detail_keys << ":" << fanout
           << " frags " << fragCount << " failed with " << errCode
           << " expected " << expectedError << endl;
    return NDBT_FAILED;
  }
  return NDBT_OK;
}

static int run_fanout_ddl(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  NdbDictionary::Dictionary *dict = pNdb->getDictionary();
  const int InvalidPartitionHash = 1243;
  const int InvalidFanout = 1244;

  dict->dropTable("FanoutDDL");

  /* Invalid metadata combinations must be rejected with error 1243/1244 */
  if (fanout_create_check(pNdb, 0, 1, 2, FANOUT_FRAG_COUNT,
                          InvalidPartitionHash) != NDBT_OK)
    return NDBT_FAILED;  // base key count zero
  if (fanout_create_check(pNdb, 1, 0, 2, FANOUT_FRAG_COUNT,
                          InvalidPartitionHash) != NDBT_OK)
    return NDBT_FAILED;  // fanout > 1 without detail keys
  if (fanout_create_check(pNdb, 1, 2, 2, FANOUT_FRAG_COUNT,
                          InvalidPartitionHash) != NDBT_OK)
    return NDBT_FAILED;  // key counts do not match primary key
  if (fanout_create_check(pNdb, 1, 1, 0, FANOUT_FRAG_COUNT,
                          InvalidPartitionHash) != NDBT_OK)
    return NDBT_FAILED;  // fanout zero
  if (fanout_create_check(pNdb, 1, 1, 70000, FANOUT_FRAG_COUNT,
                          InvalidPartitionHash) != NDBT_OK)
    return NDBT_FAILED;  // fanout exceeds compact metadata (Uint16)
  if (fanout_create_check(pNdb, 1, 1, 2 * FANOUT_FRAG_COUNT, FANOUT_FRAG_COUNT,
                          InvalidFanout) != NDBT_OK)
    return NDBT_FAILED;  // fanout exceeds partition count
  if (fanout_create_check(pNdb, 1, 1, 7, FANOUT_FRAG_COUNT, InvalidFanout) !=
      NDBT_OK)
    return NDBT_FAILED;  // fanout does not divide hash map bucket count

  /* The fanout no longer has to divide the partition count */
  if (fanout_create_check(pNdb, 1, 1, 3, FANOUT_FRAG_COUNT, 0) != NDBT_OK)
    return NDBT_FAILED;  // fanout 3 with 8 partitions
  if (fanout_create_check(pNdb, 1, 1, 4, 6, 0) != NDBT_OK)
    return NDBT_FAILED;  // fanout 4 with 6 partitions

  /* A distribution key that is a proper subset of the primary key is
   * rejected on fanout tables: fanout routing ignores declared
   * distribution keys and the flags would mislead pre-fanout clients
   * into pruning on them.
   */
  {
    NdbDictionary::Table tab;
    define_fanout_table(tab, "FanoutDDL", 1, 1, 4, FANOUT_FRAG_COUNT);
    tab.getColumn(FanoutBaseCol)->setPartitionKey(true);
    if (dict->createTable(tab) == 0) {
      ndbout << "Create with distribution key subset succeeded but should"
             << " have failed" << endl;
      dict->dropTable("FanoutDDL");
      return NDBT_FAILED;
    }
    if (dict->getNdbError().code != InvalidPartitionHash) {
      ndbout << "Create with distribution key subset failed with "
             << dict->getNdbError().code << " expected "
             << InvalidPartitionHash << endl;
      return NDBT_FAILED;
    }
  }

  /* Valid spec: create, verify dictionary round-trip, reject alter */
  {
    NdbDictionary::Table tab;
    define_fanout_table(tab, "FanoutDDL", 1, 1, 4, FANOUT_FRAG_COUNT);
    CHECK(dict->createTable(tab), dict);

    const NdbDictionary::Table *pTab = dict->getTable("FanoutDDL");
    CHECKNOTNULL(pTab, dict);
    if (pTab->getPartitionHashBaseKeyCount() != 1 ||
        pTab->getPartitionHashDetailKeyCount() != 1 ||
        pTab->getPartitionHashFanout() != 4) {
      ndbout << "Retrieved partition hash metadata mismatch: "
             << pTab->getPartitionHashBaseKeyCount() << ":"
             << pTab->getPartitionHashDetailKeyCount() << ":"
             << pTab->getPartitionHashFanout() << endl;
      return NDBT_FAILED;
    }

    /* Changing the partition hash spec through alter must be rejected */
    NdbDictionary::Table alteredTab(*pTab);
    alteredTab.setPartitionHash(2, 0, 1);
    if (dict->alterTable(*pTab, alteredTab) == 0) {
      ndbout << "Alter changing PARTITION_HASH succeeded but should not"
             << endl;
      return NDBT_FAILED;
    }
    const int alterErr = dict->getNdbError().code;
    if (alterErr != 741)  // Unsupported alter table
    {
      ndbout << "Alter changing PARTITION_HASH failed with " << alterErr
             << " expected 741" << endl;
      return NDBT_FAILED;
    }
    CHECK(dict->dropTable("FanoutDDL"), dict);
  }

  /* A table created without partition hash metadata is normalized to
   * (distribution_key_count, remaining_keys, 1) and must work
   */
  {
    NdbDictionary::Table tab;
    define_fanout_table(tab, "FanoutDDL", 0, 0, 1, 0);
    CHECK(dict->createTable(tab), dict);
    const NdbDictionary::Table *pTab = dict->getTable("FanoutDDL");
    CHECKNOTNULL(pTab, dict);
    if (pTab->getPartitionHashBaseKeyCount() != 2 ||
        pTab->getPartitionHashDetailKeyCount() != 0 ||
        pTab->getPartitionHashFanout() != 1) {
      ndbout << "Default table metadata not normalized to (pk, 0, 1): "
             << pTab->getPartitionHashBaseKeyCount() << ":"
             << pTab->getPartitionHashDetailKeyCount() << ":"
             << pTab->getPartitionHashFanout() << endl;
      return NDBT_FAILED;
    }
    CHECK(dict->dropTable("FanoutDDL"), dict);
  }

  return NDBT_OK;
}

static int run_create_fanout_table(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  NdbDictionary::Dictionary *dict = pNdb->getDictionary();
  const Uint32 fanout = ctx->getProperty("Fanout", (unsigned)4);

  dict->dropTable(FanoutTabName);

  NdbDictionary::Table tab;
  define_fanout_table(tab, FanoutTabName, 1, 1, fanout, FANOUT_FRAG_COUNT);
  CHECK(dict->createTable(tab), dict);

  const NdbDictionary::Table *pTab = dict->getTable(FanoutTabName);
  CHECKNOTNULL(pTab, dict);
  if (pTab->getPartitionHashBaseKeyCount() != 1 ||
      pTab->getPartitionHashDetailKeyCount() != 1 ||
      pTab->getPartitionHashFanout() != fanout) {
    ndbout << "Partition hash metadata did not survive dictionary round-trip"
           << endl;
    return NDBT_FAILED;
  }

  /* Ordered index on the primary key, used for interval scans */
  NdbDictionary::Index idx;
  idx.setType(NdbDictionary::Index::OrderedIndex);
  idx.setLogging(false);
  idx.setTable(FanoutTabName);
  idx.setName(FanoutIdxName);
  idx.addColumnName(FanoutBaseCol);
  idx.addColumnName(FanoutDetailCol);
  CHECK(dict->createIndex(idx), dict);

  return NDBT_OK;
}

static int run_drop_fanout_table(NDBT_Context *ctx, NDBT_Step *step) {
  GETNDB(step)->getDictionary()->dropTable(FanoutTabName);
  return NDBT_OK;
}

/* Load all (base, detail) rows without transaction hinting.
 * Must not run with error insert 8050 active.
 */
static int run_load_fanout_table(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  const NdbDictionary::Table *tab =
      pNdb->getDictionary()->getTable(FanoutTabName);
  CHECKNOTNULL(tab, pNdb->getDictionary());
  const NdbRecord *rec = tab->getDefaultRecord();
  CHECKNOTNULL(rec, pNdb);

  const Uint32 rowLen = NdbDictionary::getRecordRowLength(rec);
  char *buf = (char *)malloc(rowLen);
  CHECKNOTNULL(buf, pNdb);
  Ap bufAp(buf);

  const Uint32 baseAttrId = tab->getColumn(FanoutBaseCol)->getAttrId();
  const Uint32 detailAttrId = tab->getColumn(FanoutDetailCol)->getAttrId();
  const Uint32 resultAttrId = tab->getColumn(FanoutResultCol)->getAttrId();

  for (Uint32 b = 0; b < FANOUT_NUM_BASE; b++) {
    NdbTransaction *trans = pNdb->startTransaction();
    CHECKNOTNULL(trans, pNdb);

    for (Uint32 d = 0; d < FANOUT_NUM_DETAIL; d++) {
      memcpy(NdbDictionary::getValuePtr(rec, buf, baseAttrId), &b, sizeof(b));
      memcpy(NdbDictionary::getValuePtr(rec, buf, detailAttrId), &d,
             sizeof(d));
      const Uint32 val = fanout_result_value(b, d);
      memcpy(NdbDictionary::getValuePtr(rec, buf, resultAttrId), &val,
             sizeof(val));
      NdbDictionary::setNull(rec, buf, resultAttrId, false);

      CHECKNOTNULL(trans->insertTuple(rec, buf), trans);
    }
    CHECK(trans->execute(NdbTransaction::Commit), trans);
    trans->close();
  }
  return NDBT_OK;
}

/* Start a transaction hinted with the (base, detail) primary key */
static NdbTransaction *fanout_hinted_trans(Ndb *pNdb,
                                           const NdbDictionary::Table *tab,
                                           Uint32 base, Uint32 detail) {
  Ndb::Key_part_ptr keyParts[3];
  keyParts[0].ptr = &base;
  keyParts[0].len = sizeof(base);
  keyParts[1].ptr = &detail;
  keyParts[1].len = sizeof(detail);
  keyParts[2].ptr = NULL;
  keyParts[2].len = 0;
  return pNdb->startTransaction(tab, keyParts);
}

/**
 * Hinted primary key operations with error insert 8050: if the API
 * transaction hint (composed fanout routing hash) does not agree with
 * DBTC's routing, a data node asserts and the test fails.
 */
static int run_fanout_pk_ops(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  const NdbDictionary::Table *tab =
      pNdb->getDictionary()->getTable(FanoutTabName);
  CHECKNOTNULL(tab, pNdb->getDictionary());
  const NdbRecord *rec = tab->getDefaultRecord();
  CHECKNOTNULL(rec, pNdb);

  const Uint32 rowLen = NdbDictionary::getRecordRowLength(rec);
  char *buf = (char *)malloc(rowLen);
  char *resBuf = (char *)malloc(rowLen);
  CHECKNOTNULL(buf, pNdb);
  CHECKNOTNULL(resBuf, pNdb);
  Ap bufAp(buf);
  Ap resBufAp(resBuf);

  const Uint32 baseAttrId = tab->getColumn(FanoutBaseCol)->getAttrId();
  const Uint32 detailAttrId = tab->getColumn(FanoutDetailCol)->getAttrId();
  const Uint32 resultAttrId = tab->getColumn(FanoutResultCol)->getAttrId();
  /* Column mask covering only the Result column */
  unsigned char resultMask[4] = {0, 0, 0, 0};
  resultMask[resultAttrId >> 3] = (unsigned char)(1 << (resultAttrId & 7));

  NdbRestarter restarter;
  if (restarter.insertErrorInAllNodes(8050) != 0) return NDBT_FAILED;

  int result = NDBT_OK;
  for (Uint32 b = 0; b < FANOUT_NUM_BASE && result == NDBT_OK; b++) {
    for (Uint32 d = 0; d < FANOUT_NUM_DETAIL && result == NDBT_OK; d++) {
      const Uint32 val = fanout_result_value(b, d);

      memcpy(NdbDictionary::getValuePtr(rec, buf, baseAttrId), &b, sizeof(b));
      memcpy(NdbDictionary::getValuePtr(rec, buf, detailAttrId), &d,
             sizeof(d));
      memcpy(NdbDictionary::getValuePtr(rec, buf, resultAttrId), &val,
             sizeof(val));
      NdbDictionary::setNull(rec, buf, resultAttrId, false);

      /* Hinted insert */
      {
        NdbTransaction *trans = fanout_hinted_trans(pNdb, tab, b, d);
        if (trans == NULL) {
          ndbout << "startTransaction failed " << pNdb->getNdbError().code
                 << endl;
          result = NDBT_FAILED;
          break;
        }
        if (trans->insertTuple(rec, buf) == NULL ||
            trans->execute(NdbTransaction::Commit) != 0) {
          ndbout << "Hinted insert failed " << trans->getNdbError().code
                 << endl;
          result = NDBT_FAILED;
        }
        trans->close();
      }
      if (result != NDBT_OK) break;

      /* Hinted locking read, verify value */
      {
        NdbTransaction *trans = fanout_hinted_trans(pNdb, tab, b, d);
        if (trans == NULL) {
          result = NDBT_FAILED;
          break;
        }
        memset(resBuf, 0, rowLen);
        if (trans->readTuple(rec, buf, rec, resBuf, NdbOperation::LM_Read) ==
                NULL ||
            trans->execute(NdbTransaction::Commit) != 0) {
          ndbout << "Hinted read failed " << trans->getNdbError().code << endl;
          result = NDBT_FAILED;
        } else {
          Uint32 readVal;
          memcpy(&readVal,
                 NdbDictionary::getValuePtr(rec, resBuf, resultAttrId),
                 sizeof(readVal));
          if (readVal != val) {
            ndbout << "Read wrong value " << readVal << " expected " << val
                   << endl;
            result = NDBT_FAILED;
          }
        }
        trans->close();
      }
      if (result != NDBT_OK) break;

      /* Hinted update of the Result column */
      {
        const Uint32 newVal = val + 1;
        memcpy(NdbDictionary::getValuePtr(rec, buf, resultAttrId), &newVal,
               sizeof(newVal));
        NdbTransaction *trans = fanout_hinted_trans(pNdb, tab, b, d);
        if (trans == NULL) {
          result = NDBT_FAILED;
          break;
        }
        if (trans->updateTuple(rec, buf, rec, buf, resultMask) == NULL ||
            trans->execute(NdbTransaction::Commit) != 0) {
          ndbout << "Hinted update failed " << trans->getNdbError().code
                 << endl;
          result = NDBT_FAILED;
        }
        trans->close();
      }
      if (result != NDBT_OK) break;

      /* Hinted write (upsert) restoring the original value */
      {
        memcpy(NdbDictionary::getValuePtr(rec, buf, resultAttrId), &val,
               sizeof(val));
        NdbTransaction *trans = fanout_hinted_trans(pNdb, tab, b, d);
        if (trans == NULL) {
          result = NDBT_FAILED;
          break;
        }
        if (trans->writeTuple(rec, buf, rec, buf) == NULL ||
            trans->execute(NdbTransaction::Commit) != 0) {
          ndbout << "Hinted write failed " << trans->getNdbError().code
                 << endl;
          result = NDBT_FAILED;
        }
        trans->close();
      }
      if (result != NDBT_OK) break;

      /* Hinted delete */
      {
        NdbTransaction *trans = fanout_hinted_trans(pNdb, tab, b, d);
        if (trans == NULL) {
          result = NDBT_FAILED;
          break;
        }
        if (trans->deleteTuple(rec, buf, rec) == NULL ||
            trans->execute(NdbTransaction::Commit) != 0) {
          ndbout << "Hinted delete failed " << trans->getNdbError().code
                 << endl;
          result = NDBT_FAILED;
        }
        trans->close();
      }
    }
  }

  restarter.insertErrorInAllNodes(0);
  return result;
}

struct FanoutScanRange {
  Uint32 base;
  Uint32 detail;
  bool hasDetail;   // equality on (base, detail) instead of base only
  bool openHigh;    // low bound on base only, no high bound
};

/**
 * Run one ordered-index scan with the given ranges, check pruned state
 * and verify returned rows (contents and count).
 */
static int fanout_scan_check(Ndb *pNdb, const FanoutScanRange *ranges,
                             Uint32 rangeCount, bool expectPruned,
                             Uint32 expectedRows) {
  const NdbDictionary::Table *tab =
      pNdb->getDictionary()->getTable(FanoutTabName);
  CHECKNOTNULL(tab, pNdb->getDictionary());
  const NdbDictionary::Index *idx =
      pNdb->getDictionary()->getIndex(FanoutIdxName, FanoutTabName);
  CHECKNOTNULL(idx, pNdb->getDictionary());
  const NdbRecord *tabRec = tab->getDefaultRecord();
  const NdbRecord *idxRec = idx->getDefaultRecord();

  const Uint32 baseAttrId = tab->getColumn(FanoutBaseCol)->getAttrId();
  const Uint32 detailAttrId = tab->getColumn(FanoutDetailCol)->getAttrId();
  const Uint32 resultAttrId = tab->getColumn(FanoutResultCol)->getAttrId();

  const Uint32 boundLen = NdbDictionary::getRecordRowLength(idxRec);
  /* Each range needs its own bound buffer for the send */
  char *boundBufs = (char *)malloc(boundLen * rangeCount);
  CHECKNOTNULL(boundBufs, pNdb);
  Ap boundAp(boundBufs);

  NdbTransaction *trans = pNdb->startTransaction();
  CHECKNOTNULL(trans, pNdb);

  NdbScanOperation::ScanOptions opts;
  opts.optionsPresent = NdbScanOperation::ScanOptions::SO_SCANFLAGS;
  opts.scan_flags = NdbScanOperation::SF_MultiRange;

  NdbIndexScanOperation *op =
      trans->scanIndex(idxRec, tabRec, NdbOperation::LM_Read, NULL, NULL,
                       &opts, sizeof(opts));
  CHECKNOTNULL(op, trans);

  for (Uint32 r = 0; r < rangeCount; r++) {
    char *boundBuf = boundBufs + r * boundLen;
    memcpy(NdbDictionary::getValuePtr(idxRec, boundBuf, baseAttrId),
           &ranges[r].base, sizeof(Uint32));
    Uint32 keyCount = 1;
    if (ranges[r].hasDetail) {
      memcpy(NdbDictionary::getValuePtr(idxRec, boundBuf, detailAttrId),
             &ranges[r].detail, sizeof(Uint32));
      keyCount = 2;
    }

    NdbIndexScanOperation::IndexBound ib;
    ib.low_key = boundBuf;
    ib.low_key_count = keyCount;
    ib.low_inclusive = true;
    if (ranges[r].openHigh) {
      ib.high_key = NULL;
      ib.high_key_count = 0;
    } else {
      ib.high_key = boundBuf;
      ib.high_key_count = keyCount;
    }
    ib.high_inclusive = true;
    ib.range_no = r;

    CHECK(op->setBound(idxRec, ib), op);
  }

  if (op->getPruned() != expectPruned) {
    ndbout << "Scan pruned state was " << op->getPruned() << " expected "
           << expectPruned << endl;
    trans->close();
    return NDBT_FAILED;
  }

  CHECK(trans->execute(NdbTransaction::NoCommit), trans);

  Uint32 rowCount = 0;
  const char *resultPtr;
  int rc;
  while ((rc = op->nextResult(&resultPtr, true, true)) == 0) {
    Uint32 b, d, val;
    memcpy(&b, NdbDictionary::getValuePtr(tabRec, resultPtr, baseAttrId),
           sizeof(b));
    memcpy(&d, NdbDictionary::getValuePtr(tabRec, resultPtr, detailAttrId),
           sizeof(d));
    memcpy(&val, NdbDictionary::getValuePtr(tabRec, resultPtr, resultAttrId),
           sizeof(val));

    if (val != fanout_result_value(b, d)) {
      ndbout << "Bad row contents: base " << b << " detail " << d
             << " result " << val << endl;
      trans->close();
      return NDBT_FAILED;
    }

    /* The row must match one of the requested ranges */
    bool matched = false;
    for (Uint32 r = 0; r < rangeCount; r++) {
      if (ranges[r].openHigh) {
        matched |= (b >= ranges[r].base);
      } else if (ranges[r].hasDetail) {
        matched |= (b == ranges[r].base && d == ranges[r].detail);
      } else {
        matched |= (b == ranges[r].base);
      }
    }
    if (!matched) {
      ndbout << "Row outside requested ranges: base " << b << " detail " << d
             << endl;
      trans->close();
      return NDBT_FAILED;
    }
    rowCount++;
  }

  if (rc != 1) {
    ndbout << "Scan failed, rc " << rc << " op error "
           << op->getNdbError().code << " trans error "
           << trans->getNdbError().code << endl;
    trans->close();
    return NDBT_FAILED;
  }

  trans->close();

  if (rowCount != expectedRows) {
    ndbout << "Scan returned " << rowCount << " rows, expected "
           << expectedRows << endl;
    return NDBT_FAILED;
  }
  return NDBT_OK;
}

/**
 * Ordered index scans on a fanout table:
 * - base-key equality (open or bounded detail) prunes to the interval
 * - MRR ranges in the same interval stay pruned
 * - MRR ranges in different intervals fall back to unpruned, correctly
 * - mixed prunable and non-prunable ranges fall back to unpruned
 *
 * No error insert: interval scans span several nodes by design.
 */
static int run_fanout_interval_scan(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);

  /* a) Single range, equality on base key only: pruned interval scan */
  {
    FanoutScanRange r[1] = {{7, 0, false, false}};
    if (fanout_scan_check(pNdb, r, 1, true, FANOUT_NUM_DETAIL) != NDBT_OK)
      return NDBT_FAILED;
  }

  /* b) Single range, equality on the full primary key: still an interval */
  {
    FanoutScanRange r[1] = {{7, 3, true, false}};
    if (fanout_scan_check(pNdb, r, 1, true, 1) != NDBT_OK) return NDBT_FAILED;
  }

  /* c) MRR: several ranges with the same base key stay pruned */
  {
    FanoutScanRange r[3] = {
        {7, 3, true, false}, {7, 11, true, false}, {7, 19, true, false}};
    if (fanout_scan_check(pNdb, r, 3, true, 3) != NDBT_OK) return NDBT_FAILED;
  }

  /* d) MRR: ranges with different base keys map to different intervals,
   * the scan falls back to unpruned but must return correct rows
   */
  {
    FanoutScanRange r[8];
    for (Uint32 i = 0; i < 8; i++) {
      r[i].base = i;
      r[i].detail = 5;
      r[i].hasDetail = true;
      r[i].openHigh = false;
    }
    if (fanout_scan_check(pNdb, r, 8, false, 8) != NDBT_OK)
      return NDBT_FAILED;
  }

  /* e) MRR: one prunable range plus one open (non-prunable) range */
  {
    FanoutScanRange r[2] = {
        {2, 0, false, false},
        {FANOUT_NUM_BASE - 2, 0, false, true}};  // base >= 14: 2 base keys
    if (fanout_scan_check(pNdb, r, 2, false, 3 * FANOUT_NUM_DETAIL) != NDBT_OK)
      return NDBT_FAILED;
  }

  return NDBT_OK;
}

/**
 * Explicit hash-valued scan pruning must be rejected with error 2203 on
 * fanout tables, and explicit partition-id scans (SO_PARTITION_ID) keep
 * their existing restriction (error 4546 on non-UserDefined tables).
 */
static int run_fanout_explicit_prune(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  const NdbDictionary::Table *tab =
      pNdb->getDictionary()->getTable(FanoutTabName);
  CHECKNOTNULL(tab, pNdb->getDictionary());
  const NdbDictionary::Index *idx =
      pNdb->getDictionary()->getIndex(FanoutIdxName, FanoutTabName);
  CHECKNOTNULL(idx, pNdb->getDictionary());
  const NdbRecord *tabRec = tab->getDefaultRecord();
  const NdbRecord *idxRec = idx->getDefaultRecord();

  const Uint32 baseAttrId = tab->getColumn(FanoutBaseCol)->getAttrId();
  const Uint32 detailAttrId = tab->getColumn(FanoutDetailCol)->getAttrId();

  const Uint32 boundLen = NdbDictionary::getRecordRowLength(idxRec);
  char *boundBuf = (char *)malloc(boundLen);
  char *tabRow = (char *)malloc(NdbDictionary::getRecordRowLength(tabRec));
  CHECKNOTNULL(boundBuf, pNdb);
  CHECKNOTNULL(tabRow, pNdb);
  Ap boundAp(boundBuf);
  Ap tabRowAp(tabRow);

  Uint32 base = 1;
  Uint32 detail = 1;

  /* Common range: equality on the full primary key */
  memcpy(NdbDictionary::getValuePtr(idxRec, boundBuf, baseAttrId), &base,
         sizeof(base));
  memcpy(NdbDictionary::getValuePtr(idxRec, boundBuf, detailAttrId), &detail,
         sizeof(detail));
  NdbIndexScanOperation::IndexBound ib;
  ib.low_key = boundBuf;
  ib.low_key_count = 2;
  ib.low_inclusive = true;
  ib.high_key = boundBuf;
  ib.high_key_count = 2;
  ib.high_inclusive = true;
  ib.range_no = 0;

  for (int variant = 0; variant < 2; variant++) {
    NdbTransaction *trans = pNdb->startTransaction();
    CHECKNOTNULL(trans, pNdb);

    NdbIndexScanOperation *op = trans->scanIndex(
        idxRec, tabRec, NdbOperation::LM_Read, NULL, NULL, NULL, 0);
    CHECKNOTNULL(op, trans);

    Ndb::Key_part_ptr keyParts[3];
    keyParts[0].ptr = &base;
    keyParts[0].len = sizeof(base);
    keyParts[1].ptr = &detail;
    keyParts[1].len = sizeof(detail);
    keyParts[2].ptr = NULL;
    keyParts[2].len = 0;

    Ndb::PartitionSpec pSpec;
    if (variant == 0) {
      pSpec.type = Ndb::PartitionSpec::PS_DISTR_KEY_PART_PTR;
      pSpec.KeyPartPtr.tableKeyParts = keyParts;
      pSpec.KeyPartPtr.xfrmbuf = NULL;
      pSpec.KeyPartPtr.xfrmbuflen = 0;
    } else {
      memcpy(NdbDictionary::getValuePtr(tabRec, tabRow, baseAttrId), &base,
             sizeof(base));
      memcpy(NdbDictionary::getValuePtr(tabRec, tabRow, detailAttrId),
             &detail, sizeof(detail));
      pSpec.type = Ndb::PartitionSpec::PS_DISTR_KEY_RECORD;
      pSpec.KeyRecord.keyRecord = tabRec;
      pSpec.KeyRecord.keyRow = tabRow;
      pSpec.KeyRecord.xfrmbuf = NULL;
      pSpec.KeyRecord.xfrmbuflen = 0;
    }

    CHECK(op->setBound(idxRec, ib, &pSpec, sizeof(pSpec)), op);

    /* The scan must be rejected by DBTC with 2203:
     * hash-valued one-partition pruning is ambiguous on fanout tables
     */
    int execRc = trans->execute(NdbTransaction::NoCommit);
    int errCode = trans->getNdbError().code;
    if (execRc == 0) {
      const char *resultPtr;
      int rc = op->nextResult(&resultPtr, true, true);
      if (rc >= 0) {
        ndbout << "Explicit hash-valued prune variant " << variant
               << " was not rejected" << endl;
        trans->close();
        return NDBT_FAILED;
      }
      errCode = op->getNdbError().code != 0 ? op->getNdbError().code
                                            : trans->getNdbError().code;
    }
    if (errCode != 2203) {
      ndbout << "Explicit hash-valued prune variant " << variant
             << " failed with " << errCode << " expected 2203" << endl;
      trans->close();
      return NDBT_FAILED;
    }
    trans->close();
  }

  /* SO_PARTITION_ID is not allowed on non-UserDefined tables (unchanged) */
  {
    NdbTransaction *trans = pNdb->startTransaction();
    CHECKNOTNULL(trans, pNdb);

    NdbScanOperation::ScanOptions opts;
    opts.optionsPresent = NdbScanOperation::ScanOptions::SO_PARTITION_ID;
    opts.partitionId = 0;

    NdbScanOperation *op = trans->scanTable(tabRec, NdbOperation::LM_Read,
                                            NULL, &opts, sizeof(opts));
    if (op != NULL) {
      ndbout << "SO_PARTITION_ID scan was not rejected" << endl;
      trans->close();
      return NDBT_FAILED;
    }
    if (trans->getNdbError().code != 4546) {
      ndbout << "SO_PARTITION_ID scan failed with "
             << trans->getNdbError().code << " expected 4546" << endl;
      trans->close();
      return NDBT_FAILED;
    }
    trans->close();
  }

  return NDBT_OK;
}

/**
 * Read back every loaded row through an (unhinted) primary key read and
 * verify the Result value. A primary key read locates the row through
 * DBTC's composed routing hash, so this proves that the stored placement
 * of every row still agrees with the routing hash - e.g. after node or
 * system restarts. A placement/routing mismatch shows up as error 626
 * (row not found).
 */
static int run_fanout_verify_data(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  const NdbDictionary::Table *tab =
      pNdb->getDictionary()->getTable(FanoutTabName);
  CHECKNOTNULL(tab, pNdb->getDictionary());
  const NdbRecord *rec = tab->getDefaultRecord();
  CHECKNOTNULL(rec, pNdb);

  const Uint32 rowLen = NdbDictionary::getRecordRowLength(rec);
  char *buf = (char *)malloc(rowLen);
  char *resBuf = (char *)malloc(rowLen);
  CHECKNOTNULL(buf, pNdb);
  CHECKNOTNULL(resBuf, pNdb);
  Ap bufAp(buf);
  Ap resBufAp(resBuf);

  const Uint32 baseAttrId = tab->getColumn(FanoutBaseCol)->getAttrId();
  const Uint32 detailAttrId = tab->getColumn(FanoutDetailCol)->getAttrId();
  const Uint32 resultAttrId = tab->getColumn(FanoutResultCol)->getAttrId();

  for (Uint32 b = 0; b < FANOUT_NUM_BASE; b++) {
    for (Uint32 d = 0; d < FANOUT_NUM_DETAIL; d++) {
      memcpy(NdbDictionary::getValuePtr(rec, buf, baseAttrId), &b, sizeof(b));
      memcpy(NdbDictionary::getValuePtr(rec, buf, detailAttrId), &d,
             sizeof(d));

      NdbTransaction *trans = pNdb->startTransaction();
      CHECKNOTNULL(trans, pNdb);
      memset(resBuf, 0, rowLen);
      const NdbOperation *op =
          trans->readTuple(rec, buf, rec, resBuf, NdbOperation::LM_Read);
      if (op == NULL || trans->execute(NdbTransaction::Commit) != 0 ||
          op->getNdbError().code != 0) {
        /* A row stored in a fragment that does not match the composed
         * routing hash shows up here as error 626 (row not found)
         */
        ndbout << "PK read of base " << b << " detail " << d << " failed: "
               << (op != NULL ? op->getNdbError().code
                              : trans->getNdbError().code)
               << endl;
        trans->close();
        return NDBT_FAILED;
      }
      Uint32 readVal;
      memcpy(&readVal, NdbDictionary::getValuePtr(rec, resBuf, resultAttrId),
             sizeof(readVal));
      trans->close();
      if (readVal != fanout_result_value(b, d)) {
        ndbout << "PK read of base " << b << " detail " << d
               << " returned wrong value " << readVal << endl;
        return NDBT_FAILED;
      }
    }
  }
  return NDBT_OK;
}

/**
 * Restart one data node (normal, then initial) and verify that the
 * partition hash metadata and row placement survive: all rows are
 * still found through composed-hash routed PK reads and interval scans
 * still prune and return correct results.
 */
static int run_fanout_node_restart(NDBT_Context *ctx, NDBT_Step *step) {
  NdbRestarter restarter;
  if (restarter.getNumDbNodes() < 2) {
    ndbout << "Too few data nodes, skipping" << endl;
    return NDBT_OK;
  }

  const int nodeId = restarter.getDbNodeId(rand() % restarter.getNumDbNodes());

  ndbout << "Restarting node " << nodeId << endl;
  if (restarter.restartOneDbNode(nodeId, false /* initial */,
                                 false /* nostart */, true /* abort */) != 0)
    return NDBT_FAILED;
  if (restarter.waitClusterStarted() != 0) return NDBT_FAILED;

  if (run_fanout_verify_data(ctx, step) != NDBT_OK) return NDBT_FAILED;
  if (run_fanout_interval_scan(ctx, step) != NDBT_OK) return NDBT_FAILED;

  ndbout << "Restarting node " << nodeId << " initial" << endl;
  if (restarter.restartOneDbNode(nodeId, true /* initial */,
                                 false /* nostart */, true /* abort */) != 0)
    return NDBT_FAILED;
  if (restarter.waitClusterStarted() != 0) return NDBT_FAILED;

  if (run_fanout_verify_data(ctx, step) != NDBT_OK) return NDBT_FAILED;
  if (run_fanout_interval_scan(ctx, step) != NDBT_OK) return NDBT_FAILED;

  return NDBT_OK;
}

/**
 * System restart: the partition hash metadata must survive the DICT
 * schema file round-trip and all (logged) rows must still be found
 * through composed-hash routed PK reads.
 */
static int run_fanout_system_restart(NDBT_Context *ctx, NDBT_Step *step) {
  NdbRestarter restarter;

  /* Graceful restart: an aborted shutdown intentionally discards
   * transactions committed after the last durable GCP, so rows loaded
   * just before the restart would legitimately be lost.
   */
  ndbout << "Restarting all nodes" << endl;
  if (restarter.restartAll(false /* initial */, false /* nostart */,
                           false /* abort */) != 0)
    return NDBT_FAILED;
  if (restarter.waitClusterStarted() != 0) return NDBT_FAILED;

  if (run_fanout_verify_data(ctx, step) != NDBT_OK) return NDBT_FAILED;
  if (run_fanout_interval_scan(ctx, step) != NDBT_OK) return NDBT_FAILED;

  return NDBT_OK;
}

/**
 * fanout = 1 keeps legacy behavior: key operations and fully-equal
 * distribution key scans behave exactly like an ordinary table.
 */
static int run_fanout_one(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  const NdbDictionary::Table *tab =
      pNdb->getDictionary()->getTable(FanoutTabName);
  CHECKNOTNULL(tab, pNdb->getDictionary());
  const NdbRecord *rec = tab->getDefaultRecord();
  CHECKNOTNULL(rec, pNdb);

  const Uint32 rowLen = NdbDictionary::getRecordRowLength(rec);
  char *buf = (char *)malloc(rowLen);
  char *resBuf = (char *)malloc(rowLen);
  CHECKNOTNULL(buf, pNdb);
  CHECKNOTNULL(resBuf, pNdb);
  Ap bufAp(buf);
  Ap resBufAp(resBuf);

  const Uint32 baseAttrId = tab->getColumn(FanoutBaseCol)->getAttrId();
  const Uint32 detailAttrId = tab->getColumn(FanoutDetailCol)->getAttrId();
  const Uint32 resultAttrId = tab->getColumn(FanoutResultCol)->getAttrId();

  /* Hinted reads with 8050: legacy full-PK routing must agree */
  NdbRestarter restarter;
  if (restarter.insertErrorInAllNodes(8050) != 0) return NDBT_FAILED;

  int result = NDBT_OK;
  for (Uint32 b = 0; b < FANOUT_NUM_BASE && result == NDBT_OK; b++) {
    for (Uint32 d = 0; d < FANOUT_NUM_DETAIL; d += 7) {
      NdbTransaction *trans = fanout_hinted_trans(pNdb, tab, b, d);
      if (trans == NULL) {
        result = NDBT_FAILED;
        break;
      }
      memcpy(NdbDictionary::getValuePtr(rec, buf, baseAttrId), &b, sizeof(b));
      memcpy(NdbDictionary::getValuePtr(rec, buf, detailAttrId), &d,
             sizeof(d));
      memset(resBuf, 0, rowLen);
      if (trans->readTuple(rec, buf, rec, resBuf, NdbOperation::LM_Read) ==
              NULL ||
          trans->execute(NdbTransaction::Commit) != 0) {
        ndbout << "fanout=1 hinted read failed " << trans->getNdbError().code
               << endl;
        result = NDBT_FAILED;
      } else {
        Uint32 readVal;
        memcpy(&readVal,
               NdbDictionary::getValuePtr(rec, resBuf, resultAttrId),
               sizeof(readVal));
        if (readVal != fanout_result_value(b, d)) {
          ndbout << "fanout=1 read wrong value" << endl;
          result = NDBT_FAILED;
        }
      }
      trans->close();
      if (result != NDBT_OK) break;
    }
  }

  restarter.insertErrorInAllNodes(0);
  if (result != NDBT_OK) return result;

  /* Full distribution-key equality scans prune to one partition as on
   * any ordinary table (SPS_ONE_PARTITION, not an interval)
   */
  {
    FanoutScanRange r[1] = {{3, 5, true, false}};
    if (fanout_scan_check(pNdb, r, 1, true, 1) != NDBT_OK) return NDBT_FAILED;
  }

  /* Base-key-only equality does not prune when fanout = 1: partition
   * hash intervals are only used with fanout > 1
   */
  {
    FanoutScanRange r[1] = {{3, 0, false, false}};
    if (fanout_scan_check(pNdb, r, 1, false, FANOUT_NUM_DETAIL) != NDBT_OK)
      return NDBT_FAILED;
  }

  return NDBT_OK;
}

NDBT_TESTSUITE(testPartitioning);
TESTCASE("pk_dk", "Primary key operations with distribution key") {
  TC_PROPERTY("distributionkey", ~0);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_pk_dk);
  INITIALIZER(run_drop_table);
}
TESTCASE("hash_index_dk", "Unique index operations with distribution key") {
  TC_PROPERTY("distributionkey", ~0);
  TC_PROPERTY("OrderedIndex", (unsigned)0);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_index_dk);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
}
TESTCASE("ordered_index_dk", "Ordered index operations with distribution key") {
  TC_PROPERTY("distributionkey", (unsigned)1);
  TC_PROPERTY("OrderedIndex", (unsigned)1);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_index_dk);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
}
TESTCASE("smart_scan", "Ordered index operations with distribution key") {
  TC_PROPERTY("OrderedIndex", (unsigned)1);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table_smart_scan);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_index_dk);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
}
TESTCASE("startTransactionHint",
         "Test startTransactionHint wo/ distribution key") {
  /* If hint is incorrect, node failure occurs */
  TC_PROPERTY("distributionkey", (unsigned)0);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_startHint);
  INITIALIZER(run_drop_table);
}
TESTCASE("startTransactionHint_dk",
         "Test startTransactionHint with distribution key") {
  /* If hint is incorrect, node failure occurs */
  TC_PROPERTY("distributionkey", (unsigned)~0);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_startHint);
  INITIALIZER(run_drop_table);
}
TESTCASE("startTransactionHint_orderedIndex",
         "Test startTransactionHint and ordered index reads") {
  /* If hint is incorrect, node failure occurs */
  TC_PROPERTY("distributionkey", (unsigned)0);
  TC_PROPERTY("OrderedIndex", (unsigned)1);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_startHint_ordered_index);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
}
TESTCASE(
    "startTransactionHint_orderedIndex_dk",
    "Test startTransactionHint and ordered index reads with distribution key") {
  /* If hint is incorrect, node failure occurs */
  TC_PROPERTY("distributionkey", (unsigned)~0);
  TC_PROPERTY("OrderedIndex", (unsigned)1);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_startHint_ordered_index);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
}
TESTCASE(
    "startTransactionHint_orderedIndex_mrr_native",
    "Test hinting and MRR Ordered Index Scans for native partitioned table") {
  TC_PROPERTY("UserDefined", (unsigned)0);
  INITIALIZER(run_create_dist_table);
  INITIALIZER(run_dist_test);
  INITIALIZER(run_drop_dist_table);
}
TESTCASE(
    "pk_userDefined",
    "Test primary key operations on table with user-defined partitioning") {
  /* Check PK ops against user-defined partitioned table */
  TC_PROPERTY("UserDefined", (unsigned)1);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_pk_dk);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
};
TESTCASE("hash_index_userDefined",
         "Unique index operations on table with user-defined partitioning") {
  /* Check hash index ops against user-defined partitioned table */
  TC_PROPERTY("OrderedIndex", (unsigned)0);
  TC_PROPERTY("UserDefined", (unsigned)1);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_index_dk);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
}
TESTCASE("ordered_index_userDefined",
         "Ordered index operations on table with user-defined partitioning") {
  /* Check ordered index operations against user-defined partitioned table */
  TC_PROPERTY("OrderedIndex", (unsigned)1);
  TC_PROPERTY("UserDefined", (unsigned)1);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_index_dk);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
}
TESTCASE("startTransactionHint_orderedIndex_mrr_userDefined",
         "Test hinting and MRR Ordered Index Scans for user defined "
         "partitioned table") {
  TC_PROPERTY("UserDefined", (unsigned)1);
  INITIALIZER(run_create_dist_table);
  INITIALIZER(run_dist_test);
  INITIALIZER(run_drop_dist_table);
}
TESTCASE("startTransactionHint_orderedIndex_MaxKey",
         "Test startTransactionHint with max hash value via error insert") {
  /* Special regression case */
  TC_PROPERTY("distributionkey", (unsigned)0);
  TC_PROPERTY("OrderedIndex", (unsigned)1);
  TC_PROPERTY("errorinsertion", (unsigned)8119);
  INITIALIZER(run_drop_table);
  INITIALIZER(run_create_table);
  INITIALIZER(run_create_pk_index);
  INITIALIZER(run_startHint_ordered_index);
  INITIALIZER(run_create_pk_index_drop);
  INITIALIZER(run_drop_table);
}
TESTCASE("fanout_ddl",
         "PARTITION_HASH metadata validation through the direct NDB API") {
  INITIALIZER(run_fanout_ddl);
}
TESTCASE("fanout_pk_ops",
         "Hinted primary key operations on a PARTITION_HASH fanout table."
         " If hint and DBTC routing disagree, node failure occurs") {
  TC_PROPERTY("Fanout", (unsigned)4);
  INITIALIZER(run_create_fanout_table);
  INITIALIZER(run_fanout_pk_ops);
  FINALIZER(run_drop_fanout_table);
}
TESTCASE("fanout_interval_scan",
         "Ordered index scans on a fanout table prune base-key equality"
         " ranges to the fanout interval") {
  TC_PROPERTY("Fanout", (unsigned)4);
  INITIALIZER(run_create_fanout_table);
  INITIALIZER(run_load_fanout_table);
  INITIALIZER(run_fanout_interval_scan);
  FINALIZER(run_drop_fanout_table);
}
TESTCASE("fanout_explicit_prune",
         "Explicit hash-valued scan pruning is rejected with 2203 on"
         " fanout tables, SO_PARTITION_ID keeps its restriction") {
  TC_PROPERTY("Fanout", (unsigned)4);
  INITIALIZER(run_create_fanout_table);
  INITIALIZER(run_load_fanout_table);
  INITIALIZER(run_fanout_explicit_prune);
  FINALIZER(run_drop_fanout_table);
}
TESTCASE("fanout_one",
         "PARTITION_HASH with fanout = 1 keeps legacy routing and"
         " one-partition scan pruning") {
  TC_PROPERTY("Fanout", (unsigned)1);
  INITIALIZER(run_create_fanout_table);
  INITIALIZER(run_load_fanout_table);
  INITIALIZER(run_fanout_one);
  FINALIZER(run_drop_fanout_table);
}
TESTCASE("fanout_node_restart",
         "Fanout table placement and metadata survive node restart"
         " (normal and initial)") {
  TC_PROPERTY("Fanout", (unsigned)4);
  INITIALIZER(run_create_fanout_table);
  INITIALIZER(run_load_fanout_table);
  INITIALIZER(run_fanout_node_restart);
  FINALIZER(run_drop_fanout_table);
}
TESTCASE("fanout_system_restart",
         "Fanout table placement and metadata survive system restart") {
  TC_PROPERTY("Fanout", (unsigned)4);
  INITIALIZER(run_create_fanout_table);
  INITIALIZER(run_load_fanout_table);
  INITIALIZER(run_fanout_system_restart);
  FINALIZER(run_drop_fanout_table);
}

NDBT_TESTSUITE_END(testPartitioning)

int main(int argc, const char **argv) {
  ndb_init();
  NDBT_TESTSUITE_INSTANCE(testPartitioning);
  testPartitioning.setCreateTable(false);
  return testPartitioning.execute(argc, argv);
}
