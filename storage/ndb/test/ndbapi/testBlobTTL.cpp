/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.

 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

/*
 * Blob tests on TTL tables through the NDB API.
 *
 * RecAttrInsertOverExpired: an insert on a TTL table can land on an
 * expired row that is not purged yet; the kernel then runs it as an update
 * and keeps the columns the insert does not set. An NdbRecAttr insert that
 * takes a blob handle but sets no value must leave the blob NULL, with no
 * part rows: its head must not keep the expired value's length while the
 * part probe deletes the parts behind it.
 */

#include <NDBT.hpp>
#include <NDBT_Test.hpp>
#include <NdbSleep.h>
#include <time.h>

static const char *const TabName = "T_BLOB_TTL";
static const Uint32 TtlSec = 2;
static const Uint32 TtlColNo = 1;
static const Uint32 InlineSize = 256;
static const Uint32 PartSize = 2000;
static const Uint32 NumParts = 5;
static const Uint32 BlobLen = InlineSize + NumParts * PartSize;
static const Uint32 Key = 1;
/* Expiry wait: 40 polls of 500 ms, well past TtlSec. */
static const int ExpiryPolls = 40;

#define CHK(b)                                                         \
  if (!(b)) {                                                          \
    g_err << "ERR: " << #b << " failed at line " << __LINE__ << endl; \
    return NDBT_FAILED;                                                \
  }

#define CHK_NDB(b, obj)                                                 \
  if (!(b)) {                                                           \
    g_err << "ERR: " << #b << " failed at line " << __LINE__ << ": "    \
          << (obj)->getNdbError() << endl;                              \
    return NDBT_FAILED;                                                 \
  }

/* TIMESTAMP2 with precision 0: UTC epoch seconds, 4 bytes, big-endian. */
static void packTimestamp2(unsigned char *buf, Uint32 sec) {
  buf[0] = (unsigned char)(sec >> 24);
  buf[1] = (unsigned char)(sec >> 16);
  buf[2] = (unsigned char)(sec >> 8);
  buf[3] = (unsigned char)sec;
}

int runCreateTable(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *ndb = GETNDB(step);
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (dict->getTable(TabName) != nullptr) {
    CHK_NDB(dict->dropTable(TabName) == 0, dict);
  }

  NdbDictionary::Table tab(TabName);
  {
    NdbDictionary::Column col("id");
    col.setType(NdbDictionary::Column::Unsigned);
    col.setPrimaryKey(true);
    tab.addColumn(col);
  }
  {
    NdbDictionary::Column col("ttl");
    col.setType(NdbDictionary::Column::Timestamp2);
    col.setPrecision(0);
    col.setNullable(false);
    tab.addColumn(col);
  }
  {
    NdbDictionary::Column col("v");
    col.setType(NdbDictionary::Column::Text);
    col.setNullable(true);
    col.setInlineSize(InlineSize);
    col.setPartSize(PartSize);
    col.setStripeSize(0);
    tab.addColumn(col);
  }
  tab.setTTLSec(TtlSec);
  tab.setTTLColumnNo(TtlColNo);
  CHK_NDB(dict->createTable(tab) == 0, dict);
  return NDBT_OK;
}

int runDropTable(NDBT_Context *ctx, NDBT_Step *step) {
  NdbDictionary::Dictionary *dict = GETNDB(step)->getDictionary();
  if (dict->getTable(TabName) != nullptr) {
    CHK_NDB(dict->dropTable(TabName) == 0, dict);
  }
  return NDBT_OK;
}

/* Insert the key with a value of BlobLen bytes through NdbRecord. */
static int insertNdbRecord(Ndb *ndb, const NdbDictionary::Table *tab,
                           const char *value) {
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const Uint32 idOffset = 0;
  const Uint32 ttlOffset = 4;
  const Uint32 blobOffset = 8;
  const Uint32 blobNullOffset = blobOffset + sizeof(NdbBlob *);

  NdbDictionary::RecordSpecification spec[3];
  memset(spec, 0, sizeof(spec));
  spec[0].column = tab->getColumn("id");
  spec[0].offset = idOffset;
  spec[1].column = tab->getColumn("ttl");
  spec[1].offset = ttlOffset;
  spec[2].column = tab->getColumn("v");
  spec[2].offset = blobOffset;
  spec[2].nullbit_byte_offset = blobNullOffset;
  spec[2].nullbit_bit_in_byte = 0;
  const NdbRecord *rec = dict->createRecord(tab, spec, 3, sizeof(spec[0]));
  CHK_NDB(rec != nullptr, dict);

  Uint64 rowBuf[4];
  char *row = reinterpret_cast<char *>(rowBuf);
  memset(rowBuf, 0, sizeof(rowBuf));
  memcpy(row + idOffset, &Key, sizeof(Key));
  packTimestamp2(reinterpret_cast<unsigned char *>(row + ttlOffset),
                 Uint32(time(nullptr)));

  NdbTransaction *trans = ndb->startTransaction();
  CHK_NDB(trans != nullptr, ndb);
  const NdbOperation *op = trans->insertTuple(rec, row);
  CHK_NDB(op != nullptr, trans);
  NdbBlob *bh = op->getBlobHandle("v");
  CHK_NDB(bh != nullptr, op);
  CHK_NDB(bh->setValue(value, BlobLen) == 0, bh);
  CHK_NDB(trans->execute(NdbTransaction::Commit) == 0, trans);
  ndb->closeTransaction(trans);
  dict->releaseRecord(rec);
  return NDBT_OK;
}

/* Insert the key through NdbRecAttr: take the blob handle, set no value. */
static int insertRecAttrNoBlobValue(Ndb *ndb,
                                    const NdbDictionary::Table *tab) {
  unsigned char ttl[4];
  packTimestamp2(ttl, Uint32(time(nullptr)));

  NdbTransaction *trans = ndb->startTransaction();
  CHK_NDB(trans != nullptr, ndb);
  NdbOperation *op = trans->getNdbOperation(tab);
  CHK_NDB(op != nullptr, trans);
  CHK_NDB(op->insertTuple() == 0, op);
  CHK_NDB(op->equal("id", Key) == 0, op);
  CHK_NDB(op->setValue("ttl", reinterpret_cast<const char *>(ttl)) == 0, op);
  NdbBlob *bh = op->getBlobHandle("v");
  CHK_NDB(bh != nullptr, op);
  CHK_NDB(trans->execute(NdbTransaction::Commit) == 0, trans);
  ndb->closeTransaction(trans);
  return NDBT_OK;
}

/* Read the key without TTL bypass: error code 0 (live) or 626 (expired). */
static int readKeyError(Ndb *ndb, const NdbDictionary::Table *tab,
                        int &errCode) {
  NdbTransaction *trans = ndb->startTransaction();
  CHK_NDB(trans != nullptr, ndb);
  NdbOperation *op = trans->getNdbOperation(tab);
  CHK_NDB(op != nullptr, trans);
  CHK_NDB(op->readTuple(NdbOperation::LM_CommittedRead) == 0, op);
  CHK_NDB(op->equal("id", Key) == 0, op);
  CHK_NDB(op->getValue("id") != nullptr, op);
  trans->execute(NdbTransaction::Commit);
  errCode = trans->getNdbError().code;
  ndb->closeTransaction(trans);
  return NDBT_OK;
}

/* Count the rows in the part table of column v. Parts carry no TTL. */
static int countParts(Ndb *ndb, const NdbDictionary::Table *tab,
                      Uint32 &count) {
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *partTab = dict->getBlobTable(tab, "v");
  CHK_NDB(partTab != nullptr, dict);

  NdbTransaction *trans = ndb->startTransaction();
  CHK_NDB(trans != nullptr, ndb);
  NdbScanOperation *sop = trans->getNdbScanOperation(partTab);
  CHK_NDB(sop != nullptr, trans);
  CHK_NDB(sop->readTuples(NdbOperation::LM_CommittedRead) == 0, sop);
  CHK_NDB(sop->getValue("NDB$PART") != nullptr, sop);
  CHK_NDB(trans->execute(NdbTransaction::NoCommit) == 0, trans);
  count = 0;
  int rc;
  while ((rc = sop->nextResult(true)) == 0) count++;
  CHK_NDB(rc == 1, sop);
  ndb->closeTransaction(trans);
  return NDBT_OK;
}

int runRecAttrInsertOverExpired(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *ndb = GETNDB(step);
  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable(TabName);
  CHK_NDB(tab != nullptr, ndb->getDictionary());
  CHK(tab->isTTLEnabled());

  char *value = new char[BlobLen];
  memset(value, 'a', BlobLen);
  const int insertRes = insertNdbRecord(ndb, tab, value);
  delete[] value;
  CHK(insertRes == NDBT_OK);

  Uint32 parts = 0;
  CHK(countParts(ndb, tab, parts) == NDBT_OK);
  g_info << "Part rows after the NdbRecord insert: " << parts << endl;
  CHK(parts == NumParts);

  int errCode = 0;
  int poll = 0;
  for (; poll < ExpiryPolls; poll++) {
    CHK(readKeyError(ndb, tab, errCode) == NDBT_OK);
    if (errCode == 626) break;
    CHK(errCode == 0);
    NdbSleep_MilliSleep(500);
  }
  CHK(poll < ExpiryPolls);

  /* The expired row must still be there for the insert to land on it. */
  CHK(countParts(ndb, tab, parts) == NDBT_OK);
  if (parts != NumParts) {
    g_err << "Expired row purged before the insert (" << parts
          << " part rows): nothing to check" << endl;
    return NDBT_FAILED;
  }

  CHK(insertRecAttrNoBlobValue(ndb, tab) == NDBT_OK);

  NdbTransaction *trans = ndb->startTransaction();
  CHK_NDB(trans != nullptr, ndb);
  NdbOperation *op = trans->getNdbOperation(tab);
  CHK_NDB(op != nullptr, trans);
  CHK_NDB(op->readTuple(NdbOperation::LM_Read) == 0, op);
  CHK_NDB(op->equal("id", Key) == 0, op);
  NdbBlob *bh = op->getBlobHandle("v");
  CHK_NDB(bh != nullptr, op);
  CHK_NDB(trans->execute(NdbTransaction::NoCommit) == 0, trans);
  int isNull = -1;
  CHK_NDB(bh->getNull(isNull) == 0, bh);
  if (isNull != 1) {
    Uint64 len = 0;
    CHK_NDB(bh->getLength(len) == 0, bh);
    g_err << "Blob not NULL after an insert that set no value, length "
          << len << endl;
    char *buf = new char[len + 1];
    Uint32 bytes = Uint32(len);
    const int readRes = bh->readData(buf, bytes);
    delete[] buf;
    if (readRes != 0) {
      g_err << "Reading it fails: " << bh->getNdbError() << endl;
    }
    ndb->closeTransaction(trans);
    return NDBT_FAILED;
  }
  CHK_NDB(trans->execute(NdbTransaction::Commit) == 0, trans);
  ndb->closeTransaction(trans);

  CHK(countParts(ndb, tab, parts) == NDBT_OK);
  g_info << "Part rows after the NdbRecAttr insert: " << parts << endl;
  CHK(parts == 0);
  return NDBT_OK;
}

NDBT_TESTSUITE(testBlobTTL);
TESTCASE("RecAttrInsertOverExpired",
         "An NdbRecAttr insert that sets no blob value, over an expired "
         "multi-part row, leaves the blob NULL with no part rows") {
  INITIALIZER(runCreateTable);
  STEP(runRecAttrInsertOverExpired);
  FINALIZER(runDropTable);
}
NDBT_TESTSUITE_END(testBlobTTL)

int main(int argc, const char **argv) {
  ndb_init();
  NDBT_TESTSUITE_INSTANCE(testBlobTTL);
  testBlobTTL.setCreateTable(false);
  testBlobTTL.setRunAllTables(true);
  testBlobTTL.setEnsureIndexStatTables(false);
  int res = testBlobTTL.execute(argc, argv);
  ndb_end(0);
  return res;
}
