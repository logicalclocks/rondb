/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
   Copyright (c) 2021, 2023, Hopsworks and/or its affiliates.

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

#ifndef EMULATOR_H
#define EMULATOR_H

//===========================================================================
//
// .DESCRIPTION
//      This is the main function for the AXE VM emulator.
//      It contains some global objects and a run method.
//
//===========================================================================
#include <kernel_types.h>

#define JAM_FILE_ID 260


extern class  JobTable            globalJobTable;
extern class  TimeQueue           globalTimeQueue;
extern class  FastScheduler       globalScheduler;
extern class  TransporterRegistry globalTransporterRegistry;
extern struct GlobalData          globalData;

#ifdef VM_TRACE
extern class SignalLoggerManager globalSignalLoggers;
#endif

#if defined(VM_TRACE) || defined(ERROR_INSERT)
#define EMULATED_JAM_SIZE 65536
#else
// Keep jam buffer small for optimized build to improve locality of reference.
#define EMULATED_JAM_SIZE 1024
#endif
#define JAM_MASK (EMULATED_JAM_SIZE - 1)

/* EMULATED_JAM_SIZE must be a power of two, so JAM_MASK will work. */
#define TEST_EMULATED_JAM_SIZE(x) static_assert \
  ((((Uint32)(x)) % EMULATED_JAM_SIZE) == (((Uint32)(x)) & JAM_MASK))
TEST_EMULATED_JAM_SIZE(-123456);
TEST_EMULATED_JAM_SIZE(-1);
TEST_EMULATED_JAM_SIZE(0);
TEST_EMULATED_JAM_SIZE(1);
TEST_EMULATED_JAM_SIZE(JAM_MASK-1);
TEST_EMULATED_JAM_SIZE(JAM_MASK);
TEST_EMULATED_JAM_SIZE(EMULATED_JAM_SIZE);
TEST_EMULATED_JAM_SIZE(EMULATED_JAM_SIZE+1);
TEST_EMULATED_JAM_SIZE(123456);
#undef TEST_EMULATED_JAM_SIZE

/**
 * DESCRIPTION:
 *
 * This table maps from JAM_FILE_ID to the corresponding source file name.
 *
 * Each source file that has jam() calls must have a macro definition of the
 * following type:
 *
 *      #define JAM_FILE_ID <number>
 *
 * where <number> is unique to that source file. This definition should be
 * placed *after* the last #include of any file that may directly or indirectly
 * contain another definition of JAM_FILE_ID, and *before* the first jam() call.
 *
 * Each include file should also have an '#undef JAM_FILE_ID' at the end of the
 * file. Note that some .cpp files (e.g. SimulatedBlock.cpp) are also used as
 * include files.
 *
 * JAM_FILE_ID is used as an index into the jamFileNames table, so the
 * corresponding table entry should contain the base name of the source file.
 *
 * MAINTENANCE:
 *
 * If you wish to delete a source file, set the corresponding table entry to
 * NULL.
 *
 * If you wish to add jam trace to a new source file, find the first NULL
 * table entry and set it to the base name of the source file. If there is no
 * NULL entry, add a new entry at the end of the table. Add a JAM_FILE_ID
 * definition to the source file, as described above.
 *
 * To rename a source file, simply update the table entry with the new base
 * name.
 *
 * To check for problems, run the ./test_jamFileNames.sh script. It will check
 * that each entry in the table is used in exactly one file and vice versa, or
 * report problems.
 *
 * ./test_jamFileNames.sh
 *
 * TROUBLESHOOTING:
 *
 * Symptom: Compiler error stating that JAM_FILE_ID is undefined.
 * Causes:
 *   - The source file misses a JAM_FILE_ID definition.
 *   - The first jam() call comes before the JAM_FILE_ID definition.
 *   - JAM_FILE_ID is #undef'ed by an include file included after the definition
 *
 * Symptom: Compiler warning about JAM_FILE_ID being redefined.
 * Causes:
 *   - Missing #undef at the end of include file.
 *   - File included after the JAM_FILE_ID definition.
 *
 * Symptom: Assert failure for for JamEvent::verifyId() or jam trace entries
 *   pointing to the wrong source file.
 * Cause: jamFileNames[JAM_FILE_ID] does not contain the base name of the source
 *   file.
*/

static constexpr const char* const jamFileNames[] =
{
  // BEGIN jamFileNames (This marker is used by ./test_jamFileNames.sh)
  "NodeInfo.hpp",                       // 0
  "NodeState.hpp",                      // 1
  "NodeBitmask.hpp",                    // 2
  "LogLevel.hpp",                       // 3
  "AttributeList.hpp",                  // 4
  "AttributeDescriptor.hpp",            // 5
  "AttributeHeader.hpp",                // 6
  "ConfigChange.hpp",                   // 7
  "CreateIndx.hpp",                     // 8
  "StartInfo.hpp",                      // 9
  "DLC64HashTable.hpp",                 // 10
  "NextScan.hpp",                       // 11
  "DihFragCount.hpp",                   // 12
  "DL64HashTable2.hpp",                 // 13
  "DropTabFile.hpp",                    // 14
  "BuildIndx.hpp",                      // 15
  "TcContinueB.hpp",                    // 16
  "MasterGCP.hpp",                      // 17
  "UtilPrepare.hpp",                    // 18
  "DL64HashTable.hpp",                  // 19
  "TabCommit.hpp",                      // 20
  "LqhTransConf.hpp",                   // 21
  "CallbackSignal.hpp",                 // 22
  "ArbitSignalData.hpp",                // 23
  "FailRep.hpp",                        // 24
  "DropObj.hpp",                        // 25
  "AllocNodeId.hpp",                    // 26
  "LqhKey.hpp",                         // 27
  "CreateNodegroup.hpp",                // 28
  "GetTabInfo.hpp",                     // 29
  "BuildIndxImpl.hpp",                  // 30
  "Sync.hpp",                           // 31
  "RWPool64.hpp",                       // 32
  "CreateIndxImpl.hpp",                 // 33
  "UtilLock.hpp",                       // 34
  "ApiVersion.hpp",                     // 35
  "CreateNodegroupImpl.hpp",            // 36
  "DihAddFrag.hpp",                     // 37
  "LqhTransReq.hpp",                    // 38
  "DataFileOrd.hpp",                    // 39
  "EnableCom.hpp",                      // 40
  "SignalDataPrint.hpp",                // 41
  "SignalDroppedRep.hpp",               // 42
  "ApiBroadcast.hpp",                   // 43
  "LqhFrag.hpp",                        // 44
  "CopyFrag.hpp",                       // 45
  "CreateTab.hpp",                      // 46
  "BackupContinueB.hpp",                // 47
  "MasterLCP.hpp",                      // 48
  "WaitGCP.hpp",                        // 49
  "LocalRouteOrd.hpp",                  // 50
  "StopMe.hpp",                         // 51
  "EventReport.hpp",                    // 52
  "CreateFilegroupImpl.hpp",            // 53
  "LgmanContinueB.hpp",                 // 54
  "ListTables.hpp",                     // 55
  "ScanTab.hpp",                        // 56
  "TupKey.hpp",                         // 57
  "TcKeyConf.hpp",                      // 58
  "NodeFailRep.hpp",                    // 59
  "RouteOrd.hpp",                       // 60
  "SignalData.hpp",                     // 61
  "FsRemoveReq.hpp",                    // 62
  "DropIndxImpl.hpp",                   // 63
  "CreateHashMap.hpp",                  // 64
  "CmRegSignalData.hpp",                // 65
  NULL,                                 // 66
  "StartFragReq.hpp",                   // 67
  "DictTakeover.hpp",                   // 68
  "FireTrigOrd.hpp",                    // 69
  "BuildFK.hpp",                        // 70
  "DropTrig.hpp",                       // 71
  "AlterTab.hpp",                       // 72
  "PackedSignal.hpp",                   // 73
  "DropNodegroup.hpp",                  // 74
  "ReadConfig.hpp",                     // 75
  "InvalidateNodeLCPReq.hpp",           // 76
  "ApiRegSignalData.hpp",               // 77
  "BackupImpl.hpp",                     // 78
  "SumaImpl.hpp",                       // 79
  "CreateFragmentation.hpp",            // 80
  "AlterIndx.hpp",                      // 81
  "BackupLockTab.hpp",                  // 82
  "DihGetTabInfo.hpp",                  // 83
  "DihRestart.hpp",                     // 84
  "TupCommit.hpp",                      // 85
  "Extent.hpp",                         // 86
  "DictTabInfo.hpp",                    // 87
  "CmvmiCfgConf.hpp",                   // 88
  "BlockCommitOrd.hpp",                 // 89
  "DiGetNodes.hpp",                     // 90
  "Upgrade.hpp",                        // 91
  "ExecFragReq.hpp",                    // 92
  "TcHbRep.hpp",                        // 93
  "TcKeyFailConf.hpp",                  // 94
  "TuxMaint.hpp",                       // 95
  "DihStartTab.hpp",                    // 96
  "GetConfig.hpp",                      // 97
  "CreateFilegroup.hpp",                // 98
  "ReleasePages.hpp",                   // 99
  "CreateTrig.hpp",                     // 100
  "BackupSignalData.hpp",               // 101
  NULL,                                 // 102
  "CreateEvnt.hpp",                     // 103
  "CreateTrigImpl.hpp",                 // 104
  "StartRec.hpp",                       // 105
  "ContinueFragmented.hpp",             // 106
  "CreateObj.hpp",                      // 107
  "DihScanTab.hpp",                     // 108
  NULL,                                 // 109
  "DropFK.hpp",                         // 110
  NULL,                                 // 111
  "AlterTable.hpp",                     // 112
  "DisconnectRep.hpp",                  // 113
  "DihContinueB.hpp",                   // 114
  NULL,                                 // 115
  "AllocMem.hpp",                       // 116
  "TamperOrd.hpp",                      // 117
  "ResumeReq.hpp",                      // 118
  "UtilRelease.hpp",                    // 119
  "DropFKImpl.hpp",                     // 120
  "AccScan.hpp",                        // 121
  "DbinfoScan.hpp",                     // 122
  "SchemaTrans.hpp",                    // 123
  "UtilDelete.hpp",                     // 124
  NULL,                                 // 125
  "DictStart.hpp",                      // 126
  "TcKeyReq.hpp",                       // 127
  "SrFragidConf.hpp",                   // 128
  "QueryTree.hpp",                      // 129
  "NdbfsContinueB.hpp",                 // 130
  "GCP.hpp",                            // 131
  "TcRollbackRep.hpp",                  // 132
  "DictLock.hpp",                       // 133
  "ScanFrag.hpp",                       // 134
  "DropFilegroup.hpp",                  // 135
  "FsAppendReq.hpp",                    // 136
  "DumpStateOrd.hpp",                   // 137
  "DropTab.hpp",                        // 138
  "DictSchemaInfo.hpp",                 // 139
  "RestoreContinueB.hpp",               // 140
  "AbortAll.hpp",                       // 141
  "NdbSttor.hpp",                       // 142
  "DictObjOp.hpp",                      // 143
  "StopPerm.hpp",                       // 144
  "UtilExecute.hpp",                    // 145
  "ConfigParamId.hpp",                  // 146
  "DropIndx.hpp",                       // 147
  "FsOpenReq.hpp",                      // 148
  "DropFilegroupImpl.hpp",              // 149
  "NFCompleteRep.hpp",                  // 150
  "CreateTable.hpp",                    // 151
  "StartMe.hpp",                        // 152
  "AccLock.hpp",                        // 153
  NULL,                                 // 154
  "DbspjErr.hpp",                       // 155
  "FsReadWriteReq.hpp",                 // 156
  NULL,                                 // 157
  "DropNodegroupImpl.hpp",              // 158
  "InvalidateNodeLCPConf.hpp",          // 159
  "PrepFailReqRef.hpp",                 // 160
  "PrepDropTab.hpp",                    // 161
  "KeyInfo.hpp",                        // 162
  "TcCommit.hpp",                       // 163
  "TakeOver.hpp",                       // 164
  "NodeStateSignalData.hpp",            // 165
  "AccFrag.hpp",                        // 166
  "DropTrigImpl.hpp",                   // 167
  "IndxAttrInfo.hpp",                   // 168
  "TuxBound.hpp",                       // 169
  "LCP.hpp",                            // 170
  "StopForCrash.hpp",                   // 171
  "DihSwitchReplica.hpp",               // 172
  "CreateFK.hpp",                       // 173
  "CloseComReqConf.hpp",                // 174
  "CopyActive.hpp",                     // 175
  "DropTable.hpp",                      // 176
  "TcKeyRef.hpp",                       // 177
  "TuxContinueB.hpp",                   // 178
  "PgmanContinueB.hpp",                 // 179
  "SystemError.hpp",                    // 180
  NULL,                                 // 181
  "TsmanContinueB.hpp",                 // 182
  "SetVarReq.hpp",                      // 183
  "StartOrd.hpp",                       // 184
  "AttrInfo.hpp",                       // 185
  "UtilSequence.hpp",                   // 186
  "DictSignal.hpp",                     // 187
  "StopReq.hpp",                        // 188
  "TrigAttrInfo.hpp",                   // 189
  "CheckNodeGroups.hpp",                // 190
  "CntrStart.hpp",                      // 191
  "TransIdAI.hpp",                      // 192
  "IndexStatSignal.hpp",                // 193
  "FsRef.hpp",                          // 194
  "SetLogLevelOrd.hpp",                 // 195
  "TestOrd.hpp",                        // 196
  "TupFrag.hpp",                        // 197
  "RelTabMem.hpp",                      // 198
  "ReadNodesConf.hpp",                  // 199
  "HashMapImpl.hpp",                    // 200
  "CopyData.hpp",                       // 201
  "FsCloseReq.hpp",                     // 202
  "IndxKeyInfo.hpp",                    // 203
  "StartPerm.hpp",                      // 204
  "SchemaTransImpl.hpp",                // 205
  "FsConf.hpp",                         // 206
  "BuildFKImpl.hpp",                    // 207
  "CreateFKImpl.hpp",                   // 208
  "AlterIndxImpl.hpp",                  // 209
  "DiAddTab.hpp",                       // 210
  "TcIndx.hpp",                         // 211
  "CopyGCIReq.hpp",                     // 212
  "NodePing.hpp",                       // 213
  "RestoreImpl.hpp",                    // 214
  "Interpreter.hpp",                    // 215
  "statedesc.hpp",                      // 216
  "RefConvert.hpp",                     // 217
  "ndbd.hpp",                           // 218
  "SectionReader.hpp",                  // 219
  "SafeMutex.hpp",                      // 220
  "KeyTable.hpp",                       // 221
  "dummy_nonmt.cpp",                    // 222
  "Callback.hpp",                       // 223
  "SimplePropertiesSection.cpp",        // 224
  "test.cpp",                           // 225
  "TransporterCallback.cpp",            // 226
  "Array.hpp",                          // 227
  "LongSignalImpl.hpp",                 // 228
  "dummy_mt.cpp",                       // 229
  "Ndbinfo.hpp",                        // 230
  "ThreadConfig.hpp",                   // 231
  "SimplePropertiesSection_nonmt.cpp",  // 232
  "DynArr256.cpp",                      // 233
  "ndbd_malloc.hpp",                    // 234
  "WatchDog.cpp",                       // 235
  "mt.cpp",                             // 236
  "testLongSig.cpp",                    // 237
  "TransporterCallback_mt.cpp",         // 238
  "NdbinfoTables.cpp",                  // 239
  NULL,                                 // 240
  "LongSignal_nonmt.cpp",               // 241
  "FastScheduler.cpp",                  // 242
  "TransporterCallback_nonmt.cpp",      // 243
  "FastScheduler.hpp",                  // 244
  "CountingSemaphore.hpp",              // 245
  NULL,                                 // 246
  "TimeQueue.hpp",                      // 247
  "SimulatedBlock.hpp",                 // 248
  "IntrusiveList.cpp",                  // 249
  "test_context.cpp",                   // 250
  "NdbSeqLock.hpp",                     // 251
  "SimulatedBlock.cpp",                 // 252
  "WatchDog.hpp",                       // 253
  "SimplePropertiesSection_mt.cpp",     // 254
  "Pool.cpp",                           // 255
  "ClusterConfiguration.cpp",           // 256
  "DLCHashTable.hpp",                   // 257
  "KeyTable2.hpp",                      // 258
  "KeyDescriptor.hpp",                  // 259
  "Emulator.hpp",                       // 260
  "LHLevel.hpp",                        // 261
  "LongSignal.cpp",                     // 262
  "ThreadConfig.cpp",                   // 263
  NULL,                                 // 264
  "SafeMutex.cpp",                      // 265
  "SafeCounter.cpp",                    // 266
  NULL,                                 // 267
  NULL,                                 // 268
  "Mutex.hpp",                          // 269
  NULL,                                 // 270
  "CArray.hpp",                         // 271
  "mt_thr_config.hpp",                  // 272
  "TimeQueue.cpp",                      // 273
  "DataBuffer.hpp",                     // 274
  "mt.hpp",                             // 275
  "Configuration.hpp",                  // 276
  "GlobalData.hpp",                     // 277
  NULL,                                 // 278
  "GlobalData.cpp",                     // 279
  "Prio.hpp",                           // 280
  NULL,                                 // 281
  "pc.hpp",                             // 282
  "LockQueue.hpp",                      // 283
  NULL,                                 // 284
  "SimulatedBlock_nonmt.cpp",           // 285
  "SafeCounter.hpp",                    // 286
  "ndbd_malloc.cpp",                    // 287
  "LongSignal.hpp",                     // 288
  "ArenaPool.hpp",                      // 289
  "testDataBuffer.cpp",                 // 290
  "ndbd_malloc_impl.hpp",               // 291
  "ArrayPool.hpp",                      // 292
  "Mutex.cpp",                          // 293
  NULL,                                 // 294
  "test_context.hpp",                   // 295
  "ndbd_malloc_impl.cpp",               // 296
  "mt_thr_config.cpp",                  // 297
  "IntrusiveList.hpp",                  // 298
  "DynArr256.hpp",                      // 299
  "LongSignal_mt.cpp",                  // 300
  "Configuration.cpp",                  // 301
  NULL,                                 // 302
  NULL,                                 // 303
  "CountingPool.cpp",                   // 304
  "TransporterCallbackKernel.hpp",      // 305
  NULL,                                 // 306
  "DLHashTable2.hpp",                   // 307
  "VMSignal.cpp",                       // 308
  "ArenaPool.cpp",                      // 309
  "LHLevel.cpp",                        // 310
  "RWPool.hpp",                         // 311
  "mt-send-t.cpp",                      // 312
  "DLHashTable.hpp",                    // 313
  "VMSignal.hpp",                       // 314
  "Pool.hpp",                           // 315
  "Rope.hpp",                           // 316
  NULL,                                 // 317
  "LockQueue.cpp",                      // 318
  NULL,                                 // 319
  NULL,                                 // 320
  NULL,                                 // 321
  "SimBlockList.hpp",                   // 322
  "mt-lock.hpp",                        // 323
  "rr.cpp",                             // 324 DELETED FILE
  "testCopy.cpp",                       // 325
  "Ndbinfo.cpp",                        // 326
  "SectionReader.cpp",                  // 327
  "RequestTracker.hpp",                 // 328
  "Emulator.cpp",                       // 329
  "Rope.cpp",                           // 330
  "SimulatedBlock_mt.cpp",              // 331
  "CountingPool.hpp",                   // 332
  "angel.cpp",                          // 333
  "trpman.hpp",                         // 334
  "pgman.cpp",                          // 335
  "RestoreProxy.cpp",                   // 336
  "DbgdmProxy.hpp",                     // 337
  "DbgdmProxy.cpp",                     // 338
  "lgman.hpp",                          // 339
  "thrman.hpp",                         // 340
  "DbaccProxy.cpp",                     // 341
  "Container.hpp",                      // 342
  "DbaccProxy.hpp",                     // 343
  "Dbacc.hpp",                          // 344
  "DbaccMain.cpp",                      // 345
  "DbaccInit.cpp",                      // 346
  "record_types.hpp",                   // 347
  "DbtcProxy.hpp",                      // 348
  "DbtcInit.cpp",                       // 349
  "Dbtc.hpp",                           // 350
  "DbtcStateDesc.cpp",                  // 351
  "DbtcProxy.cpp",                      // 352
  "DbtcMain.cpp",                       // 353
  "DbdihMain.cpp",                      // 354
  "DbdihInit.cpp",                      // 355
  "Dbdih.hpp",                          // 356
  "Sysfile.hpp",                        // 357
  "printSysfile.cpp",                   // 358
  "tsman.cpp",                          // 359
  "QmgrMain.cpp",                       // 360
  "QmgrInit.cpp",                       // 361
  "Qmgr.hpp",                           // 362
  NULL,                                 // 363
  "diskpage.cpp",                       // 364
  "DbtuxGen.cpp",                       // 365
  "DbtuxDebug.cpp",                     // 366
  "DbtuxStat.cpp",                      // 367
  "DbtuxSearch.cpp",                    // 368
  "DbtuxMaint.cpp",                     // 369
  "DbtuxProxy.cpp",                     // 370
  "DbtuxScan.cpp",                      // 371
  "DbtuxNode.cpp",                      // 372
  "DbtuxBuild.cpp",                     // 373
  "Dbtux.hpp",                          // 374
  "DbtuxTree.cpp",                      // 375
  "DbtuxProxy.hpp",                     // 376
  "DbtuxMeta.cpp",                      // 377
  "DbtuxCmp.cpp",                       // 378 DELETED FILE
  "Cmvmi.hpp",                          // 379
  "Cmvmi.cpp",                          // 380
  "AsyncIoThread.hpp",                  // 381
  NULL,                                 // 382
  "Filename.cpp",                       // 383
  "PosixAsyncFile.cpp",                 // 384
  "Ndbfs.hpp",                          // 385
  "OpenFiles.hpp",                      // 386
  "AsyncFile.cpp",                      // 387
  "AsyncIoThread.cpp",                  // 388
  NULL,                                 // 389
  "MemoryChannel.cpp",                  // 390 DELETED FILE
  "AsyncFile.hpp",                      // 391
  "Filename.hpp",                       // 392
  "Ndbfs.cpp",                          // 393
  "VoidFs.cpp",                         // 394
  "Win32AsyncFile.hpp",                 // 395
  "MemoryChannel.hpp",                  // 396
  "PosixAsyncFile.hpp",                 // 397
  "Pool.hpp",                           // 398
  "Win32AsyncFile.cpp",                 // 399
  "DbUtil.cpp",                         // 400
  "DbUtil.hpp",                         // 401
  "DbtupRoutines.cpp",                  // 402
  "DbtupProxy.hpp",                     // 403
  "Undo_buffer.hpp",                    // 404
  "DbtupVarAlloc.cpp",                  // 405
  "DbtupStoredProcDef.cpp",             // 406
  "DbtupPagMan.cpp",                    // 407
  "DbtupScan.cpp",                      // 408
  "DbtupAbort.cpp",                     // 409
  "DbtupBuffer.cpp",                    // 410
  "DbtupDebug.cpp",                     // 411
  "DbtupTabDesMan.cpp",                 // 412
  "DbtupProxy.cpp",                     // 413
  "Dbtup.hpp",                          // 414
  "DbtupPageMap.cpp",                   // 415
  "DbtupCommit.cpp",                    // 416
  "DbtupClient.cpp",                    // 417
  "DbtupIndex.cpp",                     // 418
  "tuppage.hpp",                        // 419
  "DbtupGen.cpp",                       // 420
  "DbtupFixAlloc.cpp",                  // 421
  "DbtupExecQuery.cpp",                 // 422
  "DbtupTrigger.cpp",                   // 423
  "DbtupMeta.cpp",                      // 424
  "AttributeOffset.hpp",                // 425
  "DbtupDiskAlloc.cpp",                 // 426
  "tuppage.cpp",                        // 427
  "test_varpage.cpp",                   // 428
  "Undo_buffer.cpp",                    // 429
  "trpman.cpp",                         // 430
  "print_file.cpp",                     // 431
  "Trix.hpp",                           // 432
  "Trix.cpp",                           // 433
  "PgmanProxy.hpp",                     // 434
  "RestoreProxy.hpp",                   // 435
  "diskpage.hpp",                       // 436
  "LocalProxy.cpp",                     // 437
  "LocalProxy.hpp",                     // 438
  "restore.hpp",                        // 439
  "thrman.cpp",                         // 440
  "lgman.cpp",                          // 441
  "DblqhProxy.cpp",                     // 442
  "DblqhCommon.hpp",                    // 443
  "DblqhCommon.cpp",                    // 444
  "DblqhProxy.hpp",                     // 445
  "DblqhStateDesc.cpp",                 // 446
  NULL,                                 // 447
  "records.cpp",                        // 448
  "reader.cpp",                         // 449
  "Dblqh.hpp",                          // 450
  "DblqhMain.cpp",                      // 451
  "DblqhInit.cpp",                      // 452
  "restore.cpp",                        // 453
  "Dbinfo.hpp",                         // 454
  "Dbinfo.cpp",                         // 455
  "tsman.hpp",                          // 456
  "Ndbcntr.hpp",                        // 457
  "NdbcntrMain.cpp",                    // 458
  "NdbcntrInit.cpp",                    // 459
  "NdbcntrSysTable.cpp",                // 460
  "mutexes.hpp",                        // 461
  "pgman.hpp",                          // 462
  "printSchemaFile.cpp",                // 463
  "Dbdict.hpp",                         // 464
  "Dbdict.cpp",                         // 465
  "SchemaFile.hpp",                     // 466
  "Suma.cpp",                           // 467
  "SumaInit.cpp",                       // 468
  "Suma.hpp",                           // 469
  "PgmanProxy.cpp",                     // 470
  "BackupProxy.cpp",                    // 471
  "BackupInit.cpp",                     // 472
  "BackupFormat.hpp",                   // 473
  "Backup.hpp",                         // 474
  "Backup.cpp",                         // 475
  "read.cpp",                           // 476
  "FsBuffer.hpp",                       // 477
  "BackupProxy.hpp",                    // 478
  "DbspjMain.cpp",                      // 479
  "DbspjProxy.hpp",                     // 480
  "Dbspj.hpp",                          // 481
  "DbspjInit.cpp",                      // 482
  "DbspjProxy.cpp",                     // 483
  "ndbd.cpp",                           // 484
  "main.cpp",                           // 485
  "TimeModule.hpp",                     // 486
  "ErrorReporter.hpp",                  // 487
  "TimeModule.cpp",                     // 488
  "ErrorHandlingMacros.hpp",            // 489
  "ErrorReporter.cpp",                  // 490
  "angel.hpp",                          // 491
  "SimBlockList.cpp",                   // 492
  "CopyTab.hpp",                        // 493
  "IsolateOrd.hpp",                     // 494
  "IsolateOrd.cpp",                     // 495
  "SegmentList.hpp",                    // 496
  "SegmentList.cpp",                    // 497
  "LocalSysfile.hpp",                   // 498
  "UndoLogLevel.hpp",                   // 499
  "RedoStateRep.hpp",                   // 500
  "printFragfile.cpp",                  // 501
  "TransientPagePool.hpp",              // 502
  "TransientPagePool.cpp",              // 503
  "TransientSlotPool.hpp",              // 504
  "TransientSlotPool.cpp",              // 505
  "TransientPool.hpp",                  // 506
  "Slot.hpp",                           // 507
  "StaticSlotPool.hpp",                 // 508
  "StaticSlotPool.cpp",                 // 509
  "ComposedSlotPool.hpp",               // 510
  "IntrusiveTags.hpp",                  // 511
  "Sysfile.cpp",                        // 512
  "BlockThreadBitmask.hpp",             // 513
  "SyncThreadViaReqConf.hpp",           // 514
  "TakeOverTcConf.hpp",                 // 515
  "GetNumMultiTrp.hpp",                 // 516
  "Dbqlqh.hpp",                         // 517
  "Dbqlqh.cpp",                         // 518
  "DbqlqhProxy.hpp",                    // 519
  "DbqlqhProxy.cpp",                    // 520
  "Dbqacc.hpp",                         // 521
  "Dbqacc.cpp",                         // 522
  "DbqaccProxy.hpp",                    // 523
  "DbqaccProxy.cpp",                    // 524
  "Dbqtup.hpp",                         // 525
  "Dbqtup.cpp",                         // 526
  "DbqtupProxy.hpp",                    // 527
  "DbqtupProxy.cpp",                    // 528
  "Dbqtux.hpp",                         // 529
  "Dbqtux.cpp",                         // 530
  "DbqtuxProxy.hpp",                    // 531
  "DbqtuxProxy.cpp",                    // 532
  "QBackup.hpp",                        // 533
  "QBackup.cpp",                        // 534
  "QBackupProxy.hpp",                   // 535
  "QBackupProxy.cpp",                   // 536
  "QRestore.hpp",                       // 537
  "QRestore.cpp",                       // 538
  "QRestoreProxy.hpp",                  // 539
  "QRestoreProxy.cpp",                  // 540
  "Activate.hpp",                       // 541
  "SetHostname.hpp",                    // 542
  "Intrusive64List.cpp",                // 543
  "Intrusive64List.hpp",                // 544
  "Abort.hpp",                          // 545
  "TrpKeepAlive.hpp",                   // 546
  "TrpKeepAlive.cpp",                   // 547
  "CommitReq.hpp",                      // 548
  "CommitReq.cpp",                      // 549
  // END jamFileNames (This marker is used by ./test_jamFileNames.sh)
};

// Return upper-case character
constexpr char staticUpperCaseChar(const char ch)
{
  if ('a' <= ch && ch <= 'z')
  {
    return ch & 0xdf;
  }
  return ch;
}

// Return true if strings are equal, ignoring case differences
constexpr bool staticCompareStringsIgnoreCase(const char* first,
                                              const char* second)
{
  for (int i = 0; true; i++)
  {
    if (first[i] == 0)
    {
      return second[i] == 0;
    }
    if (second[i] == 0)
    {
      return false;
    }
    if (staticUpperCaseChar(first[i]) == staticUpperCaseChar(second[i]))
    {
      continue;
    }
    return false;
  }
}

// Return the base name of a file path, i.e. the substring after the last slash
// or backslash
constexpr const char* staticPathBasename(const char* path)
{
  for (int i = 0; true; i++)
  {
    if (path[i] == 0)
    {
      return path;
    }
    if (path[i] == '/' || path[i] == '\\')
    {
      return staticPathBasename(path + i + 1);
    }
  }
}

// Return true if the base name of path equals file, ignoring case differences
constexpr bool staticPathEndsWithFileIgnoreCase(const char* path,
                                                const char* file)
{
  return staticCompareStringsIgnoreCase(staticPathBasename(path), file);
}

/* Tests */
static_assert(staticPathEndsWithFileIgnoreCase("file.ext", "file.ext"));
static_assert(staticPathEndsWithFileIgnoreCase("d1/d2/File.Ext", "File.Ext"));
static_assert(staticPathEndsWithFileIgnoreCase("d1/d2/file.ext", "File.Ext"));
static_assert(staticPathEndsWithFileIgnoreCase("d1/d2/FILE.EXT", "File.Ext"));
static_assert(!staticPathEndsWithFileIgnoreCase("d1/d2/afile.ext", "file.ext"));
static_assert(!staticPathEndsWithFileIgnoreCase("file.ext", "d1/d2/file.ext"));
static_assert(!staticPathEndsWithFileIgnoreCase("d1/file.ext", "d1/file.ext"));

/**
 * JamEvents are used for recording that control passes a given point in the
 * code, reperesented by a JAM_FILE_ID value (which uniquely identifies a
 * source file), and a line number. The reason for using JAM_FILE_ID rather
 * than the predefined __FILE__ is that is faster to store a 14-bit integer
 * than a pointer. For a description of how to maintain and debug JAM_FILE_IDs,
 * please refer to the comments for jamFileNames in Emulator.hpp.
 */
class JamEvent
{
public:
  /**
   * This method is used for verifying that JAM_FILE_IDs matches the contents 
   * of the jamFileNames table. The file name may include directory names,
   * which will be ignored.
   * @returns: true if fileId and pathName matches the jamFileNames table.
   */
  static constexpr bool verifyId(Uint32 fileId, const char* pathName)
  {
    if (fileId >= sizeof jamFileNames/sizeof jamFileNames[0])
    {
      return false;
    }
    else if (jamFileNames[fileId] == NULL)
    {
      return false;
    }
    else
    {
      /** 
       * Check if pathName ends with jamFileNames[fileId]. Observe that the
       * basename() libc function is neither thread safe nor universally
       * portable, therefore it is not used here.
       *
       * With Visual C++, __FILE__ is always in lowercase. Therefore we must use
       * a comparison that ignores case differences.
      */
      return staticPathEndsWithFileIgnoreCase(pathName, jamFileNames[fileId]);
    }
  }

  enum JamEventType {
    STARTOFPACKEDSIG = 0,
    LINE = 1,
    DATA = 2,
    STARTOFSIG = 3,
    EMPTY = 4,
  };

  inline JamEventType getType() const
  {
    if (m_jamVal == 0x7fffffff)
    {
      return EMPTY;
    }
    else
    {
      return (JamEventType)(m_jamVal >> 30);
    }
  }

  // Constructor for EMPTY JamEvent
  constexpr explicit JamEvent()
    :m_jamVal(0x7fffffff){}

  // Constructor for DATA and LINE JamEvent
  constexpr explicit JamEvent(Uint32 fileId, Uint16 lineNoOrData, bool isLineNumber)
    :m_jamVal(((isLineNumber ? 1 << 30 : 1 << 31) | (fileId << 16))
              | lineNoOrData){}

  // Constructor for STARTOFSIG JamEvent
  constexpr explicit JamEvent(Uint32 theSignalId)
    :m_jamVal(theSignalId | 0xc0000000){}

  // Constructor for STARTOFPACKEDSIG JamEvent
  constexpr explicit JamEvent(Uint32 theSignalId, Uint32 thePackedIndex)
    // Fully correct it should be
    // ((theSignalId & 0x01ffffff) | ((thePackedIndex & 0x1f) << 25))
    // but thePackedIndex should never exceed 25, so `& 0x1f` is unnecessary.
    :m_jamVal((theSignalId & 0x01ffffff) | (thePackedIndex << 25)){}

  Uint32 getFileId() const
  {
    return (m_jamVal >> 16) & 0x3fff;
  }

  // Get the name of the source file, or NULL if unknown.
  const char* getFileName() const;

  Uint16 getLineNoOrData() const
  {
    return m_jamVal & 0xffff;
  }

  // STARTOFSIG events store the 30 lowest bits of the Signal Id.
  // STARTOFPACKEDSIG events store the 25 lowest bits of the Signal Id.
  Uint32 getSignalId() const
  {
    if (getType() == STARTOFSIG)
    {
      return m_jamVal & (0x3fffffff);
    }
    if (getType() == STARTOFPACKEDSIG)
    {
      return m_jamVal & (0x01ffffff);
    }
    return 0;
  }

  Uint32 getPackedIndex() const
  {
    return (m_jamVal >> 25) & 0x1f;
  }

private:
  /*
       Type-->| LINE        | DATA    | STARTOFSIG | STARTOFPACKEDSIG | EMPTY
    ----------+-------------+---------+------------+------------------+-------
    Bit  0-15 | Line number | Data    | Signal id  | Signal id        | 0xffff
    Bit 16-24 | File id     | File id | Signal id  | Signal id        | 0x1ff
    Bit 25-29 | File id     | File id | Signal id  | Pack index       | 0x1f
    Bit 30-31 | 1           | 2       | 3          | 0                | 1

    Motivation for choice of bit 30-31:
      LINE:
        Can be anything since the whole entry will be known at compile time.
      DATA:
        Can be anything. Since fileId is known at compile time, compiler
        optimization should make it so that only a bitwise or is required to
        create the entry at runtime.
      STARTOFSIG:
        Choosing to set both bits lets us create the entry at runtime with only
        a bitwise or.
      STARTOFPACKEDSIG:
        Choosing to leave both bits low minimizes the operations necessary to
        create this type of entry at runtime.
      EMPTY:
        Can be anything, since the whole entry is a constant. The jam macros
        assert that (fileId != 0x3fff) so that the Empty and Line number types
        will not collide.
  */
  Uint32 m_jamVal;
};

/***
 * This is a ring buffer of JamEvents for a thread.
 */
struct EmulatedJamBuffer
{
  // Index of the next entry.
  Uint32 theEmulatedJamIndex;
  JamEvent theEmulatedJam[EMULATED_JAM_SIZE];

  inline void insertJamEvent(const JamEvent event)
  {
    theEmulatedJam[theEmulatedJamIndex] = event;
    theEmulatedJamIndex = (theEmulatedJamIndex + 1) & JAM_MASK;
  }

  // Insert a JamEvent marking the start of a signal.
  inline void markStartOfSigExec(Uint32 theSignalId)
  {
    insertJamEvent(JamEvent(theSignalId));
  }

  // Insert a JamEvent marking the start of a packed signal.
  inline void markStartOfPackedSigExec(Uint32 theSignalId, Uint32 packedIndex)
  {
    insertJamEvent(JamEvent(theSignalId, packedIndex));
  }
};

struct EmulatorData {
  class Configuration * theConfiguration;
  class WatchDog      * theWatchDog;
  class ThreadConfig  * theThreadConfig;
  class SimBlockList  * theSimBlockList;
  class SocketServer  * m_socket_server;
  class Ndbd_mem_manager * m_mem_manager;

  /**
   * Constructor
   *
   *  Sets all the pointers to NULL
   */
  EmulatorData();
  
  /**
   * Create all the objects
   */
  void create();
  
  /**
   * Destroys all the objects
   */
  void destroy();
};

extern struct EmulatorData globalEmulatorData;

/**
 * Compute no of pages to be used as job-buffer
 */
Uint32 compute_jb_pages(struct EmulatorData* ed);


#undef JAM_FILE_ID

#endif 
