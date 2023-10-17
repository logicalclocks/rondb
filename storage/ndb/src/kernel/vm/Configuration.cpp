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

#include "util/require.h"
#include <ndb_global.h>
#include "ndb_config.h"

#include <TransporterRegistry.hpp>
#include "Configuration.hpp"
#include <ErrorHandlingMacros.hpp>
#include "GlobalData.hpp"
#include "portlib/NdbTCP.h"
#include "transporter/TransporterCallback.hpp"

#include <ConfigRetriever.hpp>
#include <IPCConfig.hpp>
#include <ndb_version.h>
#include <NdbOut.hpp>
#include <WatchDog.hpp>
#include <NdbConfig.h>
#include <NdbSpin.h>

#include <mgmapi_configuration.hpp>
#include <kernel_config_parameters.h>

#include <util/ConfigValues.hpp>
#include <NdbEnv.h>

#include "vm/SimBlockList.hpp"
#include <ndbapi_limits.h>
#include "mt.hpp"
#include <dblqh/Dblqh.hpp>
#include <dbacc/Dbacc.hpp>
#include <dbtup/Dbtup.hpp>
#include <dbtux/Dbtux.hpp>
#include <dbtc/Dbtc.hpp>
#include <dbspj/Dbspj.hpp>
#include <dbdih/Dbdih.hpp>
#include <dbdict/Dbdict.hpp>
#include <backup/Backup.hpp>
#include <suma/Suma.hpp>
#include <pgman.hpp>
#include <KeyDescriptor.hpp>


#include "../../common/util/parse_mask.hpp"

#include <EventLogger.hpp>

#define JAM_FILE_ID 301


extern Uint32 g_start_type;

NdbNodeBitmask g_nowait_nodes;
NodeBitmask g_not_active_nodes;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_AUTOMATIC_MEMORY 1
#endif

#ifdef DEBUG_AUTOMATIC_MEMORY
#define DEB_AUTOMATIC_MEMORY(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AUTOMATIC_MEMORY(arglist) do { } while (0)
#endif

#define DEFAULT_TC_RESERVED_CONNECT_RECORD 8192

bool
Configuration::init(int _no_start, int _initial,
                    int _initialstart)
{
  // Check the start flag
  if (_no_start)
    globalData.theRestartFlag = initial_state;
  else 
    globalData.theRestartFlag = perform_start;

  // Check the initial flag
  if (_initial)
    _initialStart = true;

  globalData.ownId= 0;

  if (_initialstart)
  {
    _initialStart = true;
    g_start_type |= (1 << NodeState::ST_INITIAL_START);
  }

  threadIdMutex = NdbMutex_Create();
  if (!threadIdMutex)
  {
    g_eventLogger->error("Failed to create threadIdMutex");
    return false;
  }
  initThreadArray();
  return true;
}

Configuration::Configuration()
{
  _fsPath = 0;
  _backupPath = 0;
  _initialStart = false;
  m_config_retriever= 0;
  m_logLevel= 0;
}

Configuration::~Configuration(){

  if(_fsPath != NULL)
    free(_fsPath);

  if(_backupPath != NULL)
    free(_backupPath);

  if (m_config_retriever) {
    delete m_config_retriever;
  }

  if(m_logLevel) {
    delete m_logLevel;
  }
  ndb_mgm_destroy_iterator(m_clusterConfigIter);
}

void
Configuration::closeConfiguration(bool end_session){
  m_config_retriever->end_session(end_session);
  if (m_config_retriever) {
    delete m_config_retriever;
  }
  m_config_retriever= 0;
}

void
Configuration::fetch_configuration(const char* _connect_string,
                                   int force_nodeid,
                                   const char* _bind_address,
                                   NodeId allocated_nodeid,
                                   int connect_retries, int connect_delay)
{
  /**
   * Fetch configuration from management server
   */
  if (m_config_retriever) {
    delete m_config_retriever;
  }

  m_config_retriever= new ConfigRetriever(_connect_string,
                                          force_nodeid,
                                          NDB_VERSION,
                                          NDB_MGM_NODE_TYPE_NDB,
					  _bind_address);
  if (!m_config_retriever)
  {
    ERROR_SET(fatal, NDBD_EXIT_MEMALLOC,
              "Failed to create ConfigRetriever", "");
  }

  if (m_config_retriever->hasError())
  {
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
	      "Could not initialize handle to management server",
	      m_config_retriever->getErrorString());
  }

  if(m_config_retriever->do_connect(connect_retries, connect_delay, 1) == -1){
    const char * s = m_config_retriever->getErrorString();
    if(s == 0)
      s = "No error given!";
    /* Set stop on error to true otherwise NDB will
       go into an restart loop...
    */
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Could not connect to ndb_mgmd", s);
  }

  if (allocated_nodeid)
  {
    // The angel has already allocated the nodeid, no need to
    // allocate it
    globalData.ownId = allocated_nodeid;
  }
  else
  {

    const int alloc_retries = 10;
    const int alloc_delay = 3;
    globalData.ownId =
        m_config_retriever->allocNodeId(alloc_retries, alloc_delay);
    if(globalData.ownId == 0)
    {
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
                "Unable to alloc node id",
                m_config_retriever->getErrorString());
    }
  }
  assert(globalData.ownId);

  m_clusterConfig = m_config_retriever->getConfig(globalData.ownId);
  if(!m_clusterConfig){
    const char * s = m_config_retriever->getErrorString();
    if(s == 0)
      s = "No error given!";
    
    /* Set stop on error to true otherwise NDB will
       go into an restart loop...
    */
    
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Could not fetch configuration"
	      "/invalid configuration", s);
  }

  const ConfigValues& cfg = m_clusterConfig.get()->m_config_values;
  cfg.pack_v1(m_clusterConfigPacked_v1);
  if (OUR_V2_VERSION)
  {
    cfg.pack_v2(m_clusterConfigPacked_v2);
  }

  {
    Uint32 generation;
    ndb_mgm_configuration_iterator sys_iter(m_clusterConfig.get(),
                                            CFG_SECTION_SYSTEM);
    char sockaddr_buf[512];
    char* sockaddr_string =
        Ndb_combine_address_port(sockaddr_buf, sizeof(sockaddr_buf),
                                 m_config_retriever->get_mgmd_host(),
                                 m_config_retriever->get_mgmd_port());

    if (sys_iter.get(CFG_SYS_CONFIG_GENERATION, &generation))
    {
        g_eventLogger->info("Configuration fetched from '%s', unknown generation!!"
                            " (likely older ndb_mgmd)", sockaddr_string);
    }
    else
    {
        g_eventLogger->info("Configuration fetched from '%s', generation: %d",
                            sockaddr_string, generation);
    }
  }

  ndb_mgm_configuration_iterator iter(m_clusterConfig.get(), CFG_SECTION_NODE);
  if (iter.find(CFG_NODE_ID, globalData.ownId)) {
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched",
              "DB missing");
  }

  if(iter.get(CFG_DB_STOP_ON_ERROR, &_stopOnError)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched", 
	      "StopOnError missing");
  }

  Uint32 use_only_ipv4 = 0;
  iter.get(CFG_TCP_ONLY_IPV4, &use_only_ipv4);
  globalData.theUseOnlyIPv4Flag = use_only_ipv4;

  const char * pidfile_dir;
  if(iter.get(CFG_NODE_PIDFILE_DIR, &pidfile_dir) == 0)
  {
    NdbConfig_SetPidfilePath(pidfile_dir);
    g_eventLogger->debug("Using Directory: %s for pid file", pidfile_dir);
  }

  const char * datadir;
  if(iter.get(CFG_NODE_DATADIR, &datadir)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched",
	      "DataDir missing");
  }
  NdbConfig_SetPath(datadir);

}

static char * get_and_validate_path(ndb_mgm_configuration_iterator &iter,
				    Uint32 param, const char *param_string)
{ 
  const char* path = NULL;
  if(iter.get(param, &path)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched missing ", 
	      param_string);
  } 
  
  if(path == 0 || strlen(path) == 0){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
	      "Invalid configuration fetched. Configuration does not contain valid ",
	      param_string);
  }
  
  // check that it is pointing on a valid directory
  // 
  char buf2[PATH_MAX];
  memset(buf2, 0,sizeof(buf2));
#ifdef _WIN32
  char* szFilePart;
  if(!GetFullPathName(path, sizeof(buf2), buf2, &szFilePart) ||
     (GetFileAttributes(buf2) & FILE_ATTRIBUTE_READONLY))
#else
  if((::realpath(path, buf2) == NULL)||
       (::access(buf2, W_OK) != 0))
#endif
  {
    ERROR_SET(fatal, NDBD_EXIT_AFS_INVALIDPATH, path, param_string);
  }
  
  if (strcmp(&buf2[strlen(buf2) - 1], DIR_SEPARATOR))
    strcat(buf2, DIR_SEPARATOR);
  
  return strdup(buf2);
}

Uint32
Configuration::get_num_threads()
{
  Uint32 num_ldm_threads = globalData.ndbMtLqhThreads;
  Uint32 num_tc_threads = globalData.ndbMtTcThreads;
  Uint32 num_main_threads = globalData.ndbMtMainThreads;
  Uint32 num_recv_threads = globalData.ndbMtReceiveThreads;
  return num_ldm_threads +
         num_tc_threads +
         num_main_threads +
         num_recv_threads;
}

Uint64
Configuration::get_total_memory(const ndb_mgm_configuration_iterator *p,
                                bool &total_memory_set)
{
  Uint64 total_memory_size = 0;
  ndb_mgm_get_int64_parameter(p,
                              CFG_DB_TOTAL_MEMORY_CONFIG,
                              &total_memory_size);
  if (total_memory_size == 0)
  {
    struct ndb_hwinfo *hwinfo = Ndb_GetHWInfo(false);
    return hwinfo->hw_memory_size;
    total_memory_set = false;
  }
  else
  {
    total_memory_set = true;
    return total_memory_size;
  }
}

void
Configuration::set_not_active_nodes()
{
  const char * msg = "Invalid configuration fetched";
  char buf[255];
  ndb_mgm_configuration_iterator * p = m_clusterConfigIter;

  g_eventLogger->info("Set not active nodes");
  Uint32 nodeNo = 0;
  NodeBitmask nodes;
  for(ndb_mgm_first(p); ndb_mgm_valid(p); ndb_mgm_next(p), nodeNo++)
  {
    Uint32 nodeId;
    Uint32 nodeType;
    
    if(ndb_mgm_get_int_parameter(p, CFG_NODE_ID, &nodeId)){
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg,
                "Node data (Id) missing");
    }
    
    if(ndb_mgm_get_int_parameter(p, CFG_TYPE_OF_SECTION, &nodeType)){
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg,
                "Node data (Type) missing");
    }
    
    if(nodeId > MAX_NODES || nodeId == 0){
      BaseString::snprintf(buf, sizeof(buf),
	       "Invalid node id: %d", nodeId);
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
    }
    
    if(nodes.get(nodeId)){
      BaseString::snprintf(buf, sizeof(buf),
                           "Two nodes can not have the same node id: %d",
	                   nodeId);
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
    }
    nodes.set(nodeId);
        
    switch(nodeType){
    case NODE_TYPE_DB:
      if(nodeId > MAX_NDB_NODES){
		  BaseString::snprintf(buf, sizeof(buf),
                  "Maximum node id for a ndb node is: %d", 
		 MAX_NDB_NODES);
	ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
      }
      break;
    case NODE_TYPE_API:
      break;
    case NODE_TYPE_MGM:
      break;
    default:
      BaseString::snprintf(buf, sizeof(buf),
                           "Unknown node type: %d", nodeType);
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
    }
    Uint32 active_node = 1;
    ndb_mgm_get_int_parameter(p, CFG_NODE_ACTIVE, &active_node);
    if (active_node == 0)
    {
      g_not_active_nodes.set(nodeId);
      if (nodeType == NODE_TYPE_DB)
        g_nowait_nodes.set(nodeId);
      globalTransporterRegistry.set_active_node(nodeId, 0, true);
    }
  }
}

void
Configuration::get_num_nodes(Uint32 &noOfNodes,
                             Uint32 &noOfDBNodes,
                             Uint32 &noOfAPINodes,
                             Uint32 &noOfMGMNodes)
{
  const char * msg = "Invalid configuration fetched";
  char buf[255];
  ndb_mgm_configuration_iterator * p = m_clusterConfigIter;

  Uint32 nodeNo = 0;
  NodeBitmask nodes;
  for(ndb_mgm_first(p); ndb_mgm_valid(p); ndb_mgm_next(p), nodeNo++){
    
    Uint32 nodeId;
    Uint32 nodeType;
    
    if(ndb_mgm_get_int_parameter(p, CFG_NODE_ID, &nodeId)){
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg,
                "Node data (Id) missing");
    }
    
    if(ndb_mgm_get_int_parameter(p, CFG_TYPE_OF_SECTION, &nodeType)){
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg,
                "Node data (Type) missing");
    }
    
    if(nodeId > MAX_NODES || nodeId == 0){
      BaseString::snprintf(buf, sizeof(buf),
	       "Invalid node id: %d", nodeId);
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
    }
    
    if(nodes.get(nodeId)){
      BaseString::snprintf(buf, sizeof(buf),
                           "Two node can not have the same node id: %d",
	                   nodeId);
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
    }
    nodes.set(nodeId);
        
    switch(nodeType){
    case NODE_TYPE_DB:
      noOfDBNodes++; // No of NDB processes
      
      if(nodeId > MAX_NDB_NODES){
		  BaseString::snprintf(buf, sizeof(buf),
                  "Maximum node id for a ndb node is: %d", 
		 MAX_NDB_NODES);
	ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
      }
      break;
    case NODE_TYPE_API:
      noOfAPINodes++; // No of API processes
      break;
    case NODE_TYPE_MGM:
      noOfMGMNodes++; // No of MGM processes
      break;
    default:
      BaseString::snprintf(buf, sizeof(buf),
                           "Unknown node type: %d", nodeType);
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
    }
  }
  noOfNodes = nodeNo;
}

Uint64
Configuration::get_schema_memory(ndb_mgm_configuration_iterator *p)
{
  Uint64 schema_memory = 0;
  ndb_mgm_get_int64_parameter(p,
                              CFG_DB_SCHEMA_MEM,
                              &schema_memory);
  if (schema_memory != 0)
  {
    globalData.theSchemaMemory = schema_memory;
    return schema_memory;
  }
  Uint32 num_replicas = 2;
  ndb_mgm_get_int_parameter(p, CFG_DB_NO_REPLICAS, &num_replicas);
  Uint32 num_fragments = 0;
  ndb_mgm_get_int_parameter(p, CFG_LQH_FRAG, &num_fragments);
  Uint32 num_tot_fragments = 0;
  ndb_mgm_get_int_parameter(p, CFG_DIH_FRAG_CONNECT, &num_tot_fragments);
  Uint32 num_triggers = globalData.theMaxNoOfTriggers;
  Uint32 num_attributes = globalData.theMaxNoOfAttributes;
  Uint32 num_tables = globalData.theMaxNoOfTables;
  Uint32 num_ordered_indexes = globalData.theMaxNoOfOrderedIndexes;
  Uint32 num_unique_hash_indexes = globalData.theMaxNoOfUniqueHashIndexes;

  Uint32 partitions_per_node = 2;
  ndb_mgm_get_int_parameter(p,
                            CFG_DB_PARTITIONS_PER_NODE,
                            &partitions_per_node);

  Uint64 num_table_objects = Uint64(num_tables) + Uint64(2) +
                             Uint64(num_ordered_indexes) +
                             Uint64(num_unique_hash_indexes);
  Uint64 num_replica_records =
    num_table_objects * (partitions_per_node + 1) * num_replicas;
  Uint64 num_ldm_threads = Uint64(globalData.ndbMtLqhWorkers);
  Uint64 num_tc_threads = Uint64(globalData.ndbMtTcWorkers);

  DEB_AUTOMATIC_MEMORY(("num_table_objects: %llu, num_attributes: %u",
                        num_table_objects,
                        num_attributes));
  DEB_AUTOMATIC_MEMORY(("num_triggers: %u, num_replica_records: %llu",
                        num_triggers,
                        num_replica_records));
  DEB_AUTOMATIC_MEMORY(("num_tables: %u, num_ordered_indexes: %u",
                        num_tables,
                        num_ordered_indexes));
  DEB_AUTOMATIC_MEMORY(("num_unique_hash_indexes: %u, num_fragments: %u",
                        num_unique_hash_indexes,
                        num_fragments));
  DEB_AUTOMATIC_MEMORY(("num_tot_fragments: %u, num_replicas: %u",
                        num_tot_fragments,
                        num_replicas));

  Uint64 dict_attribute_mem =
    Dbdict::getAttributeRecordSize() * num_attributes;
  Uint64 dict_trigger_mem =
    Dbdict::getTriggerRecordSize() * num_triggers;
  Uint64 dict_table_mem =
    Dbdict::getTableRecordSize() * num_table_objects;
  Uint64 dict_obj_mem =
    (Dbdict::getDictObjectRecordSize() + 8) * (num_table_objects + num_triggers);
  Uint64 dict_key_descriptor_mem =
    sizeof(struct KeyDescriptor) * num_table_objects;

  Uint64 dict_mem = dict_attribute_mem +
                    dict_trigger_mem +
                    dict_table_mem +
                    dict_obj_mem +
                    dict_key_descriptor_mem;

  DEB_AUTOMATIC_MEMORY(("DICT Schema Memory %llu MBytes", dict_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("DICT Attribute record size: %zu",
                        Dbdict::getAttributeRecordSize()));
  DEB_AUTOMATIC_MEMORY(("DICT Trigger record size: %zu",
                        Dbdict::getTriggerRecordSize()));
  DEB_AUTOMATIC_MEMORY(("DICT Table record size: %zu",
                        Dbdict::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("DICT Object record size: %zu",
                        Dbdict::getDictObjectRecordSize() + 8));
  DEB_AUTOMATIC_MEMORY(("DICT Key Descriptor size: %zu",
                        sizeof(struct KeyDescriptor)));

  Uint64 acc_table_mem = num_ldm_threads *
    Dbacc::getTableRecordSize() * num_table_objects;
  Uint64 acc_fragment_mem = num_ldm_threads *
    Dbacc::getFragmentRecordSize() * num_fragments;

  Uint64 acc_mem = acc_table_mem + acc_fragment_mem;

  DEB_AUTOMATIC_MEMORY(("ACC Schema Memory %llu MBytes", acc_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("ACC Table record size: %zu",
                        Dbacc::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("ACC Fragment record size: %zu",
                        Dbacc::getFragmentRecordSize()));

  Uint64 lqh_table_mem = num_ldm_threads *
    Dblqh::getTableRecordSize() * num_table_objects;
  Uint64 lqh_fragment_mem = num_ldm_threads *
    Dblqh::getFragmentRecordSize() * num_fragments;

  Uint64 lqh_mem = lqh_table_mem + lqh_fragment_mem;

  DEB_AUTOMATIC_MEMORY(("LQH Schema Memory %llu MBytes", lqh_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("LQH Table record size: %zu",
                        Dblqh::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("LQH Fragment record size: %zu",
                        Dblqh::getFragmentRecordSize()));

  Uint64 tup_table_mem = num_ldm_threads *
    Dbtup::getTableRecordSize() * num_table_objects;
  Uint64 tup_fragment_mem = num_ldm_threads *
    Dbtup::getFragmentRecordSize() * num_fragments;
  Uint64 tup_attribute_mem = num_ldm_threads *
    Dbtup::getAttributeRecordSize() * num_attributes;

  Uint64 tup_mem = tup_table_mem +
                   tup_fragment_mem +
                   tup_attribute_mem;

  DEB_AUTOMATIC_MEMORY(("TUP Schema Memory %llu MBytes", tup_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("TUP Table record size: %zu",
                        Dbtup::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("TUP Fragment record size: %zu",
                        Dbtup::getFragmentRecordSize()));
  DEB_AUTOMATIC_MEMORY(("TUP Attribute record size: %zu",
                        Dbtup::getAttributeRecordSize()));

  Uint64 tux_table_mem = num_ldm_threads *
    Dbtux::getTableRecordSize() * num_table_objects;
  Uint64 tux_fragment_mem = num_ldm_threads *
    Dbtux::getFragmentRecordSize() * num_fragments;
  Uint64 tux_attribute_size =
      (Dbtux::DescHeadSize +
       (4 * Dbtux::KeyTypeSize) +
       (4 * Dbtux::AttributeHeaderSize));
  Uint64 tux_attribute_mem = num_ldm_threads *
    num_table_objects * tux_attribute_size * 4;

  Uint64 tux_mem = tux_table_mem +
                   tux_fragment_mem +
                   tux_attribute_mem;

  DEB_AUTOMATIC_MEMORY(("TUX Schema Memory %llu MBytes", tux_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("TUX Table Memory %llu MBytes",
                        tux_table_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("TUX Fragment Memory %llu MBytes",
                        tux_fragment_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("TUX Attribute Memory %llu MBytes",
                        tux_attribute_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("TUX Table record size: %zu",
                        Dbtux::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("TUX Fragment record size: %zu",
                        Dbtux::getFragmentRecordSize()));
  DEB_AUTOMATIC_MEMORY(("TUX Attribute record size: %llu",
                        tux_attribute_size));

  Uint64 tc_table_mem = num_tc_threads *
    Dbtc::getTableRecordSize() * num_table_objects;
  Uint64 tc_trigger_mem = num_tc_threads *
    Dbtc::getTriggerRecordSize() * num_table_objects;
  Uint64 spj_table_mem = num_tc_threads *
    Dbspj::getTableRecordSize() * num_table_objects;

  Uint64 tc_mem = tc_table_mem + tc_trigger_mem + spj_table_mem;

  DEB_AUTOMATIC_MEMORY(("TC Schema Memory %llu MBytes", tc_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("TC Table record size: %zu",
                        Dbtc::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("TC Trigger record size: %zu",
                        Dbtc::getTriggerRecordSize()));
  DEB_AUTOMATIC_MEMORY(("SPJ Table record size: %zu",
                        Dbspj::getTableRecordSize()));

  Uint64 dih_table_mem =
    Dbdih::getTableRecordSize() * num_table_objects;
  Uint64 dih_fragment_mem =
    Dbdih::getFragmentRecordSize() * num_tot_fragments;
  Uint64 dih_replica_mem =
    Dbdih::getReplicaRecordSize() * num_replica_records;
  Uint64 dih_file_mem =
    Dbdih::getFileRecordSize() * ((2 * num_table_objects) + 2);
  Uint64 dih_page_mem =
    Dbdih::getPageRecordSize() * ZPAGEREC;

  Uint64 dih_mem = dih_table_mem +
                   dih_fragment_mem +
                   dih_replica_mem +
                   dih_file_mem +
                   dih_page_mem;

  DEB_AUTOMATIC_MEMORY(("DIH Schema Memory %llu MBytes", dih_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("DIH Table record size: %zu",
                        Dbdih::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("DIH Fragment record size: %zu",
                        Dbdih::getFragmentRecordSize()));
  DEB_AUTOMATIC_MEMORY(("DIH Replica record size: %zu",
                        Dbdih::getReplicaRecordSize()));
  DEB_AUTOMATIC_MEMORY(("DIH File record size: %zu",
                        Dbdih::getFileRecordSize()));
  DEB_AUTOMATIC_MEMORY(("DIH Page record size: %zu",
                        Dbdih::getPageRecordSize()));
  DEB_AUTOMATIC_MEMORY(("DIH ZPAGEREC: %u", ZPAGEREC));

  Uint64 pgman_table_mem = num_ldm_threads *
    Pgman::getTableRecordSize() * num_table_objects;
  Uint64 pgman_fragment_mem = num_ldm_threads *
    (Pgman::getFragmentRecordSize() + 4) * num_fragments;

  Uint64 pgman_mem = pgman_table_mem + pgman_fragment_mem;

  DEB_AUTOMATIC_MEMORY(("PGMAN Schema Memory %llu MBytes",
                       pgman_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("PGMAN Table record size: %zu",
                        Pgman::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("PGMAN Fragment record size: %zu",
                        Pgman::getFragmentRecordSize()));


  Uint64 suma_table_mem =
    Suma::getTableRecordSize() * num_table_objects;
  Uint32 suma_subscription_mem =
    Suma::getSubscriptionRecordSize() * num_table_objects;
  Uint32 suma_subscriber_mem =
    Suma::getSubscriberRecordSize() * 2 * num_table_objects;
  Uint64 suma_data_buffer_mem =
    Suma::getDataBufferRecordSize() * (num_attributes + 45);

  Uint64 suma_other_mem = suma_subscription_mem +
                          suma_subscriber_mem +
                          suma_data_buffer_mem;
  Uint64 suma_mem = suma_table_mem + suma_other_mem;
                   
  DEB_AUTOMATIC_MEMORY(("SUMA Schema Memory %llu MBytes",
                       suma_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("SUMA Table record size: %zu",
                        Suma::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("SUMA Subscription record size: %zu",
                        Suma::getSubscriptionRecordSize()));
  DEB_AUTOMATIC_MEMORY(("SUMA Subscriber record size: %zu",
                        Suma::getSubscriberRecordSize()));
  DEB_AUTOMATIC_MEMORY(("SUMA DataBuffer record size: %zu",
                        Suma::getDataBufferRecordSize()));

  Uint64 table_mem = dict_table_mem +
                     dict_obj_mem +
                     dict_key_descriptor_mem +
                     acc_table_mem +
                     tup_table_mem +
                     tux_table_mem +
                     tux_attribute_mem +
                     lqh_table_mem +
                     dih_table_mem +
                     dih_file_mem +
                     dih_page_mem +
                     tc_table_mem +
                     spj_table_mem +
                     pgman_table_mem +
                     suma_table_mem;

  DEB_AUTOMATIC_MEMORY(("Table memory %llu MBytes", table_mem / MBYTE64));
  Uint64 fragment_mem = acc_fragment_mem +
                        tup_fragment_mem +
                        lqh_fragment_mem +
                        tux_fragment_mem +
                        dih_fragment_mem +
                        dih_replica_mem +
                        pgman_fragment_mem;
  DEB_AUTOMATIC_MEMORY(("Fragment memory %llu MBytes",
                       fragment_mem / MBYTE64));

  Uint64 attr_mem = dict_attribute_mem +
                    tup_attribute_mem;
  DEB_AUTOMATIC_MEMORY(("Attribute memory %llu MBytes", attr_mem / MBYTE64));

  Uint64 trig_mem = dict_trigger_mem +
                    tc_trigger_mem;
  DEB_AUTOMATIC_MEMORY(("Trigger memory %llu MBytes", trig_mem / MBYTE64));

  Uint64 schema_mem_block = dict_mem +
                            tup_mem +
                            acc_mem +
                            lqh_mem +
                            tux_mem +
                            dih_mem +
                            tc_mem +
                            pgman_mem +
                            suma_mem;
  Uint64 schema_mem_type = table_mem +
                           fragment_mem +
                           trig_mem +
                           attr_mem +
                           suma_other_mem;
                 
  require(schema_mem_block == schema_mem_type);

#define PER_MALLOC_OVERHEAD 16
  Uint64 ldm_fragment_map_size = sizeof(Uint64) +
                                 sizeof(Uint16) +
                                 PER_MALLOC_OVERHEAD;
  ldm_fragment_map_size *= (partitions_per_node * num_replicas);
  ldm_fragment_map_size *= num_table_objects;

  Uint64 dih_fragment_map_size = sizeof(64);
  dih_fragment_map_size *= partitions_per_node;
  dih_fragment_map_size += PER_MALLOC_OVERHEAD;
  dih_fragment_map_size *= num_table_objects;

  Uint64 map_size = ldm_fragment_map_size + dih_fragment_map_size;

  DEB_AUTOMATIC_MEMORY(("Fragment map size %llu MBytes", map_size / MBYTE64));

  schema_memory = schema_mem_block - table_mem;
  schema_memory += map_size;
  globalData.theSchemaMemory = schema_memory;
  return schema_mem_block + map_size;
}

Uint64
Configuration::get_backup_schema_memory(ndb_mgm_configuration_iterator *p)
{
  Uint64 backup_schema_memory = 0;
  ndb_mgm_get_int64_parameter(p,
                              CFG_DB_BACKUP_SCHEMA_MEM,
                              &backup_schema_memory);
  if (backup_schema_memory != 0)
  {
    globalData.theBackupSchemaMemory = backup_schema_memory;
    return backup_schema_memory;
  }
  Uint64 num_ldm_threads = Uint64(globalData.ndbMtLqhWorkers);
  Uint32 num_tables = globalData.theMaxNoOfTables;
  Uint32 num_ordered_indexes = globalData.theMaxNoOfOrderedIndexes;
  Uint32 num_unique_hash_indexes = globalData.theMaxNoOfUniqueHashIndexes;

  Uint32 partitions_per_node = 2;
  ndb_mgm_get_int_parameter(p,
                            CFG_DB_PARTITIONS_PER_NODE,
                            &partitions_per_node);

  Uint64 num_table_objects = Uint64(num_tables) + Uint64(2) +
                             Uint64(num_ordered_indexes) +
                             Uint64(num_unique_hash_indexes);
  Uint32 num_fragments = 0;
  ndb_mgm_get_int_parameter(p, CFG_LQH_FRAG, &num_fragments);
  Uint32 num_tot_fragments = 0;
  ndb_mgm_get_int_parameter(p, CFG_DIH_FRAG_CONNECT, &num_tot_fragments);
  Uint32 num_triggers = globalData.theMaxNoOfTriggers;

  Uint64 backup_table_mem = num_ldm_threads *
    (Backup::getTableRecordSize() + 4) * num_table_objects;
  Uint64 backup_fragment_mem = num_ldm_threads *
    Backup::getFragmentRecordSize() * num_tot_fragments;
  Uint64 backup_trigger_mem = num_ldm_threads *
    Backup::getTriggerRecordSize() * num_table_objects * 3;
  Uint64 backup_delete_lcp_file_mem = num_ldm_threads *
    Backup::getDeleteLcpFileRecordSize() * num_fragments;
  Uint64 tup_trigger_mem = num_ldm_threads *
    Dbtup::getTriggerRecordSize() *
    (num_triggers + (3 * num_table_objects));

  Uint64 backup_mem = backup_table_mem +
                      backup_fragment_mem +
                      backup_trigger_mem +
                      backup_delete_lcp_file_mem +
                      tup_trigger_mem;

  DEB_AUTOMATIC_MEMORY(("Backup Schema Memory %llu MBytes", backup_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("Backup Table Memory %llu MBytes",
                        backup_table_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("Backup Fragment Memory %llu MBytes",
                        backup_fragment_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("Backup Trigger Memory %llu MBytes",
                        backup_trigger_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("Backup Delete LCP File Memory %llu MBytes",
                        backup_delete_lcp_file_mem / MBYTE64));
  DEB_AUTOMATIC_MEMORY(("Backup Table record size: %zu",
                        Backup::getTableRecordSize()));
  DEB_AUTOMATIC_MEMORY(("Backup Fragment record size: %zu",
                        Backup::getFragmentRecordSize()));
  DEB_AUTOMATIC_MEMORY(("Backup Trigger record size: %zu",
                        Backup::getTriggerRecordSize()));
  DEB_AUTOMATIC_MEMORY(("Backup DeleteLcpFile record size: %zu",
                        Backup::getDeleteLcpFileRecordSize()));
  DEB_AUTOMATIC_MEMORY(("TUP Trigger record size: %zu",
                        Dbtup::getTriggerRecordSize()));

  globalData.theBackupSchemaMemory = backup_mem;
  return backup_mem;
}

Uint64
Configuration::get_replication_memory(ndb_mgm_configuration_iterator *p)
{
  Uint64 replication_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_REPLICATION_MEM, &replication_memory);
  if (replication_memory != 0)
  {
    globalData.theReplicationMemory = replication_memory;
    return replication_memory;
  }
  Uint64 num_ldm_threads = Uint64(globalData.ndbMtLqhWorkers);

  Uint64 suma_buffer_mem =
          Uint64(48) * MBYTE64 * num_ldm_threads;

  globalData.theReplicationMemory = suma_buffer_mem;
  return suma_buffer_mem;
}

Uint64
Configuration::get_and_set_transaction_memory(
                 const ndb_mgm_configuration_iterator *p,
                 Uint64 min_transaction_memory)
{
  Uint64 transaction_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_TRANSACTION_MEM, &transaction_memory);
  if (transaction_memory == 0)
  {
    Uint32 num_threads = get_num_threads();
    transaction_memory = Uint64(300) * MBYTE64;
    transaction_memory += (Uint64(num_threads) * Uint64(45) * MBYTE64);
  }
  transaction_memory = MAX(transaction_memory,
                           min_transaction_memory);
  if (transaction_memory == min_transaction_memory)
  {
    g_eventLogger->info("The reserved amount of TransactionMemory"
                        " requires us to increase the size of the "
                        "TransactionMemory");
  }
  globalData.theTransactionMemory = transaction_memory;
  return transaction_memory;
}

Uint64
Configuration::get_and_set_redo_buffer(const ndb_mgm_configuration_iterator *p)
{
  Uint32 redo_buffer = 0;
  Uint32 num_log_parts = 4;
  ndb_mgm_get_int_parameter(p, CFG_DB_NO_REDOLOG_PARTS, &num_log_parts);
  ndb_mgm_get_int_parameter(p, CFG_DB_REDO_BUFFER, &redo_buffer);
  Uint64 redo_buffer64 = Uint64(redo_buffer);
  if (redo_buffer == 0)
  {
    Uint32 num_ldm_threads = globalData.ndbMtLqhWorkers;
    redo_buffer64 = Uint64(num_ldm_threads) * Uint64(64) * MBYTE64;
    redo_buffer64 /= Uint64(num_log_parts);
    redo_buffer64 = MIN(redo_buffer64, Uint64(64) * MBYTE64);
  }
  globalData.theRedoBuffer = redo_buffer64;
  Uint64 ret_size = redo_buffer64 * Uint64(num_log_parts);
  return ret_size;
}

Uint64
Configuration::get_and_set_undo_buffer(const ndb_mgm_configuration_iterator *p)
{
  Uint64 undo_buffer = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_UNDO_BUFFER, &undo_buffer);
  if (undo_buffer == 0)
  {
    Uint32 num_ldm_threads = globalData.ndbMtLqhWorkers;
    undo_buffer = Uint64(48) * MBYTE64 * Uint64(num_ldm_threads);
  }
  if (undo_buffer > Uint64(16384) * MBYTE64)
  {
    undo_buffer = Uint64(16384) * MBYTE64;
  }
  globalData.theUndoBuffer = undo_buffer;
  return undo_buffer;
}

Uint64
Configuration::get_send_buffer(const ndb_mgm_configuration_iterator *p)
{
  Uint64 mem;
  Uint32 tot_mem = 0;
  ndb_mgm_get_int_parameter(p, CFG_TOTAL_SEND_BUFFER_MEMORY, &tot_mem);
  if (tot_mem)
  {
    mem = (Uint64)tot_mem;
  }
  else
  {
    Uint32 num_threads = get_num_threads();
    Uint32 num_extra_threads = globalData.ndbMtRecoverThreads;
    num_threads += num_extra_threads;
    mem = globalTransporterRegistry.get_total_max_send_buffer();
    mem += (Uint64(2) * MBYTE64 * num_threads);
  }
  return mem;
}

Uint64
Configuration::get_and_set_long_message_buffer(
                 const ndb_mgm_configuration_iterator *p)
{
  Uint32 long_signal_buffer = 0;
  ndb_mgm_get_int_parameter(p, CFG_DB_LONG_SIGNAL_BUFFER, &long_signal_buffer);
  Uint64 long_signal_buffer64 = Uint64(long_signal_buffer);
  if (long_signal_buffer64 == 0)
  {
    Uint32 num_threads = get_num_threads();
    long_signal_buffer64 = (Uint64(32) * MBYTE64);
    long_signal_buffer64 += (Uint64(num_threads - 1) * Uint64(12) * MBYTE64);
  }
  globalData.theLongSignalMemory = long_signal_buffer64;
  return long_signal_buffer64;
}

Uint64
Configuration::compute_os_overhead(Uint64 total_memory)
{
  /**
   * This includes memory used by the OS for all sorts of internal operations.
   * It also includes some free memory that is a safety to ensure that there
   * is some room for small processes to start and do some action on the VM.
   * Also includes buffers for file system access, thus providing fast access
   * to some commonly used files. We provide a bit more space for those things
   * with more threads used.
   *
   * When running ndbmtd in a graphical user environment it is a good idea to
   * use the TotalMemoryConfig variable since this number here is based on
   * running ndbmtd in a VM in the cloud.
   *
   * We remove 1% of the total memory, 100 MBytes per thread, in addition
   * we remove 1.4 GByte to handle smaller VM sizes.
   */
  Uint64 reserved_part = total_memory / Uint64(100);
  Uint32 num_threads = get_num_threads() + globalData.ndbMtRecoverThreads;
  Uint64 os_static_overhead = Uint64(1400) * MBYTE64;
  Uint64 os_cpu_overhead = Uint64(num_threads) * Uint64(100) * MBYTE64;
  return os_static_overhead + os_cpu_overhead + reserved_part;
}

Uint64
Configuration::compute_backup_page_memory(
                 const ndb_mgm_configuration_iterator *p)
{
  Uint32 backup_log_buffer = BACKUP_DEFAULT_LOGBUFFER_SIZE;
  ndb_mgm_get_int_parameter(p, CFG_DB_BACKUP_LOG_BUFFER_MEM, &backup_log_buffer);
  backup_log_buffer += BACKUP_DEFAULT_WRITE_SIZE;
  Uint64 lcp_buffer = BACKUP_DEFAULT_BUFFER_SIZE + BACKUP_DEFAULT_WRITE_SIZE;
  lcp_buffer *= ((2 * BackupFormat::NDB_MAX_FILES_PER_LCP) + 1); 
  Uint64 per_thread_buffer = lcp_buffer + Uint64(backup_log_buffer);
  per_thread_buffer +=
    ((Backup::NO_OF_PAGES_META_FILE + 9) * GLOBAL_PAGE_SIZE);
  Uint32 num_ldm_threads = globalData.ndbMtLqhWorkers;
  return per_thread_buffer * Uint64(num_ldm_threads);
}

Uint64
Configuration::compute_pack_memory()
{
  Uint32 num_ldm_threads = globalData.ndbMtLqhWorkers;
  Uint32 num_tc_threads = globalData.ndbMtTcWorkers;
  Uint64 ldm_pack_memory = Uint64(num_ldm_threads) * Uint64(8) * MBYTE64;
  Uint64 tc_pack_memory = Uint64(num_tc_threads) * Uint64(11) * MBYTE64;
  return ldm_pack_memory + tc_pack_memory;
}

Uint64
Configuration::compute_fs_memory()
{
  /**
   * Each FS thread allocates:
   * 32 kB read buffer
   * 16 kB write buffer
   * 256 kB inflate/deflate buffer
   * 192 kB stack memory
   * Thus around 512 kB of memory per FS thread.
   * The number of FS threads is a bit dynamic and increases with the
   * number of LDM threads. Around 4-8 threads are added per LDM thread.
   * Thus we add 4 MByte of memory space here for each LDM thread.
   */
  Uint32 num_ldm_threads = globalData.ndbMtLqhWorkers;
  Uint64 size_fs_mem = Uint64(4) * MBYTE64 * Uint64(num_ldm_threads);
  size_fs_mem = MAX(size_fs_mem, Uint64(32) * MBYTE64);
  return size_fs_mem;
}

Uint64
Configuration::get_and_set_shared_global_memory(
                 const ndb_mgm_configuration_iterator *p)
{
  Uint64 shared_global_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_SGA, &shared_global_memory);
  if (shared_global_memory == 0)
  {
    Uint32 num_threads = get_num_threads();
    shared_global_memory = Uint64(700) * MBYTE64;
    shared_global_memory += (Uint64(num_threads) * Uint64(50) * MBYTE64);
  }
  globalData.theSharedGlobalMemory = shared_global_memory;
  return shared_global_memory;
}

/*
  Return the value given by specified key in semicolon separated list
  of name=value and name:value pairs which is found before first
  name:value pair

  i.e list looks like
    [name1=value1][;name2=value2][;name3:value3][;name4:value4][;name5=value5]
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    searches this part of list

  the function will terminate it's search when first name:value pair
  is found

  NOTE! This is anlogue to how the InitialLogFileGroup and
  InitialTablespace strings are parsed in NdbCntrMain.cpp
*/

static
Uint64
parse_size(const char * src)
{
  Uint64 num = 0;
  char * endptr = 0;
  num = strtoll(src, &endptr, 10);

  if (endptr)
  {
    switch(* endptr){
    case 'k':
    case 'K':
      num *= 1024;
      break;
    case 'm':
    case 'M':
      num *= 1024;
      num *= 1024;
      break;
    case 'g':
    case 'G':
      num *= 1024;
      num *= 1024;
      num *= 1024;
      break;
    }
  }
  return num;
}

static void
parse_key_value_before_filespecs(const char *src,
                                 const char* key, Uint64& value)
{
  const size_t keylen = strlen(key);
  BaseString arg(src);
  Vector<BaseString> list;
  arg.split(list, ";");

  for (unsigned i = 0; i < list.size(); i++)
  {
    list[i].trim();
    if (native_strncasecmp(list[i].c_str(), key, keylen) == 0)
    {
      // key found, save its value
      value = parse_size(list[i].c_str() + keylen);
    }

    if (strchr(list[i].c_str(), ':'))
    {
      // found name:value pair, look no further
      return;
    }
  }
}

void
Configuration::assign_default_memory_sizes(
                 const ndb_mgm_configuration_iterator *p,
                 Uint64 min_transaction_memory)
{
  Uint32 long_signal_buffer = 0;
  ndb_mgm_get_int_parameter(p, CFG_DB_LONG_SIGNAL_BUFFER, &long_signal_buffer);
  if (long_signal_buffer == 0)
  {
    long_signal_buffer = Uint64(32) * MBYTE64;
  }
  globalData.theLongSignalMemory = Uint64(long_signal_buffer);

  Uint32 redo_buffer = 0;
  ndb_mgm_get_int_parameter(p, CFG_DB_REDO_BUFFER, &redo_buffer);
  if (redo_buffer == 0)
  {
    redo_buffer = Uint64(16) * MBYTE64;
  }
  globalData.theRedoBuffer = Uint64(redo_buffer);

  Uint64 undo_buffer = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_UNDO_BUFFER, &undo_buffer);
  if (undo_buffer == 0)
  {
    const char * lgspec = 0;
    if (!ndb_mgm_get_string_parameter(p,
                                      CFG_DB_DD_LOGFILEGROUP_SPEC,
                                      &lgspec))
    {
      parse_key_value_before_filespecs(lgspec,
                                       "undo_buffer_size=",
                                       undo_buffer);
    }
  }
  globalData.theUndoBuffer = undo_buffer;

  Uint64 shared_global_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_SGA, &shared_global_memory);
  if (shared_global_memory == 0)
  {
    shared_global_memory = Uint64(128) * MBYTE64;
  }
  globalData.theSharedGlobalMemory = shared_global_memory;

  Uint64 transaction_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_TRANSACTION_MEM, &transaction_memory);
  if (transaction_memory == 0)
  {
    transaction_memory = Uint64(32) * MBYTE64;
  }
  transaction_memory = MAX(transaction_memory,
                           min_transaction_memory);
  globalData.theTransactionMemory = transaction_memory;

  Uint64 schema_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_SCHEMA_MEM, &schema_memory);
  if (schema_memory == 0)
  {
    schema_memory = Uint64(16) * MBYTE64;
  }
  globalData.theSchemaMemory = schema_memory;

  Uint64 backup_schema_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_BACKUP_SCHEMA_MEM, &backup_schema_memory);
  if (backup_schema_memory == 0)
  {
    backup_schema_memory = Uint64(16) * MBYTE64;
  }
  globalData.theBackupSchemaMemory = backup_schema_memory;

  Uint64 replication_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_REPLICATION_MEM, &replication_memory);
  if (replication_memory == 0)
  {
    replication_memory = Uint64(16) * MBYTE64;
  }
  globalData.theReplicationMemory = replication_memory;

  Uint64 data_memory = 0;
  ndb_mgm_get_int64_parameter(p, CFG_DB_DATA_MEM, &data_memory);
  if (data_memory == 0)
  {
    data_memory = Uint64(98) * MBYTE64;
  }
  globalData.theDataMemory = data_memory;

  Uint64 page_cache_size = 0;
  ndb_mgm_get_int64_parameter(p,
                              CFG_DB_DISK_PAGE_BUFFER_MEMORY,
                              &page_cache_size);
  if (page_cache_size == 0)
  {
    page_cache_size = Uint64(64) * MBYTE64;
  }
  globalData.theDiskPageBufferMemory = page_cache_size;
}

Uint64
Configuration::compute_restore_memory()
{
  Uint32 num_ldm_threads = globalData.ndbMtLqhWorkers;
  Uint32 num_restore_threads = globalData.ndbMtRecoverThreads;
  num_ldm_threads += num_restore_threads;
  Uint64 restore_memory = Uint64(4) * MBYTE64 * Uint64(num_ldm_threads);
  return restore_memory;
}

Uint64
Configuration::compute_static_overhead()
{
  /**
   * Overhead from DBDIH pages, DBINFO, DBUTIL, DBDICT, block overhead
   * 122 MByte from mt.cpp. Schema transaction memory.
   */
  Uint64 static_overhead = Uint64(208) * MBYTE64;
  Uint32 num_threads = get_num_threads();
  Uint32 num_extra_threads = globalData.ndbMtRecoverThreads;
  static_overhead += // Small memory allocations
    ((num_threads + num_extra_threads) * MBYTE64);
  Uint32 num_send_threads = globalData.ndbMtSendThreads;
  Uint64 num_all_threads = num_threads + num_extra_threads + num_send_threads;
  static_overhead += (num_all_threads * 2 * MBYTE64); // Stack memory
  return static_overhead;
}

bool
Configuration::calculate_automatic_memory(ndb_mgm_configuration_iterator *p,
                                          Uint64 min_transaction_memory)
{
  bool total_memory_set = false;
  g_eventLogger->info("Automatic Memory Configuration start");
  Uint64 total_memory = get_total_memory(p, total_memory_set);
  if (total_memory < (Uint64(2048) * MBYTE64))
  {
    g_eventLogger->alert("AutomaticMemoryConfig requires at least 2 GByte of"
                         " available memory");
    return false;
  }
  Uint64 schema_memory = get_schema_memory(p);
  Uint64 backup_schema_memory = get_backup_schema_memory(p);
  Uint64 replication_memory = get_replication_memory(p);
  Uint64 transaction_memory =
    get_and_set_transaction_memory(p, min_transaction_memory);
  Uint64 redo_buffer = get_and_set_redo_buffer(p);
  Uint64 undo_buffer = get_and_set_undo_buffer(p);
  Uint64 long_message_buffer = get_and_set_long_message_buffer(p);
  Uint64 job_buffer = compute_jb_pages(&globalEmulatorData) * 
                      GLOBAL_PAGE_SIZE;
  Uint64 static_overhead = compute_static_overhead();
  Uint64 os_overhead = 0;
  if (!total_memory_set)
  {
    os_overhead = compute_os_overhead(total_memory);
  }
  Uint64 send_buffer = get_send_buffer(p);
  Uint64 backup_page_memory = compute_backup_page_memory(p);
  Uint64 restore_memory = compute_restore_memory();
  Uint64 pack_memory = compute_pack_memory();
  Uint64 fs_memory = compute_fs_memory();
  Uint64 shared_global_memory = get_and_set_shared_global_memory(p);
  Uint64 used_memory =
    schema_memory +
    backup_schema_memory +
    replication_memory +
    transaction_memory +
    redo_buffer +
    undo_buffer +
    long_message_buffer +
    job_buffer +
    send_buffer +
    static_overhead +
    os_overhead +
    backup_page_memory +
    restore_memory +
    pack_memory +
    fs_memory +
    shared_global_memory;
  g_eventLogger->info("SchemaMemory is %llu MBytes", schema_memory / MBYTE64);
  g_eventLogger->info("BackupSchemaMemory is %llu MBytes",
                      backup_schema_memory / MBYTE64);
  g_eventLogger->info("ReplicationMemory is %llu MBytes",
                      replication_memory / MBYTE64);
  g_eventLogger->info("TransactionMemory is %llu MBytes",
                      transaction_memory / MBYTE64);
  g_eventLogger->info("Redo log buffer size total are %llu MBytes",
                      redo_buffer / MBYTE64);
  g_eventLogger->info("Undo log buffer is %llu MBytes", undo_buffer / MBYTE64);
  g_eventLogger->info("LongMessageBuffer is %llu MBytes",
                      long_message_buffer / MBYTE64);
  g_eventLogger->info("Send buffer sizes are %llu MBytes",
                      send_buffer / MBYTE64);
  g_eventLogger->info("Job buffer sizes are %llu MBytes",
                      job_buffer / MBYTE64);
  g_eventLogger->info("Static overhead is %llu MBytes",
                      static_overhead / MBYTE64);
  g_eventLogger->info("OS overhead is %llu MBytes", os_overhead / MBYTE64);
  g_eventLogger->info("Backup Page memory is %llu MBytes",
                      backup_page_memory / MBYTE64);
  g_eventLogger->info("Restore memory is %llu MBytes",
                      restore_memory / MBYTE64);
  g_eventLogger->info("Packed signal memory is %llu MBytes",
                      pack_memory / MBYTE64);
  g_eventLogger->info("NDBFS memory is %llu MBytes", fs_memory / MBYTE64);
  g_eventLogger->info("SharedGlobalMemory is %llu MBytes",
                      shared_global_memory / MBYTE64);
  g_eventLogger->info("Total memory is %llu MBytes", total_memory / MBYTE64);
  g_eventLogger->info("Used memory is %llu MBytes", used_memory / MBYTE64);
  if (used_memory + (Uint64(512) * MBYTE64) >= total_memory)
  {
    /**
     * We require at least 512 MByte for DataMemory and DiskPageBufferMemory
     * to even start in AutomaticMemoryConfig mode.
     */
    g_eventLogger->info("AutomaticMemoryConfig mode requires at least"
                        " 512 MByte of space for DataMemory and"
                        " DiskPageBufferMemory");
    g_eventLogger->alert("Not enough memory using automatic memory config,"
                         " exiting, used memory is %u MBytes, total memory"
                         " is %u MBytes, required memory is %u MBytes",
                         Uint32(used_memory / MBYTE64),
                         Uint32(total_memory / MBYTE64),
                         Uint32((used_memory / MBYTE64) + 512));
    return false;
  }
  Uint64 remaining_memory = total_memory - used_memory;
  g_eventLogger->info("Remaining memory is %llu MBytes",
                      remaining_memory / MBYTE64);
  Uint64 page_cache_size = 0;
  Uint64 data_memory = 0;
  ndb_mgm_get_int64_parameter(p,
                              CFG_DB_DISK_PAGE_BUFFER_MEMORY,
                              &page_cache_size);
  ndb_mgm_get_int64_parameter(p, CFG_DB_DATA_MEM, &data_memory);
  if (page_cache_size == 0)
  {
    if (data_memory == 0)
    {
      data_memory = (Uint64(9) * remaining_memory) / Uint64(10);
      page_cache_size = (Uint64(1) * remaining_memory) / Uint64(10);
    }
    else
    {
      if (data_memory > (remaining_memory - Uint64(64) * MBYTE64))
      {
        g_eventLogger->alert("Not enough memory left for DiskPageBufferMemory,"
                             " exiting");
        return false;
      }
      page_cache_size = remaining_memory - data_memory;
    }
  }
  else
  {
    if (data_memory == 0)
    {
      if (page_cache_size > (remaining_memory - Uint64(512) * MBYTE64))
      {
        g_eventLogger->alert("Not enough memory left for DataMemory, exiting");
        return false;
      }
      data_memory = remaining_memory - page_cache_size;
    }
    else
    {
      if (data_memory + page_cache_size > remaining_memory)
      {
        g_eventLogger->alert("Not enough memory left for DiskPageBufferMemory"
                             " and DataMemory, exiting");
        return false;
      }
    }
  }
  /**
   * Each page in the disk page buffer requires 10 Page_entries in PGMAN.
   * This is part of the Disk Page Buffer Memory. But theDiskPageBufferMemory
   * doesn't take this into account.
   */
  Uint32 num_entries_per_page = 10;
  ndb_mgm_get_int_parameter(p,
                            CFG_DB_DISK_PAGE_BUFFER_ENTRIES,
                            &num_entries_per_page);
  Uint64 page_entry_size = Pgman::getPageEntryRecordSize();
  Uint64 tot_page_cache_size = page_cache_size;
  page_entry_size *= num_entries_per_page;
  page_cache_size *= GLOBAL_PAGE_SIZE;
  page_cache_size /= (GLOBAL_PAGE_SIZE + page_entry_size);
  Uint64 tot_page_entry_size = tot_page_cache_size - page_cache_size;
  DEB_AUTOMATIC_MEMORY(("Page entry size per page is %llu bytes",
                        page_entry_size));

  g_eventLogger->info("Setting DataMemory to %llu MBytes",
                      data_memory / MBYTE64);
  g_eventLogger->info("Setting DiskPageBufferMemory to %llu MBytes",
                      page_cache_size / MBYTE64);
  g_eventLogger->info("%llu MBytes used for Page Entry objects",
                      tot_page_entry_size / MBYTE64);

  globalData.theDataMemory = data_memory;
  globalData.theDiskPageBufferMemory = page_cache_size;
  g_eventLogger->info("Automatic Memory Configuration done");
  return true;
}

Uint32
Configuration::getSharedLdmInstance(Uint32 instance)
{
  if (instance == 0 ||
      instance > globalData.ndbMtLqhWorkers)
    return 0;
  return m_thr_config.get_shared_ldm_instance(instance,
                                              globalData.ndbMtLqhThreads);
}

void
Configuration::setupConfiguration()
{

  DBUG_ENTER("Configuration::setupConfiguration");

  /**
   * Configure transporters
   */
  if (!globalTransporterRegistry.init(globalData.ownId))
  {
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
              "Invalid configuration fetched",
              "Could not init transporter registry");
  }

  if (!IPCConfig::configureTransporters(globalData.ownId,
                                        m_clusterConfig.get(),
                                        globalTransporterRegistry))
  {
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
              "Invalid configuration fetched",
              "Could not configure transporters");
  }

  /**
   * Setup cluster configuration for this node
   */
  ndb_mgm_configuration_iterator iter(m_clusterConfig.get(), CFG_SECTION_NODE);
  if (iter.find(CFG_NODE_ID, globalData.ownId)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched", "DB missing");
  }

  unsigned type;
  if(!(iter.get(CFG_TYPE_OF_SECTION, &type) == 0 && type == NODE_TYPE_DB)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched",
	      "I'm wrong type of node");
  }

  /**
   * Iff we use the 'default' (non-mt) send buffer implementation, the
   * send buffers are allocated here.
   */
  if (getNonMTTransporterSendHandle() != NULL)
  {
    Uint32 total_send_buffer = 0;
    iter.get(CFG_TOTAL_SEND_BUFFER_MEMORY, &total_send_buffer);
    Uint64 extra_send_buffer = 0;
    iter.get(CFG_EXTRA_SEND_BUFFER_MEMORY, &extra_send_buffer);
    getNonMTTransporterSendHandle()->
      allocate_send_buffers(total_send_buffer,
                            extra_send_buffer);
  }

  if(iter.get(CFG_DB_NO_SAVE_MSGS, &_maxErrorLogs)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched", 
	      "MaxNoOfSavedMessages missing");
  }
  
  if(iter.get(CFG_DB_MEMLOCK, &_lockPagesInMainMemory)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched", 
	      "LockPagesInMainMemory missing");
  }

  if(iter.get(CFG_DB_WATCHDOG_INTERVAL, &_timeBetweenWatchDogCheck)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched", 
	      "TimeBetweenWatchDogCheck missing");
  }

  _schedulerResponsiveness = 5;
  iter.get(CFG_DB_SCHED_RESPONSIVENESS, &_schedulerResponsiveness);

  _schedulerExecutionTimer = 50;
  iter.get(CFG_DB_SCHED_EXEC_TIME, &_schedulerExecutionTimer);

  _schedulerSpinTimer = DEFAULT_SPIN_TIME;
  iter.get(CFG_DB_SCHED_SPIN_TIME, &_schedulerSpinTimer);
  /* Always set SchedulerSpinTimer to 0 on platforms not supporting spin */
  if (!NdbSpin_is_supported())
  {
    _schedulerSpinTimer = 0;
  }
  g_eventLogger->info("SchedulerSpinTimer = %u", _schedulerSpinTimer);

  _spinTimePerCall = 1000;
  iter.get(CFG_DB_SPIN_TIME_PER_CALL, &_spinTimePerCall);

  _realtimeScheduler = 0;
  iter.get(CFG_DB_REALTIME_SCHEDULER, &_realtimeScheduler);

  if(iter.get(CFG_DB_WATCHDOG_INTERVAL_INITIAL, 
              &_timeBetweenWatchDogCheckInitial)){
    ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, "Invalid configuration fetched", 
	      "TimeBetweenWatchDogCheckInitial missing");
  }

#ifdef ERROR_INSERT
  _mixologyLevel = 0;
  iter.get(CFG_MIXOLOGY_LEVEL, &_mixologyLevel);
  if (_mixologyLevel)
  {
    g_eventLogger->info("Mixology level set to 0x%x", _mixologyLevel);
    globalTransporterRegistry.setMixologyLevel(_mixologyLevel);
  }
#endif
  
  /**
   * Get paths
   */  
  if (_fsPath)
    free(_fsPath);
  _fsPath= get_and_validate_path(iter,
                                 CFG_DB_FILESYSTEM_PATH,
                                 "FileSystemPath");
  if (_backupPath)
    free(_backupPath);
  _backupPath= get_and_validate_path(iter,
                                     CFG_DB_BACKUP_DATADIR,
                                     "BackupDataDir");

  if(iter.get(CFG_DB_STOP_ON_ERROR_INSERT, &m_restartOnErrorInsert)){
    ERROR_SET(fatal,
              NDBD_EXIT_INVALID_CONFIG,
              "Invalid configuration fetched", 
	      "RestartOnErrorInsert missing");
  }
  
  /**
   * Create the watch dog thread
   */
  { 
    if (_timeBetweenWatchDogCheckInitial < _timeBetweenWatchDogCheck)
      _timeBetweenWatchDogCheckInitial = _timeBetweenWatchDogCheck;
    
    Uint32 t = _timeBetweenWatchDogCheckInitial;
    t = globalEmulatorData.theWatchDog ->setCheckInterval(t);
    _timeBetweenWatchDogCheckInitial = t;
  }

  const char * lockmask = 0;
  {
    if (iter.get(CFG_DB_EXECUTE_LOCK_CPU, &lockmask) == 0)
    {
      int res = m_thr_config.setLockExecuteThreadToCPU(lockmask);
      if (res < 0)
      {
        // Could not parse LockExecuteThreadToCPU mask
        g_eventLogger->warning("Failed to parse 'LockExecuteThreadToCPU=%s' "
                               "(error: %d), ignoring it!",
                               lockmask, res);
      }
    }
  }

  {
    Uint32 maintCPU = NO_LOCK_CPU;
    iter.get(CFG_DB_MAINT_LOCK_CPU, &maintCPU);
    if (maintCPU == 65535)
      maintCPU = NO_LOCK_CPU; // Ignore old default(may come from old mgmd)
    if (maintCPU != NO_LOCK_CPU)
      m_thr_config.setLockIoThreadsToCPU(maintCPU);
  }

  const char * thrconfigstring = nullptr;
  Uint32 mtthreads = 0;
  Uint32 auto_thread_config = 0;
  Uint32 num_cpus = 0;
  iter.get(CFG_DB_AUTO_THREAD_CONFIG, &auto_thread_config);
  iter.get(CFG_DB_NUM_CPUS, &num_cpus);
  g_eventLogger->info("AutomaticThreadConfig = %u, NumCPUs = %u",
                      auto_thread_config,
                      num_cpus);
  iter.get(CFG_DB_MT_THREADS, &mtthreads);
  iter.get(CFG_DB_MT_THREAD_CONFIG, &thrconfigstring);
  if (auto_thread_config == 0 &&
      thrconfigstring != nullptr && thrconfigstring[0] != 0)
  {
    int res = m_thr_config.do_parse(
        thrconfigstring, _realtimeScheduler, _schedulerSpinTimer);
    if (res != 0)
    {
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
                "Invalid configuration fetched, invalid ThreadConfig",
                m_thr_config.getErrorMessage());
    }
  }
  else
  {
    if (auto_thread_config != 0)
    {
      Uint32 num_cpus = 0;
      iter.get(CFG_DB_NUM_CPUS, &num_cpus);
      g_eventLogger->info("Use automatic thread configuration");
      m_thr_config.do_parse(_realtimeScheduler,
                            _schedulerSpinTimer,
                            num_cpus,
                            globalData.ndbRRGroups);
    }
    else
    {
      Uint32 classic = 0;
      iter.get(CFG_NDBMT_CLASSIC, &classic);
#ifdef NDB_USE_GET_ENV
      const char* p = NdbEnv_GetEnv("NDB_MT_LQH", (char*)0, 0);
      if (p != 0)
      {
        if (strstr(p, "NOPLEASE") != 0)
          classic = 1;
      }
#endif
      Uint32 lqhthreads = 0;
      iter.get(CFG_NDBMT_LQH_THREADS, &lqhthreads);
      int res = m_thr_config.do_parse(mtthreads,
                                      lqhthreads,
                                      classic,
                                      _realtimeScheduler,
                                      _schedulerSpinTimer);
      if (res != 0)
      {
        ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
          "Invalid configuration fetched, invalid thread configuration",
          m_thr_config.getErrorMessage());
      }
    }
  }
  if (NdbIsMultiThreaded())
  {
    if (thrconfigstring)
    {
      g_eventLogger->info(
          "ThreadConfig: input: %s LockExecuteThreadToCPU: %s => parsed: %s",
          thrconfigstring, lockmask ? lockmask : "",
          m_thr_config.getConfigString());
    }
    else if (mtthreads == 0)
    {
      g_eventLogger->info(
          "Automatic Thread Config: LockExecuteThreadToCPU: %s => parsed: %s",
          lockmask ? lockmask : "", m_thr_config.getConfigString());
    }
    else
    {
      g_eventLogger->info(
          "ThreadConfig (old ndb_mgmd) LockExecuteThreadToCPU: %s => parsed: %s",
          lockmask ? lockmask : "", m_thr_config.getConfigString());
    }
  }

  ConfigValues* cf = ConfigValuesFactory::extractCurrentSection(iter.m_config);

  ndb_mgm_destroy_iterator(m_clusterConfigIter);
  m_clusterConfigIter = ndb_mgm_create_configuration_iterator(
      m_clusterConfig.get(), CFG_SECTION_NODE);

  /**
   * This is parts of get_multithreaded_config
   */
  do
  {
    globalData.isNdbMt = NdbIsMultiThreaded();
    require(globalData.isNdbMt);

    globalData.ndbMtReceiveThreads =
      m_thr_config.getThreadCount(THRConfig::T_RECV);
    globalData.ndbMtSendThreads =
      m_thr_config.getThreadCount(THRConfig::T_SEND);
    globalData.ndbMtQueryThreads =
      m_thr_config.getThreadCount(THRConfig::T_QUERY);
    if (globalData.ndbMtQueryThreads > 0)
    {
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
               "Invalid configuration fetched. ",
               " Query threads are no longer supported"
               ", instead Query blocks are part of LDM threads,"
               " thus move the query thread instances to LDM threads"
               " and also the CPU bindings should be moved");
    }
    globalData.ndbMtRecoverThreads =
      m_thr_config.getThreadCount(THRConfig::T_RECOVER);
    globalData.ndbMtTcThreads = m_thr_config.getThreadCount(THRConfig::T_TC);
    if (globalData.ndbMtTcThreads == 0)
    {
      globalData.ndbMtTcWorkers = globalData.ndbMtReceiveThreads;
    }
    else
    {
      globalData.ndbMtTcWorkers = globalData.ndbMtTcThreads;
    }
    if (globalData.ndbMtReceiveThreads == 0)
    {
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
               "Invalid configuration fetched. ",
                "Setting number of receive threads to 0 isn't allowed");
    }
    /**
     * ndbMtMainThreads is the total number of main and rep threads.
     * There can be 0 or 1 main threads, 0 or 1 rep threads. If there
     * is 0 main threads then the blocks handled by the main thread is
     * handled by the receive thread and so is the rep thread blocks.
     *
     * When there is one main thread, then we will have both the main
     * thread blocks and the rep thread blocks handled by this single
     * main thread. With two main threads we will have one main thread
     * that handles the main thread blocks and one thread handling the
     * rep thread blocks.
     *
     * The nomenclature can be a bit confusing that we have a main thread
     * that is separate from the main threads. So possibly one could have
     * called this variable globalData.ndbMtMainRepThreads instead.
     */
    globalData.ndbMtMainThreads =
      m_thr_config.getThreadCount(THRConfig::T_MAIN) +
      m_thr_config.getThreadCount(THRConfig::T_REP);

    require(globalData.ndbMtMainThreads <= 2);
    globalData.isNdbMtLqh = true;
    {
      if (m_thr_config.getMtClassic())
      {
        ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
                   "Invalid configuration fetched. ",
                   "Multithreaded classic is no longer supported");
      }
    }
    require(globalData.isNdbMtLqh);

    Uint32 ldm_threads = m_thr_config.getThreadCount(THRConfig::T_LDM);
    Uint32 ldm_workers = ldm_threads;
    if (ldm_threads == 0)
    {
      /**
       * If there are no LDM Threads we will use 1 LDM worker per
       * receive thread.
       */
      ldm_workers = globalData.ndbMtReceiveThreads;
    }

    globalData.ndbMtLqhWorkers = ldm_workers;
    globalData.ndbMtLqhThreads = ldm_threads;
    globalData.ndbMtQueryWorkers = ldm_workers;
    if (ldm_threads == 0)
    {
      /**
       * With no LDM threads we will only allow them to be combined
       * with receive threads. We will allow 0-2 main threads. These
       * will only handle main thread. Thus receive threads will handle
       * receive, tc and ldm and even query thread work and potentially
       * even sending.
       *
       * With 1 receive thread we will allow for a maximum of 1 main
       * thread.
       */
      if ((globalData.ndbMtTcThreads > 0) ||
          (globalData.ndbMtRecoverThreads > 0))
      {
        ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
                  "Invalid configuration fetched. ",
                  "Setting number of ldm threads to 0 must be combined"
                  " with 0 tc and recover threads");
      }
      if (globalData.ndbMtReceiveThreads > 1)
      {
        /**
         * In the case of 1 receive thread and no LDM threads we will not
         * assign any Query workers. But with more than 1 receive thread
         * and no LDM thread we will use 1 LDM worker and 1 Query worker in
         * each receive thread in addition to a TC worker.
         *
         * This ensures that the receive thread can take care of the entire
         * query for Committed Read queries, a sort of parallel ndbd setup.
         */
        globalData.ndbMtQueryWorkers = globalData.ndbMtReceiveThreads;
      }
      else
      {
        globalData.ndbMtQueryWorkers = 0;
        if (globalData.ndbMtMainThreads > 1)
        {
          ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
                    "Invalid configuration fetched. ",
                    "No LDM threads with 1 receive thread allows for"
                    " 1 main thread, but no more");
        }
      }
    }
    globalData.QueryThreadsPerLdm = 0;
    if (globalData.ndbMtQueryWorkers > 0)
    {
      globalData.QueryThreadsPerLdm = 1;
    }

    if ((globalData.ndbMtRecoverThreads + globalData.ndbMtQueryWorkers) >
         MAX_NDBMT_QUERY_THREADS)
    {
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
                "Invalid configuration fetched. ",
                "Sum of recover threads and query threads can be max 127");
    }
    require(globalData.ndbMtQueryWorkers == globalData.ndbMtLqhThreads ||
            globalData.ndbMtQueryWorkers == globalData.ndbMtReceiveThreads);
  } while (0);

  calcSizeAlt(cf);
  set_not_active_nodes();
  DBUG_VOID_RETURN;
}

void
Configuration::setupMemoryConfiguration(Uint64 min_transaction_memory)
{
  DBUG_ENTER("Configuration::setupMemoryConfiguration");
  ndb_mgm_configuration_iterator * it_p =
    globalEmulatorData.theConfiguration->getOwnConfigIterator();
  Uint32 automatic_memory_config = 1;
  ndb_mgm_get_int_parameter(it_p,
                            CFG_DB_AUTO_MEMORY_CONFIG,
                            &automatic_memory_config);
  if (automatic_memory_config)
  {
    if (!calculate_automatic_memory(it_p, min_transaction_memory))
    {
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG,
                "Invalid configuration fetched",
                "Could not handle automatic memory config");
      DBUG_VOID_RETURN;
    }
  }
  else
  {
    assign_default_memory_sizes(it_p, min_transaction_memory);
  }
  DBUG_VOID_RETURN;
}

Uint32
Configuration::lockPagesInMainMemory() const {
  return _lockPagesInMainMemory;
}

int 
Configuration::schedulerExecutionTimer() const {
  return _schedulerExecutionTimer;
}

void 
Configuration::schedulerExecutionTimer(int value) {
  if (value < 11000)
    _schedulerExecutionTimer = value;
}

Uint32
Configuration::spinTimePerCall() const {
  return _spinTimePerCall;
}

int 
Configuration::schedulerSpinTimer() const {
  return _schedulerSpinTimer;
}

void 
Configuration::schedulerSpinTimer(int value) {
  if (value < 500)
    value = 500;
  _schedulerSpinTimer = value;
}

bool 
Configuration::realtimeScheduler() const
{
  return (bool)_realtimeScheduler;
}

void 
Configuration::realtimeScheduler(bool realtime_on)
{
   bool old_value = (bool)_realtimeScheduler;
  _realtimeScheduler = (Uint32)realtime_on;
  if (old_value != realtime_on)
    setAllRealtimeScheduler();
}

int 
Configuration::timeBetweenWatchDogCheck() const {
  return _timeBetweenWatchDogCheck;
}

void 
Configuration::timeBetweenWatchDogCheck(int value) {
  _timeBetweenWatchDogCheck = value;
}

int 
Configuration::maxNoOfErrorLogs() const {
  return _maxErrorLogs;
}

void 
Configuration::maxNoOfErrorLogs(int val){
  _maxErrorLogs = val;
}

bool
Configuration::stopOnError() const {
  return _stopOnError;
}

void 
Configuration::stopOnError(bool val){
  _stopOnError = val;
}

int
Configuration::getRestartOnErrorInsert() const {
  return m_restartOnErrorInsert;
}

void
Configuration::setRestartOnErrorInsert(int i){
  m_restartOnErrorInsert = i;
}

#ifdef ERROR_INSERT
Uint32
Configuration::getMixologyLevel() const {
  return _mixologyLevel;
}

void
Configuration::setMixologyLevel(Uint32 l){
  _mixologyLevel = l;
}
#endif

ndb_mgm_configuration_iterator * 
Configuration::getOwnConfigIterator() const {
  return m_ownConfigIterator;
}

const ConfigValues*
Configuration::get_own_config_values()
{
  return &m_ownConfig->m_config_values;
}


ndb_mgm_configuration_iterator * 
Configuration::getClusterConfigIterator() const {
  return m_clusterConfigIter;
}

Uint32 
Configuration::get_config_generation() const {
  Uint32 generation = ~0;
  ndb_mgm_configuration_iterator sys_iter(m_clusterConfig.get(),
                                          CFG_SECTION_SYSTEM);
  sys_iter.get(CFG_SYS_CONFIG_GENERATION, &generation);
  return generation;
}

void
Configuration::calcSizeAlt(ConfigValues * ownConfig)
{
  const char * msg = "Invalid configuration fetched";
  char buf[255];

  /**
   * These initializations are only used for variables that are not present in
   * the configuration we receive from the management server. That will only
   * happen for older management servers that lacks definitions for some
   * variables.
   */
  unsigned int noOfTables = 0;
  unsigned int noOfUniqueHashIndexes = 0;
  unsigned int noOfOrderedIndexes = 0;
  unsigned int noOfTriggers = 0;
  unsigned int noOfReplicas = 0;
  unsigned int noOfDBNodes = 0;
  unsigned int noOfAPINodes = 0;
  unsigned int noOfMGMNodes = 0;
  unsigned int noOfNodes = 0;
  unsigned int noOfAttributes = 0;
  unsigned int noOfOperations = 32768;
  unsigned int noOfLocalOperations = 32;
  unsigned int noOfTransactions = 4096;
  unsigned int noOfScanRecords = 256;
  unsigned int noOfLocalScanRecords = 32;
  unsigned int noBatchSize = 0;
  unsigned int noOfIndexOperations = 8192;
  unsigned int noOfTriggerOperations = 4000;
  unsigned int reservedScanRecords = 256 / 4;
  unsigned int reservedLocalScanRecords = 32 / 4;
  unsigned int reservedOperations = DEFAULT_TC_RESERVED_CONNECT_RECORD;
  unsigned int reservedTransactions = 4096 / 4;
  unsigned int reservedIndexOperations = 8192 / 4;
  unsigned int reservedTriggerOperations = 4000 / 4;
  unsigned int transactionBufferBytes = 1048576;
  unsigned int reservedTransactionBufferBytes = 1048576 / 4;
  unsigned int maxOpsPerTrans = ~(Uint32)0;
  unsigned int classicFragmentation = 0;
  unsigned int partitionsPerNode = 2;
  unsigned int automaticThreadConfig = 1;
  unsigned int automaticMemoryConfig = 1;

  m_logLevel = new LogLevel();
  if (!m_logLevel)
  {
    ERROR_SET(fatal, NDBD_EXIT_MEMALLOC, "Failed to create LogLevel", "");
  }
  
  struct AttribStorage { int paramId; Uint32 * storage; bool computable; };
  AttribStorage tmp[] = {
    { CFG_DB_NO_SCANS, &noOfScanRecords, false },
    { CFG_DB_RESERVED_SCANS, &reservedScanRecords, true },
    { CFG_DB_NO_LOCAL_SCANS, &noOfLocalScanRecords, true },
    { CFG_DB_RESERVED_LOCAL_SCANS, &reservedLocalScanRecords, true },
    { CFG_DB_BATCH_SIZE, &noBatchSize, false },
    { CFG_DB_NO_TABLES, &noOfTables, false },
    { CFG_DB_NO_ORDERED_INDEXES, &noOfOrderedIndexes, false },
    { CFG_DB_NO_UNIQUE_HASH_INDEXES, &noOfUniqueHashIndexes, false },
    { CFG_DB_NO_TRIGGERS, &noOfTriggers, true },
    { CFG_DB_NO_REPLICAS, &noOfReplicas, false },
    { CFG_DB_NO_ATTRIBUTES, &noOfAttributes, false },
    { CFG_DB_NO_OPS, &noOfOperations, false },
    { CFG_DB_RESERVED_OPS, &reservedOperations, true },
    { CFG_DB_NO_LOCAL_OPS, &noOfLocalOperations, true },
    { CFG_DB_NO_TRANSACTIONS, &noOfTransactions, false },
    { CFG_DB_RESERVED_TRANSACTIONS, &reservedTransactions, true },
    { CFG_DB_MAX_DML_OPERATIONS_PER_TRANSACTION, &maxOpsPerTrans, false },
    { CFG_DB_NO_INDEX_OPS, &noOfIndexOperations, true },
    { CFG_DB_RESERVED_INDEX_OPS, &reservedIndexOperations, true },
    { CFG_DB_NO_TRIGGER_OPS, &noOfTriggerOperations, true },
    { CFG_DB_RESERVED_TRIGGER_OPS, &reservedTriggerOperations, true },
    { CFG_DB_TRANS_BUFFER_MEM, &transactionBufferBytes, false },
    { CFG_DB_RESERVED_TRANS_BUFFER_MEM, &reservedTransactionBufferBytes, true },
    { CFG_DB_CLASSIC_FRAGMENTATION, &classicFragmentation, false },
    { CFG_DB_PARTITIONS_PER_NODE, &partitionsPerNode, false },
    { CFG_DB_AUTO_THREAD_CONFIG, &automaticThreadConfig, false },
    { CFG_DB_AUTO_MEMORY_CONFIG, &automaticMemoryConfig, false },
  };

  ndb_mgm_configuration_iterator db(
      reinterpret_cast<ndb_mgm_configuration *>(ownConfig), 0);

  const int sz = sizeof(tmp)/sizeof(AttribStorage);
  for(int i = 0; i<sz; i++){
    if(ndb_mgm_get_int_parameter(&db, tmp[i].paramId, tmp[i].storage)){
      if (tmp[i].computable) {
        *tmp[i].storage = 0;
      } else {
        BaseString::snprintf(buf, sizeof(buf),"ConfigParam: %d not found", tmp[i].paramId);
        ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
      }
    }
  }

  if (noOfAttributes == 0)
  {
    if (automaticMemoryConfig)
      noOfAttributes = 500000;
    else
      noOfAttributes = 1000;
  }
  globalData.theMaxNoOfAttributes = noOfAttributes;
  if (noOfTriggers == 0)
  {
    if (automaticMemoryConfig)
      noOfTriggers = 200000;
    else
      noOfTriggers = 768;
  }
  globalData.theMaxNoOfTriggers = noOfTriggers;
  if (noOfTables == 0)
  {
    if (automaticMemoryConfig)
      noOfTables = 8000;
    else
      noOfTables = 128;
  }
  globalData.theMaxNoOfTables = noOfTables;
  if (noOfOrderedIndexes == 0)
  {
    if (automaticMemoryConfig)
      noOfOrderedIndexes = 10000;
    else
      noOfOrderedIndexes = 128;
  }
  globalData.theMaxNoOfOrderedIndexes = noOfOrderedIndexes;
  if (noOfUniqueHashIndexes == 0)
  {
    if (automaticMemoryConfig)
      noOfUniqueHashIndexes = 2300;
    else
      noOfUniqueHashIndexes = 64;
  }
  globalData.theMaxNoOfUniqueHashIndexes = noOfUniqueHashIndexes;
  Uint32 noOfMetaTables = globalData.theMaxNoOfTables +
                          globalData.theMaxNoOfOrderedIndexes +
                          globalData.theMaxNoOfUniqueHashIndexes;

  Uint32 ldmInstances = 1;
  if (globalData.isNdbMtLqh)
  {
    ldmInstances = globalData.ndbMtLqhWorkers;
  }

  Uint32 tcInstances = 1;
  if (globalData.ndbMtTcWorkers > 1)
  {
    tcInstances = globalData.ndbMtTcWorkers;
  }

#define DO_DIV(x,y) (((x) + (y - 1)) / (y))

  for(unsigned j = 0; j<LogLevel::LOGLEVEL_CATEGORIES; j++)
  {
    Uint32 tmp;
    if (!ndb_mgm_get_int_parameter(&db, CFG_MIN_LOGLEVEL+j, &tmp))
    {
      m_logLevel->setLogLevel((LogLevel::EventCategory)j, tmp);
    }
  }
  
  get_num_nodes(noOfNodes,
                noOfDBNodes,
                noOfAPINodes,
                noOfMGMNodes);

  noOfMetaTables+= 2; // Add System tables
  noOfAttributes += 9;  // Add System table attributes
  globalData.theMaxNoOfTables += 2;
  globalData.theMaxNoOfAttributes += 9;

  {
    Uint32 neededNoOfTriggers =   /* types: Insert/Update/Delete/Custom */
      3 * noOfUniqueHashIndexes + /* for unique hash indexes, I/U/D */
      3 * NDB_MAX_ACTIVE_EVENTS + /* for events in suma, I/U/D */
      3 * noOfMetaTables +        /* for backup, I/U/D */
      3 * noOfMetaTables +        /* for Fully replicated tables, I/U/D */
      noOfOrderedIndexes;         /* for ordered indexes, C */
    if (noOfTriggers < neededNoOfTriggers)
    {
      noOfTriggers = neededNoOfTriggers;
      globalData.theMaxNoOfTriggers = noOfTriggers;
    }
    g_eventLogger->info("MaxNoOfTriggers set to %u", noOfTriggers);
  }

  /**
   * Do size calculations
   */
  ConfigValuesFactory cfg(ownConfig);

  cfg.begin();
  /**
   * Ensure that Backup doesn't fail due to lack of trigger resources
   */
  cfg.put(CFG_TUP_NO_TRIGGERS, noOfTriggers + 3 * noOfMetaTables);

  Uint32 noOfMetaTablesDict= noOfMetaTables;
  if (noOfMetaTablesDict > NDB_MAX_TABLES)
    noOfMetaTablesDict= NDB_MAX_TABLES;

  {
    /**
     * Dict Size Alt values
     */
    cfg.put(CFG_DICT_ATTRIBUTE, 
	    noOfAttributes);

    cfg.put(CFG_DICT_TABLE,
	    noOfMetaTablesDict);
  }


  if (noOfLocalScanRecords == 0)
  {
    noOfLocalScanRecords = tcInstances * ldmInstances *
      (noOfDBNodes * noOfScanRecords) +
      1 /* NR */ + 
      1 /* LCP */;
    if (noOfLocalScanRecords > 100000)
    {
      /**
       * Number of local scan records is clearly very large, this should
       * only happen in very large clusters with lots of data nodes, lots
       * of TC instances, lots of LDM instances. In this case it is highly
       * unlikely that all these resources are allocated simultaneously.
       * It is still possible to set MaxNoOfLocalScanRecords to a higher
       * number if desirable.
       */
      g_eventLogger->info("Capped calculation of local scan records to "
                          "100000 from %u, still possible to set"
                          " MaxNoOfLocalScans"
                          " explicitly to go higher",
                          noOfLocalScanRecords);
      noOfLocalScanRecords = 100000;
    }
    if (noOfLocalScanRecords * noBatchSize > 1000000)
    {
      /**
       * Ensure that we don't use up more than 100 MByte of lock operation
       * records per LDM instance to avoid ridiculous amount of memory
       * allocated for operation records. We keep old numbers in smaller
       * configs for easier upgrades.
       */
      Uint32 oldBatchSize = noBatchSize;
      noBatchSize = 1000000 / noOfLocalScanRecords;
      g_eventLogger->info("Capped BatchSizePerLocalScan to %u from %u to avoid"
                          " very large memory allocations"
                          ", still possible to set MaxNoOfLocalScans"
                          " explicitly to go higher",
                          noBatchSize,
                          oldBatchSize);
    }
  }
  cfg.put(CFG_LDM_BATCH_SIZE, noBatchSize);

  if (noOfLocalOperations == 0) {
    if (noOfOperations == 0)
      noOfLocalOperations = 11 * 32768 / 10;
    else
      noOfLocalOperations= (11 * noOfOperations) / 10;
  }

  const Uint32 noOfTCLocalScanRecords = DO_DIV(noOfLocalScanRecords,
                                               tcInstances);
  const Uint32 noOfTCScanRecords = noOfScanRecords;

  // ReservedXXX defaults to 25% of MaxNoOfXXX
  if (reservedScanRecords == 0)
  {
    reservedScanRecords = noOfScanRecords / 4;
  }
  if (reservedLocalScanRecords == 0)
  {
    /**
     * We allocate 4096 scan records per LDM instance. This affects operation
     * records in LDMs and TC threads. This consumes about 2.5 MByte of storage
     * in LDM threads and 
     */
#if (defined(VM_TRACE)) || (defined(ERROR_INSERT))
    reservedLocalScanRecords = 16;
#else
    reservedLocalScanRecords = 4096 * ldmInstances;
#endif
  }
  if (reservedOperations == 0)
  {
    /**
     * We reserve 50 MByte per LDM instance for use as operation
     * records. The performance drops significantly in using the
     * non-reserved operation records, so we ensure that we use a
     * large portion of the memory for reserved operation records.
     */
#if (defined(VM_TRACE)) || (defined(ERROR_INSERT))
    reservedOperations = 1000 * ldmInstances;
#else
    reservedOperations = 100000 * ldmInstances;
#endif
  }
  if (reservedTransactions == 0)
  {
    reservedTransactions = noOfTransactions / 4;
  }
  if (reservedIndexOperations == 0)
  {
    reservedIndexOperations = noOfIndexOperations / 4;
  }
  if (reservedTriggerOperations == 0)
  {
    reservedTriggerOperations = noOfTriggerOperations / 4;
  }
  if (reservedTransactionBufferBytes == 0)
  {
    reservedTransactionBufferBytes = transactionBufferBytes / 4;
  }

  noOfLocalOperations = DO_DIV(noOfLocalOperations, ldmInstances);
  noOfLocalScanRecords = DO_DIV(noOfLocalScanRecords, ldmInstances);

  Uint32 noFragPerTable = 0;
  Uint32 numReplicas = 0;
  Uint32 numFragmentsTotal = 0;
  Uint32 numFragmentsPerNodePerLdm = 0;
  if (classicFragmentation == 0 ||
      automaticThreadConfig == 1)
  {
    numFragmentsPerNodePerLdm =
      (partitionsPerNode + 1) * noOfMetaTables * noOfReplicas / ldmInstances;
    noFragPerTable= (noOfDBNodes * (partitionsPerNode + 1));
    numReplicas = noOfMetaTables * (partitionsPerNode + 1) * noOfReplicas;
  }
  else
  {
    noFragPerTable= noOfDBNodes * ldmInstances;
    numFragmentsPerNodePerLdm =
      noOfMetaTables * NO_OF_FRAG_PER_NODE * noOfReplicas;
    numReplicas = NO_OF_FRAG_PER_NODE * noOfMetaTables *
	          noOfDBNodes * noOfReplicas * ldmInstances;
  }
  numFragmentsTotal *= noOfNodes;
  {
    Uint32 noOfAccTables= noOfMetaTables/*noOfTables+noOfUniqueHashIndexes*/;
    /**
     * Acc Size Alt values
     */
    // Can keep 65536 pages (= 0.5 GByte)
    cfg.put(CFG_ACC_FRAGMENT, numFragmentsPerNodePerLdm);
    
    /*-----------------------------------------------------------------------*/
    // The extra operation records added are used by the scan and node 
    // recovery process. 
    // Node recovery process will have its operations dedicated to ensure
    // that they never have a problem with allocation of the operation record.
    // The remainder are allowed for use by the scan processes.
    /*-----------------------------------------------------------------------*/
    /**
     * We add an extra 150 operations, 100 of those are dedicated to DBUTIL
     * interactions and LCP and Backup scans. The remaining 50 are
     * non-dedicated things for local usage.
     */
#define EXTRA_LOCAL_OPERATIONS 150
    Uint32 local_acc_operations = 
#if (defined(VM_TRACE)) || (defined(ERROR_INSERT))
                               16;
#else
                               4096;
#endif
    local_acc_operations = MIN(local_acc_operations, UINT28_MAX);
    cfg.put(CFG_ACC_OP_RECS, local_acc_operations);

#ifdef VM_TRACE
    g_eventLogger->info(
        "reservedOperations: %u, reservedLocalScanRecords: %u,"
        " NODE_RECOVERY_SCAN_OP_RECORDS: %u",
        reservedOperations, reservedLocalScanRecords,
        NODE_RECOVERY_SCAN_OP_RECORDS);
#endif
    Uint32 ldm_reserved_operations =
            (reservedOperations / ldmInstances) + EXTRA_LOCAL_OPERATIONS +
            (reservedLocalScanRecords / ldmInstances) +
            NODE_RECOVERY_SCAN_OP_RECORDS;
    ldm_reserved_operations = MIN(ldm_reserved_operations, UINT28_MAX);
    cfg.put(CFG_LDM_RESERVED_OPERATIONS, ldm_reserved_operations);

    cfg.put(CFG_ACC_TABLE, noOfAccTables);
    
    cfg.put(CFG_ACC_RESERVED_SCAN_RECORDS,
            reservedLocalScanRecords / ldmInstances);
    cfg.put(CFG_TUP_RESERVED_SCAN_RECORDS,
            reservedLocalScanRecords / ldmInstances);
    cfg.put(CFG_TUX_RESERVED_SCAN_RECORDS,
            reservedLocalScanRecords / ldmInstances);
    cfg.put(CFG_LQH_RESERVED_SCAN_RECORDS,
            reservedLocalScanRecords / ldmInstances);
  }
  
  {
    /**
     * Dih Size Alt values
     */

    cfg.put(CFG_DIH_FRAG_CONNECT, 
	    noFragPerTable *  noOfMetaTables);
    
    cfg.put(CFG_DIH_REPLICAS, numReplicas);

    cfg.put(CFG_DIH_TABLE, 
	    noOfMetaTables);
  }
  
  {
    /**
     * Lqh Size Alt values
     */
    cfg.put(CFG_LQH_FRAG, numFragmentsPerNodePerLdm);
    
    cfg.put(CFG_LQH_TABLE, 
	    noOfMetaTables);
  }
  
  {
    /**
     * Spj Size Alt values
     */
    cfg.put(CFG_SPJ_TABLE, 
	    noOfMetaTables);
  }
  
  {
    /**
     * Tc Size Alt values
     */
    if (maxOpsPerTrans == ~(Uint32)0)
    {
      maxOpsPerTrans = noOfOperations;
    }
    if (maxOpsPerTrans > noOfOperations)
    {
      BaseString::snprintf(
          buf,
          sizeof(buf),
          "Config param MaxDMLOperationsPerTransaction(%u) must not be bigger"
          " than available failover records given by "
          "MaxNoOfConcurrentOperations(%u)\n",
          maxOpsPerTrans,
          noOfOperations);
      ERROR_SET(fatal, NDBD_EXIT_INVALID_CONFIG, msg, buf);
    }

    cfg.put(CFG_TC_TARGET_FRAG_LOCATION, Uint32(0));
    cfg.put(CFG_TC_MAX_FRAG_LOCATION, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_FRAG_LOCATION, Uint32(0));

    cfg.put(CFG_TC_TARGET_SCAN_FRAGMENT, noOfTCLocalScanRecords);
    cfg.put(CFG_TC_MAX_SCAN_FRAGMENT, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_SCAN_FRAGMENT, reservedLocalScanRecords / tcInstances);

    cfg.put(CFG_TC_TARGET_SCAN_RECORD, noOfTCScanRecords);
    cfg.put(CFG_TC_MAX_SCAN_RECORD, noOfTCScanRecords);
    cfg.put(CFG_TC_RESERVED_SCAN_RECORD, reservedScanRecords / tcInstances);

    cfg.put(CFG_TC_TARGET_CONNECT_RECORD, noOfOperations + 16 + noOfTransactions);
    cfg.put(CFG_TC_MAX_CONNECT_RECORD, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_CONNECT_RECORD, reservedOperations);

    const Uint32 takeOverOperations = noOfOperations +
                                      EXTRA_OPERATIONS_FOR_FIRST_TRANSACTION;
    cfg.put(CFG_TC_TARGET_TO_CONNECT_RECORD, takeOverOperations);
    cfg.put(CFG_TC_MAX_TO_CONNECT_RECORD, takeOverOperations);
    cfg.put(CFG_TC_RESERVED_TO_CONNECT_RECORD, takeOverOperations);

    cfg.put(CFG_TC_TARGET_COMMIT_ACK_MARKER, noOfTransactions);
    cfg.put(CFG_TC_MAX_COMMIT_ACK_MARKER, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_COMMIT_ACK_MARKER, reservedTransactions / tcInstances);

    cfg.put(CFG_TC_TARGET_TO_COMMIT_ACK_MARKER, Uint32(0));
    cfg.put(CFG_TC_MAX_TO_COMMIT_ACK_MARKER, Uint32(0));
    cfg.put(CFG_TC_RESERVED_TO_COMMIT_ACK_MARKER, Uint32(0));

    cfg.put(CFG_TC_TARGET_INDEX_OPERATION, noOfIndexOperations);
    cfg.put(CFG_TC_MAX_INDEX_OPERATION, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_INDEX_OPERATION, reservedIndexOperations / tcInstances);

    cfg.put(CFG_TC_TARGET_API_CONNECT_RECORD, noOfTransactions);
    cfg.put(CFG_TC_MAX_API_CONNECT_RECORD, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_API_CONNECT_RECORD, reservedTransactions / tcInstances);

    cfg.put(CFG_TC_TARGET_TO_API_CONNECT_RECORD, reservedTransactions);
    cfg.put(CFG_TC_MAX_TO_API_CONNECT_RECORD, noOfTransactions);
    cfg.put(CFG_TC_RESERVED_TO_API_CONNECT_RECORD, reservedTransactions / tcInstances);

    cfg.put(CFG_TC_TARGET_CACHE_RECORD, noOfTransactions);
    cfg.put(CFG_TC_MAX_CACHE_RECORD, noOfTransactions);
    cfg.put(CFG_TC_RESERVED_CACHE_RECORD, reservedTransactions / tcInstances);

    cfg.put(CFG_TC_TARGET_FIRED_TRIGGER_DATA, noOfTriggerOperations);
    cfg.put(CFG_TC_MAX_FIRED_TRIGGER_DATA, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_FIRED_TRIGGER_DATA, reservedTriggerOperations / tcInstances);

    cfg.put(CFG_TC_TARGET_ATTRIBUTE_BUFFER, transactionBufferBytes);
    cfg.put(CFG_TC_MAX_ATTRIBUTE_BUFFER, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_ATTRIBUTE_BUFFER, reservedTransactionBufferBytes / tcInstances);

    cfg.put(CFG_TC_TARGET_COMMIT_ACK_MARKER_BUFFER, 2 * noOfTransactions);
    cfg.put(CFG_TC_MAX_COMMIT_ACK_MARKER_BUFFER, UINT32_MAX);
    cfg.put(CFG_TC_RESERVED_COMMIT_ACK_MARKER_BUFFER, 2 * reservedTransactions / tcInstances);

    cfg.put(CFG_TC_TARGET_TO_COMMIT_ACK_MARKER_BUFFER, Uint32(0));
    cfg.put(CFG_TC_MAX_TO_COMMIT_ACK_MARKER_BUFFER, Uint32(0));
    cfg.put(CFG_TC_RESERVED_TO_COMMIT_ACK_MARKER_BUFFER, Uint32(0));

    cfg.put(CFG_TC_TABLE, 
	    noOfMetaTables);
  }
  
  {
    /**
     * Tup Size Alt values
     */
    cfg.put(CFG_TUP_FRAG, numFragmentsPerNodePerLdm);
    
    cfg.put(CFG_TUP_TABLE, 
	    noOfMetaTables);
  }

  {
    /**
     * Tux Size Alt values
     */
    cfg.put(CFG_TUX_INDEX, 
	    noOfMetaTables /*noOfOrderedIndexes*/);
    
    cfg.put(CFG_TUX_FRAGMENT, numFragmentsPerNodePerLdm);
    
    cfg.put(CFG_TUX_ATTRIBUTE, 
	    noOfMetaTables * 4);
  }

  require(cfg.commit(true));
  m_ownConfig = (ndb_mgm_configuration*)cfg.getConfigValues();
  m_ownConfigIterator = ndb_mgm_create_configuration_iterator(m_ownConfig, 0);
}

void
Configuration::setAllRealtimeScheduler()
{
  Uint32 i;
  for (i = 0; i < threadInfo.size(); i++)
  {
    if (threadInfo[i].type != NotInUse)
    {
      if (setRealtimeScheduler(threadInfo[i].pThread,
                               threadInfo[i].type,
                               _realtimeScheduler,
                               false))
        return;
    }
  }
}

void
Configuration::setAllLockCPU(bool exec_thread)
{
  Uint32 i;
  for (i = 0; i < threadInfo.size(); i++)
  {
    if (threadInfo[i].type == NotInUse)
      continue;

    bool run = 
      (exec_thread && threadInfo[i].type == BlockThread) ||
      (!exec_thread && threadInfo[i].type != BlockThread);

    if (run)
    {
      setLockCPU(threadInfo[i].pThread, threadInfo[i].type);
    }
  }
}

int
Configuration::setRealtimeScheduler(NdbThread* pThread,
                                    enum ThreadTypes type,
                                    bool real_time,
                                    bool init)
{
  /*
    We ignore thread characteristics on platforms where we cannot
    determine the thread id.
  */
  if (!init || real_time)
  {
    int error_no;
    bool high_prio = !((type == BlockThread) ||
                       (type == ReceiveThread) ||
                       (type == SendThread));
    if ((error_no = NdbThread_SetScheduler(pThread, real_time, high_prio)))
    {
      //Warning, no permission to set scheduler
      if (init)
      {
        g_eventLogger->info("Failed to set real-time prio on tid = %d,"
                            " error_no = %d",
                            NdbThread_GetTid(pThread), error_no);
        abort(); /* Fail on failures at init */
      }
      return 1;
    }
    else if (init)
    {
      g_eventLogger->info("Successfully set real-time prio on tid = %d",
                          NdbThread_GetTid(pThread));
    }
  }
  return 0;
}

int
Configuration::setLockCPU(NdbThread * pThread,
                          enum ThreadTypes type)
{
  int res = 0;
  if (type != BlockThread &&
      type != SendThread &&
      type != ReceiveThread)
  {
    if (type == NdbfsThread)
    {
      /*
       * NdbfsThread (IO threads).
       */
      res = m_thr_config.do_bind_io(pThread);
    }
    else
    {
      /*
       * WatchDogThread, SocketClientThread, SocketServerThread
       */
      res = m_thr_config.do_bind_watchdog(pThread);
    }
  }
  else if (!NdbIsMultiThreaded())
  {
    BlockNumber list[1];
    list[0] = numberToBlock(TRPMAN, 1);
    res = m_thr_config.do_bind(pThread, list, 1);
  }

  if (res != 0)
  {
    if (res > 0)
    {
      g_eventLogger->info("Locked tid = %d to CPU ok",
                          NdbThread_GetTid(pThread));
      return 0;
    }
    else
    {
      g_eventLogger->info("Failed to lock tid = %d to CPU, error_no = %d",
                          NdbThread_GetTid(pThread), (-res));
#ifndef HAVE_MAC_OS_X_THREAD_INFO
      abort(); /* We fail when failing to lock to CPUs */
#endif
      return 1;
    }
  }

  return 0;
}

int
Configuration::setThreadPrio(NdbThread * pThread,
                             enum ThreadTypes type)
{
  int res = 0;
  unsigned thread_prio = 0;
  if (type != BlockThread &&
      type != SendThread &&
      type != ReceiveThread)
  {
    if (type == NdbfsThread)
    {
      /*
       * NdbfsThread (IO threads).
       */
      res = m_thr_config.do_thread_prio_io(pThread, thread_prio);
    }
    else
    {
      /*
       * WatchDogThread, SocketClientThread, SocketServerThread
       */
      res = m_thr_config.do_thread_prio_watchdog(pThread, thread_prio);
    }
  }
  else if (!NdbIsMultiThreaded())
  {
    BlockNumber list[1];
    list[0] = numberToBlock(TRPMAN, 1);
    res = m_thr_config.do_thread_prio(pThread, list, 1, thread_prio);
  }

  if (res != 0)
  {
    if (res > 0)
    {
      g_eventLogger->info("Set thread prio to %u for tid: %d ok",
                          thread_prio, NdbThread_GetTid(pThread));
      return 0;
    }
    else
    {
      g_eventLogger->info("Failed to set thread prio to %u for tid: %d,"
                          " error_no = %d",
                          thread_prio,
                          NdbThread_GetTid(pThread),
                          (-res));
      abort(); /* We fail when failing to set thread prio */
      return 1;
    }
  }
  return 0;
}

bool
Configuration::get_io_real_time() const
{
  return m_thr_config.do_get_realtime_io();
}

const char*
Configuration::get_type_string(enum ThreadTypes type)
{
  const char *type_str;
  switch (type)
  {
    case WatchDogThread:
      type_str = "WatchDogThread";
      break;
    case SocketServerThread:
      type_str = "SocketServerThread";
      break;
    case SocketClientThread:
      type_str = "SocketClientThread";
      break;
    case NdbfsThread:
      type_str = "NdbfsThread";
      break;
    case BlockThread:
      type_str = "BlockThread";
      break;
    case SendThread:
      type_str = "SendThread";
      break;
    case ReceiveThread:
      type_str = "ReceiveThread";
      break;
    default:
      type_str = NULL;
      abort();
  }
  return type_str;
}

Uint32
Configuration::addThread(struct NdbThread* pThread,
                         enum ThreadTypes type,
                         bool single_threaded)
{
  const char *type_str;
  Uint32 i;
  NdbMutex_Lock(threadIdMutex);
  for (i = 0; i < threadInfo.size(); i++)
  {
    if (threadInfo[i].type == NotInUse)
      break;
  }
  if (i == threadInfo.size())
  {
    struct ThreadInfo tmp;
    threadInfo.push_back(tmp);
  }
  threadInfo[i].pThread = pThread;
  threadInfo[i].type = type;
  NdbMutex_Unlock(threadIdMutex);

  type_str = get_type_string(type);

  bool real_time;
  if (single_threaded)
  {
    setRealtimeScheduler(pThread, type, _realtimeScheduler, true);
  }
  else if (type == WatchDogThread ||
           type == SocketClientThread ||
           type == SocketServerThread ||
           type == NdbfsThread)
  {
    if (type != NdbfsThread)
    {
      /**
       * IO threads are handled internally in NDBFS with
       * regard to setting real time properties on the
       * IO thread.
       *
       * WatchDog, SocketServer and SocketClient have no
       * special handling of real-time breaks since we
       * don't expect these threads to long without
       * breaks.
       */
      real_time = m_thr_config.do_get_realtime_wd();
      setRealtimeScheduler(pThread, type, real_time, true);
    }
    /**
     * main threads are set in ThreadConfig::ipControlLoop
     * as it's handled differently with mt
     */
    setLockCPU(pThread, type);
  }
  /**
   * All other thread types requires special handling of real-time
   * property which is handled in the thread itself for multithreaded
   * nbdmtd process.
   */
  return i;
}

void
Configuration::removeThread(struct NdbThread *pThread)
{
  NdbMutex_Lock(threadIdMutex);
  for (Uint32 i = 0; i < threadInfo.size(); i++)
  {
    if (threadInfo[i].pThread == pThread)
    {
      threadInfo[i].pThread = 0;
      threadInfo[i].type = NotInUse;
      break;
    }
  }
  NdbMutex_Unlock(threadIdMutex);
}

void
Configuration::yield_main(Uint32 index, bool start)
{
  if (_realtimeScheduler)
  {
    if (start)
      setRealtimeScheduler(threadInfo[index].pThread,
                           threadInfo[index].type,
                           false,
                           false);
    else
      setRealtimeScheduler(threadInfo[index].pThread,
                           threadInfo[index].type,
                           true,
                           false);
  }
}

void
Configuration::initThreadArray()
{
  NdbMutex_Lock(threadIdMutex);
  for (Uint32 i = 0; i < threadInfo.size(); i++)
  {
    threadInfo[i].pThread = 0;
    threadInfo[i].type = NotInUse;
  }
  NdbMutex_Unlock(threadIdMutex);
}

template class Vector<struct ThreadInfo>;

