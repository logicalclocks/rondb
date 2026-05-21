/*
   Copyright (c) 2004, 2025, Oracle and/or its affiliates.

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

#include <ConfigValues.hpp>
#include <NdbOut.hpp>
#include <mgmapi/mgmapi_config_parameters.h>
#include <string.h>
#include "util/require.h"

void print(Uint32 i, ConfigValues::ConstIterator &cf) {
  ndbout_c("---");
  for (Uint32 j = 2; j <= 7; j++) {
    switch (cf.getTypeOf(j)) {
      case ConfigSection::IntTypeId:
        ndbout_c("Node %d : CFG(%d) : %d", i, j, cf.get(j, 999));
        break;
      case ConfigSection::Int64TypeId:
        ndbout_c("Node %d : CFG(%d) : %lld (64)", i, j, cf.get64(j, 999));
        break;
      case ConfigSection::StringTypeId:
        ndbout_c("Node %d : CFG(%d) : %s", i, j, cf.get(j, "<NOT FOUND>"));
        break;
      default:
        ndbout_c("Node %d : CFG(%d) : TYPE: %d", i, j, cf.getTypeOf(j));
    }
  }
}

void print(Uint32 i, ConfigValues &_cf) {
  ConfigValues::ConstIterator cf(_cf);
  print(i, cf);
}

void print(ConfigValues &_cf) {
  ConfigValues::ConstIterator cf(_cf);
  Uint32 i = 0;
  while (cf.openSection(CONFIG_SECTION_NODE, i)) {
    print(i, cf);
    cf.closeSection();
    i++;
  }
}

static void create_rdma_config(ConfigValuesFactory &cvf) {
  require(cvf.begin());
  require(cvf.createSection(CONFIG_SECTION_SYSTEM, 0));
  require(cvf.put(CFG_SYS_CONFIG_GENERATION, 1));
  cvf.closeSection();

  require(cvf.createSection(CONFIG_SECTION_NODE, DATA_NODE_TYPE));
  require(cvf.put(CFG_NODE_ID, 1));
  require(cvf.put(CFG_NODE_HOST, "db1.example.com"));
  require(cvf.put(CFG_NODE_ACTIVE, 1));
  cvf.closeSection();

  require(cvf.createSection(CONFIG_SECTION_NODE, DATA_NODE_TYPE));
  require(cvf.put(CFG_NODE_ID, 2));
  require(cvf.put(CFG_NODE_HOST, "db2-new.example.com"));
  require(cvf.put(CFG_NODE_ACTIVE, static_cast<Uint32>(0)));
  cvf.closeSection();

  require(cvf.createSection(CONFIG_SECTION_NODE, API_NODE_TYPE));
  require(cvf.put(CFG_NODE_ID, 10));
  cvf.closeSection();

  require(cvf.createSection(CONFIG_SECTION_NODE, MGM_NODE_TYPE));
  require(cvf.put(CFG_NODE_ID, 20));
  cvf.closeSection();

  require(cvf.createSection(CONFIG_SECTION_CONNECTION, RDMA_TYPE));
  require(cvf.put(CFG_CONNECTION_NODE_1, 1));
  require(cvf.put(CFG_CONNECTION_NODE_2, 2));
  require(cvf.put(CFG_CONNECTION_HOSTNAME_1, "db1.example.com"));
  require(cvf.put(CFG_CONNECTION_HOSTNAME_2, "db2-new.example.com"));
  require(cvf.put(CFG_CONNECTION_SERVER_PORT, static_cast<Uint32>(0)));
  require(cvf.put(CFG_CONNECTION_NODE_ID_SERVER, 1));
  require(cvf.put(CFG_CONNECTION_SEND_SIGNAL_ID, 1));
  require(cvf.put(CFG_CONNECTION_CHECKSUM, 1));
  require(cvf.put(CFG_CONNECTION_PRESEND_CHECKSUM, static_cast<Uint32>(0)));
  require(cvf.put(CFG_RDMA_SEND_BUFFER_SIZE, 2 * 1024 * 1024));
  require(cvf.put(CFG_RDMA_RECV_BUFFER_SIZE, 2 * 1024 * 1024));
  require(cvf.put(CFG_RDMA_QUEUE_DEPTH, 64));
  require(cvf.put(CFG_RDMA_INLINE_THRESHOLD, 256));
  require(cvf.put(CFG_RDMA_COMPLETION_POLL_BUDGET, 32));
  require(cvf.put(CFG_RDMA_SPINTIME, 75));
  require(cvf.put(CFG_RDMA_DEVICE_NAME, "mlx5_0"));
  require(cvf.put(CFG_RDMA_PORT, 1));
  require(cvf.put(CFG_RDMA_GID_INDEX, static_cast<Uint32>(0)));
  require(cvf.put(CFG_RDMA_TRAFFIC_CLASS, static_cast<Uint32>(0)));
  require(cvf.put(CFG_RDMA_RETRY_COUNT, 7));
  require(cvf.put(CFG_RDMA_RNR_RETRY_COUNT, 7));
  cvf.closeSection();

  require(cvf.createSection(CONFIG_SECTION_CONNECTION, RDMA_TYPE));
  require(cvf.put(CFG_CONNECTION_NODE_1, 10));
  require(cvf.put(CFG_CONNECTION_NODE_2, 1));
  require(cvf.put(CFG_CONNECTION_HOSTNAME_1, "api.example.com"));
  require(cvf.put(CFG_CONNECTION_HOSTNAME_2, "db1.example.com"));
  require(cvf.put(CFG_CONNECTION_SERVER_PORT, static_cast<Uint32>(0)));
  require(cvf.put(CFG_CONNECTION_NODE_ID_SERVER, 1));
  require(cvf.put(CFG_CONNECTION_SEND_SIGNAL_ID, 1));
  require(cvf.put(CFG_CONNECTION_CHECKSUM, 1));
  require(cvf.put(CFG_CONNECTION_PRESEND_CHECKSUM, static_cast<Uint32>(0)));
  require(cvf.put(CFG_RDMA_SEND_BUFFER_SIZE, 2 * 1024 * 1024));
  require(cvf.put(CFG_RDMA_RECV_BUFFER_SIZE, 2 * 1024 * 1024));
  require(cvf.put(CFG_RDMA_QUEUE_DEPTH, 64));
  require(cvf.put(CFG_RDMA_INLINE_THRESHOLD, 256));
  require(cvf.put(CFG_RDMA_COMPLETION_POLL_BUDGET, 32));
  require(cvf.put(CFG_RDMA_SPINTIME, 75));
  require(cvf.put(CFG_RDMA_DEVICE_NAME, "mlx5_0"));
  require(cvf.put(CFG_RDMA_PORT, 1));
  require(cvf.put(CFG_RDMA_GID_INDEX, static_cast<Uint32>(0)));
  require(cvf.put(CFG_RDMA_TRAFFIC_CLASS, static_cast<Uint32>(0)));
  require(cvf.put(CFG_RDMA_RETRY_COUNT, 7));
  require(cvf.put(CFG_RDMA_RNR_RETRY_COUNT, 7));
  cvf.closeSection();

  require(cvf.commit(false));
}

static void verify_rdma_connection(ConfigValues::ConstIterator &iter,
                                   Uint32 expected_node1,
                                   Uint32 expected_node2,
                                   const char *expected_host1,
                                   const char *expected_host2) {
  Uint32 value = 0;
  const char *string_value = nullptr;

  require(iter.get(CFG_TYPE_OF_SECTION, &value));
  require(value == CONNECTION_TYPE_RDMA);
  require(iter.get(CFG_CONNECTION_NODE_1, &value));
  require(value == expected_node1);
  require(iter.get(CFG_CONNECTION_NODE_2, &value));
  require(value == expected_node2);
  require(iter.get(CFG_CONNECTION_HOSTNAME_1, &string_value));
  require(strcmp(string_value, expected_host1) == 0);
  require(iter.get(CFG_CONNECTION_HOSTNAME_2, &string_value));
  require(strcmp(string_value, expected_host2) == 0);
  require(iter.get(CFG_CONNECTION_SERVER_PORT, &value));
  require(value == 0);
  require(iter.get(CFG_CONNECTION_NODE_ID_SERVER, &value));
  require(value == 1);
  require(iter.get(CFG_CONNECTION_SEND_SIGNAL_ID, &value));
  require(value == 1);
  require(iter.get(CFG_CONNECTION_CHECKSUM, &value));
  require(value == 1);
  require(iter.get(CFG_CONNECTION_PRESEND_CHECKSUM, &value));
  require(value == 0);
  require(iter.get(CFG_RDMA_SEND_BUFFER_SIZE, &value));
  require(value == 2 * 1024 * 1024);
  require(iter.get(CFG_RDMA_RECV_BUFFER_SIZE, &value));
  require(value == 2 * 1024 * 1024);
  require(iter.get(CFG_RDMA_QUEUE_DEPTH, &value));
  require(value == 64);
  require(iter.get(CFG_RDMA_INLINE_THRESHOLD, &value));
  require(value == 256);
  require(iter.get(CFG_RDMA_COMPLETION_POLL_BUDGET, &value));
  require(value == 32);
  require(iter.get(CFG_RDMA_SPINTIME, &value));
  require(value == 75);
  require(iter.get(CFG_RDMA_DEVICE_NAME, &string_value));
  require(strcmp(string_value, "mlx5_0") == 0);
  require(iter.get(CFG_RDMA_PORT, &value));
  require(value == 1);
  require(iter.get(CFG_RDMA_GID_INDEX, &value));
  require(value == 0);
  require(iter.get(CFG_RDMA_TRAFFIC_CLASS, &value));
  require(value == 0);
  require(iter.get(CFG_RDMA_RETRY_COUNT, &value));
  require(value == 7);
  require(iter.get(CFG_RDMA_RNR_RETRY_COUNT, &value));
  require(value == 7);
}

static void verify_rdma_config(const ConfigValues &cfg) {
  ConfigValues::ConstIterator iter(cfg);

  require(iter.openSection(CONFIG_SECTION_NODE, 1));
  Uint32 node_id = 0;
  Uint32 is_active = 1;
  const char *hostname = nullptr;
  require(iter.get(CFG_NODE_ID, &node_id));
  require(node_id == 2);
  require(iter.get(CFG_NODE_ACTIVE, &is_active));
  require(is_active == 0);
  require(iter.get(CFG_NODE_HOST, &hostname));
  require(strcmp(hostname, "db2-new.example.com") == 0);
  iter.closeSection();

  require(iter.openSection(CONFIG_SECTION_CONNECTION, 0));
  verify_rdma_connection(iter, 1, 2, "db1.example.com",
                         "db2-new.example.com");
  iter.closeSection();

  require(iter.openSection(CONFIG_SECTION_CONNECTION, 1));
  verify_rdma_connection(iter, 10, 1, "api.example.com", "db1.example.com");
  iter.closeSection();
}

static void test_rdma_config_roundtrip() {
  ConfigValuesFactory cvf;
  create_rdma_config(cvf);
  verify_rdma_config(*cvf.m_cfg);

  {
    UtilBuffer buf;
    Uint32 l1 = cvf.m_cfg->pack_v1(buf);
    Uint32 l2 = cvf.m_cfg->get_v1_packed_size();
    require(l1 == l2);

    ConfigValuesFactory cvf2;
    require(cvf2.unpack_v1_buf(buf));
    verify_rdma_config(*cvf2.m_cfg);
  }

  {
    UtilBuffer buf;
    Uint32 l1 = cvf.m_cfg->pack_v2(buf);
    Uint32 l2 = cvf.m_cfg->get_v2_packed_size(0);
    require(l1 == l2);

    ConfigValuesFactory cvf2;
    require(cvf2.unpack_v2_buf(buf));
    verify_rdma_config(*cvf2.m_cfg);
  }
}

int main(void) {
  ndb_init();
  ConfigValuesFactory cvf;
  cvf.begin();
  cvf.createSection(CONFIG_SECTION_SYSTEM, 0);
  cvf.put(2, 12);
  cvf.put64(3, 13);
  cvf.put(4, 14);
  cvf.put64(5, 15);
  cvf.put(6, "Keso");
  cvf.put(7, "Kent");
  cvf.closeSection();

  cvf.createSection(CONFIG_SECTION_NODE, DATA_NODE_TYPE);
  cvf.put(CONFIG_NODE_ID, 1);
  cvf.put(2, 22);
  cvf.put(4, 24);
  cvf.put64(5, 25);
  cvf.put(6, "Kalle");
  cvf.put(7, "Anka");
  cvf.closeSection();

  cvf.createSection(CONFIG_SECTION_NODE, API_NODE_TYPE);
  cvf.put(CONFIG_NODE_ID, 10);
  cvf.closeSection();
  cvf.createSection(CONFIG_SECTION_NODE, MGM_NODE_TYPE);
  cvf.put(CONFIG_NODE_ID, 20);
  cvf.closeSection();

  cvf.createSection(CONFIG_SECTION_CONNECTION, TCP_TYPE);
  cvf.put(CONFIG_FIRST_NODE_ID, 1);
  cvf.put(CONFIG_SECOND_NODE_ID, 2);
  cvf.closeSection();
  cvf.commit(false);

  ndbout_c("-- print --");
  print(*cvf.m_cfg);

  ndbout_c("packed size: %d", cvf.m_cfg->get_v1_packed_size());
  ndbout_c("packed size v2: %d", cvf.m_cfg->get_v2_packed_size(0));

  ConfigValues::ConstIterator iter(*cvf.m_cfg);
  require(iter.openSection(CONFIG_SECTION_NODE, 0));
  ConfigValues *cfg2 = ConfigValuesFactory::extractCurrentSection(iter);
  cvf.closeSection();
  print(99, *cfg2);

  ndbout_c("packed size: %d", cfg2->get_v1_packed_size());
  delete cfg2;

  {
    UtilBuffer buf;
    Uint32 l1 = cvf.m_cfg->pack_v1(buf);
    Uint32 l2 = cvf.m_cfg->get_v1_packed_size();
    require(l1 == l2);

    ConfigValuesFactory cvf2;
    require(cvf2.unpack_v1_buf(buf));
    UtilBuffer buf2;
    Uint32 l3 = cvf2.m_cfg->pack_v1(buf2);
    require(l1 == l3);

    ndbout_c("unpack\n-- print --");
    print(*cvf2.m_cfg);
  }
  {
    UtilBuffer buf;
    Uint32 l1 = cvf.m_cfg->pack_v2(buf);
    Uint32 l2 = cvf.m_cfg->get_v2_packed_size(0);
    require(l1 == l2);

    ConfigValuesFactory cvf2;
    require(cvf2.unpack_v2_buf(buf));
    UtilBuffer buf2;
    Uint32 l3 = cvf2.m_cfg->pack_v2(buf2);
    require(l1 == l3);

    ndbout_c("unpack v2 \n-- print --");
    print(*cvf2.m_cfg);
  }
  test_rdma_config_roundtrip();
  ndb_end(0);
  return 0;
}
