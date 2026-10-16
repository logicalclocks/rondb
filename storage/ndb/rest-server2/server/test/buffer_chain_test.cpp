/*
 * Copyright (C) 2026 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

/*
 * RONDB-1144: the buffer chain and the response writer's spill path,
 * against a small explicitly initialised pool. Cluster-free.
 */

#include <gtest/gtest.h>

#include <NdbMutex.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include "buffer_chain.hpp"
#include "buffer_manager.hpp"
#include "config_structs.hpp"
#include "db_operations/pk/pkr_response.hpp"
#include "feature_store_ctrl.hpp"
#include "rdrs_const.h"
#include "rdrs_dal.h"

/* Owned by main.cc in the server binary; test binaries provide their own
 * (same pattern as error_handling_test.cpp). */
NdbMutex *globalConfigsMutex = nullptr;

namespace {

constexpr Uint32 kReq = 16 * 1024;
constexpr Uint32 kResp = 64 * 1024;
constexpr Uint32 kPrealloc = 2;
constexpr Uint32 kMax = 16;
constexpr Uint32 kReserve = 1024;

class BufferChainTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    globalConfigs.internal.startReserveOverride = kReserve;
    rsBufferArrayManager.init(kReq, kResp, kPrealloc, kMax, 50, 0, 1ULL << 30);
  }
  static void TearDownTestSuite() {
    rsBufferArrayManager.shutdown();
    globalConfigs.internal.startReserveOverride = 0;
  }
  static Int64 resp_in_use() {
    BufferPoolStats st{};
    rsBufferArrayManager.get_resp_buffers_stats(st);
    return st.in_use;
  }
  static Int64 req_in_use() {
    BufferPoolStats st{};
    rsBufferArrayManager.get_req_buffers_stats(st);
    return st.in_use;
  }
};

}  // namespace

TEST_F(BufferChainTest, ViewsPackUntilTheReserveThenTakeANewOwner) {
  {
    RS_BufferChain chain(RS_BufferChain::RESP, 4);
    ASSERT_EQ(chain.take_first().http_code, SUCCESS);
    EXPECT_EQ(chain.owner_count(), 1U);
    EXPECT_EQ(chain.view(0).buffer, chain.owner(0).buffer);
    EXPECT_EQ(chain.view(0).size, kResp);
    EXPECT_TRUE(chain.starts_at_offset_zero(0));

    // Pretend op 0 wrote 100 bytes: the next view follows it, 4-aligned.
    chain.finish_op(0, 100, false, 0);
    ASSERT_EQ(chain.next_op_view(1).http_code, SUCCESS);
    EXPECT_EQ(chain.owner_of(1), 0U);
    EXPECT_EQ(chain.view(1).buffer, chain.owner(0).buffer + 100);
    EXPECT_EQ(chain.view(1).size, kResp - 100);
    EXPECT_FALSE(chain.starts_at_offset_zero(1));

    // Fill the owner to less than the reserve: op 2 goes to a new owner.
    chain.finish_op(1, kResp - 100 - (kReserve / 2), false, 0);
    ASSERT_EQ(chain.next_op_view(2).http_code, SUCCESS);
    EXPECT_EQ(chain.owner_count(), 2U);
    EXPECT_EQ(chain.owner_of(2), 1U);
    EXPECT_TRUE(chain.starts_at_offset_zero(2));
    EXPECT_EQ(resp_in_use(), 2);
  }
  // The destructor returned both owners.
  EXPECT_EQ(resp_in_use(), 0);
}

/* The request side knows each operation's upper bound before writing:
 * when it does not fit behind the previous operation the view starts in
 * a fresh owner even though the reserve rule alone would have packed it,
 * which is how a lowered start reserve stays harmless for requests. */
TEST_F(BufferChainTest, KnownNeedLargerThanTheReserveTakesANewOwner) {
  {
    RS_BufferChain chain(RS_BufferChain::REQ, 3);
    ASSERT_EQ(chain.take_first().http_code, SUCCESS);
    chain.advance(kReq - (2 * kReserve));
    // 2 * kReserve is free: enough for the reserve, not for 3 * kReserve.
    ASSERT_EQ(chain.next_op_view(1, kReserve).http_code, SUCCESS);
    EXPECT_EQ(chain.owner_of(1), 0U);
    ASSERT_EQ(chain.next_op_view(2, 3 * kReserve).http_code, SUCCESS);
    EXPECT_EQ(chain.owner_of(2), 1U);
    EXPECT_TRUE(chain.starts_at_offset_zero(2));
    EXPECT_EQ(chain.view(2).size, kReq);
  }
  EXPECT_EQ(req_in_use(), 0);
}

TEST_F(BufferChainTest, ContinuationsAppendAndRestartAndResetReturnBuffers) {
  {
    RS_BufferChain chain(RS_BufferChain::RESP, 3);
    ASSERT_EQ(chain.take_first().http_code, SUCCESS);
    Uint32 owner = 0;
    ASSERT_EQ(chain.take_continuation(&owner).http_code, SUCCESS);
    EXPECT_EQ(owner, 1U);
    ASSERT_EQ(chain.take_continuation(&owner).http_code, SUCCESS);
    EXPECT_EQ(owner, 2U);
    EXPECT_EQ(chain.cur(), 2U);
    EXPECT_EQ(chain.head(), 0U);
    // The spilled op ended 500 bytes into the last continuation.
    chain.finish_op(0, 300, true, 500);
    EXPECT_EQ(chain.head(), 500U);
    ASSERT_EQ(chain.next_op_view(1).http_code, SUCCESS);
    EXPECT_EQ(chain.owner_of(1), 2U);
    EXPECT_EQ(chain.view(1).buffer, chain.owner(2).buffer + 500);

    // Rule 5: op 1 restarts at offset 0 of a fresh owner.
    ASSERT_EQ(chain.restart_op_in_fresh_buffer(1).http_code, SUCCESS);
    EXPECT_EQ(chain.owner_count(), 4U);
    EXPECT_EQ(chain.owner_of(1), 3U);
    EXPECT_TRUE(chain.starts_at_offset_zero(1));
    EXPECT_EQ(resp_in_use(), 4);

    // create_response starts over: only the first owner stays.
    chain.reset_to_first_buffer();
    EXPECT_EQ(chain.owner_count(), 1U);
    EXPECT_EQ(resp_in_use(), 1);
    EXPECT_EQ(chain.view(0).buffer, chain.owner(0).buffer);
    EXPECT_EQ(chain.head(), 0U);
  }
  EXPECT_EQ(resp_in_use(), 0);
}

TEST_F(BufferChainTest, PoolCapIsReportedAs503AndNothingLeaks) {
  {
    RS_BufferChain chain(RS_BufferChain::REQ, 1);
    ASSERT_EQ(chain.take_first().http_code, SUCCESS);
    Uint32 taken = 1;
    Uint32 owner = 0;
    RS_Status status = RS_OK;
    while (taken < kMax + 2) {
      status = chain.take_continuation(&owner);
      if (status.http_code != SUCCESS) {
        break;
      }
      taken++;
    }
    EXPECT_EQ(status.http_code, 503);
    EXPECT_EQ(taken, kMax);
    EXPECT_EQ(req_in_use(), static_cast<Int64>(kMax));
  }
  EXPECT_EQ(req_in_use(), 0);
}

/* A response whose raw column is larger than its view spills into
 * continuation buffers, fills each one to its last byte, and records the
 * total length and the virtual offset of its first byte. */
TEST_F(BufferChainTest, ResponseSpillsAcrossBuffersAndKeepsOffsets) {
  {
    RS_BufferChain chain(RS_BufferChain::RESP, 2);
    ASSERT_EQ(chain.take_first().http_code, SUCCESS);
    PKRResponse resp(chain.view(0), &chain, 0);
    ASSERT_EQ(resp.SetOperationID("op").http_code, SUCCESS);
    ASSERT_EQ(resp.SetNoOfColumns(3).http_code, SUCCESS);
    // The writer sets the status before any column record (the status
    // message lives in the home view, which a spill fills to its last
    // byte), so the test keeps that order.
    ASSERT_EQ(resp.SetStatus(SUCCESS, "OK").http_code, SUCCESS);
    resp.SetSlot(0);
    ASSERT_EQ(resp.Append_i64(42).http_code, SUCCESS);

    // Column 1: a raw value of 2.5 buffers, written in uneven chunks.
    const Uint32 total = (kResp * 2) + (kResp / 2);
    std::vector<char> value(total);
    for (Uint32 i = 0; i < total; i++) {
      value[i] = static_cast<char>(i * 7);
    }
    resp.SetSlot(1);
    ASSERT_EQ(resp.BeginRawColumn(RDRS_BINARY_DATATYPE).http_code, SUCCESS);
    const Uint32 home_offset = resp.GetWriteHeader();
    Uint32 off = 0;
    const Uint32 chunks[] = {1000, 40000, 30000, 7, kResp, 9000};
    for (Uint32 chunk : chunks) {
      const Uint32 take = std::min(chunk, total - off);
      ASSERT_EQ(resp.AppendRawWithSpill(value.data() + off, take).http_code,
                SUCCESS);
      off += take;
      if (off == total) {
        break;
      }
    }
    while (off < total) {
      const Uint32 take = std::min(12345U, total - off);
      ASSERT_EQ(resp.AppendRawWithSpill(value.data() + off, take).http_code,
                SUCCESS);
      off += take;
    }
    resp.SetBlobLen(1, total);
    EXPECT_TRUE(resp.Spilled());
    EXPECT_FALSE(resp.Overflowed());

    // Column 2: a second raw value starts inside a continuation buffer, so
    // its offset is past the home view's end.
    resp.SetSlot(2);
    ASSERT_EQ(resp.BeginRawColumn(RDRS_BINARY_DATATYPE).http_code, SUCCESS);
    const char tail[] = "tail";
    ASSERT_EQ(resp.AppendRawWithSpill(tail, 4).http_code, SUCCESS);
    resp.SetBlobLen(2, 4);
    Uint32 home_len = 0;
    ASSERT_EQ(resp.Close(home_len).http_code, SUCCESS);

    // Home view: the first segment fills it to the last byte.
    const Uint32 *hdr = reinterpret_cast<const Uint32 *>(chain.view(0).buffer);
    EXPECT_EQ(hdr[PK_RESP_LENGTH_IDX], home_len);
    const Uint32 *table = reinterpret_cast<const Uint32 *>(
        chain.view(0).buffer + hdr[PK_RESP_COLS_IDX]);
    EXPECT_EQ(table[0], 3U);
    EXPECT_EQ(table[1 + (4 * 1) + 0], total);        // len of column 1
    EXPECT_EQ(table[1 + (4 * 1) + 1], home_offset);  // addr of column 1
    EXPECT_EQ(table[1 + (4 * 2) + 0], 4U);           // len of column 2
    const Uint32 first_seg = kResp - home_offset;
    // Reassemble column 1 from the home view tail and the continuations.
    std::vector<char> got;
    got.insert(got.end(), chain.view(0).buffer + home_offset,
               chain.view(0).buffer + kResp);
    Uint32 remaining = total - first_seg;
    for (Uint32 own = 1; remaining > 0; own++) {
      ASSERT_LT(own, chain.owner_count());
      const Uint32 take = std::min(kResp, remaining);
      got.insert(got.end(), chain.owner(own).buffer,
                 chain.owner(own).buffer + take);
      remaining -= take;
    }
    ASSERT_EQ(got.size(), value.size());
    EXPECT_EQ(memcmp(got.data(), value.data(), total), 0);
    // Column 2 sits right after column 1 in the last continuation.
    const Uint32 virt2 = table[1 + (4 * 2) + 1];
    EXPECT_EQ(virt2, kResp + total - first_seg);
    // home view + 2 continuations: total - first_seg = 1.5 R + home_offset.
    EXPECT_EQ(chain.owner_count(), 3U);
  }
  EXPECT_EQ(resp_in_use(), 0);
}

/* Without a chain a spill is an overflow, which is what a response that
 * cannot grow reports. */
TEST_F(BufferChainTest, SpillWithoutChainOverflows) {
  std::vector<char> storage(4096);
  RS_Buffer view{storage.data(), 4096};
  PKRResponse resp(view, nullptr, 0);
  ASSERT_EQ(resp.SetNoOfColumns(1).http_code, SUCCESS);
  resp.SetSlot(0);
  ASSERT_EQ(resp.BeginRawColumn(RDRS_BINARY_DATATYPE).http_code, SUCCESS);
  std::vector<char> value(8192, 'x');
  EXPECT_NE(resp.AppendRawWithSpill(value.data(), 8192).http_code, SUCCESS);
  EXPECT_TRUE(resp.Overflowed());
}

/* A value that spills while the response is written can run the pool dry
 * mid-batch. The writer reports 503 "buffer pool exhausted", and the
 * feature store controllers pass a batch read's error through
 * TranslateRonDbError, which must keep the 503 (it turned every status
 * other than 400 and 429 into 500 before). */
TEST_F(BufferChainTest, SpillExhaustionStays503ThroughFeatureStore) {
  {
    RS_BufferChain chain(RS_BufferChain::RESP, 1);
    ASSERT_EQ(chain.take_first().http_code, SUCCESS);
    PKRResponse resp(chain.view(0), &chain, 0);
    ASSERT_EQ(resp.SetOperationID("op").http_code, SUCCESS);
    ASSERT_EQ(resp.SetNoOfColumns(1).http_code, SUCCESS);
    ASSERT_EQ(resp.SetStatus(SUCCESS, "OK").http_code, SUCCESS);
    resp.SetSlot(0);
    ASSERT_EQ(resp.BeginRawColumn(RDRS_BINARY_DATATYPE).http_code, SUCCESS);
    std::vector<char> chunk(kResp, '\x01');
    RS_Status status = RS_OK;
    for (Uint32 i = 0; i < kMax + 2 && status.http_code == SUCCESS; i++) {
      status = resp.AppendRawWithSpill(chunk.data(), kResp);
    }
    ASSERT_EQ(status.http_code, 503);
    auto fsError = TranslateRonDbError(status.http_code, status.message);
    ASSERT_NE(fsError, nullptr);
    EXPECT_EQ(fsError->GetStatus(), 503);
    EXPECT_NE(fsError->Error().find("buffer pool exhausted"), std::string::npos)
        << fsError->Error();
  }
  EXPECT_EQ(resp_in_use(), 0);
}
