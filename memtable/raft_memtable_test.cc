//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memtable/raft_hash_list.h"
#include <set>
#include "memory/arena.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/coding.h"

namespace rocksdb {

typedef uint64_t Key;

void Serialize(uint64_t t, std::string* dest) {
  for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
     dest->append(
       1, static_cast<char>(
         (t >> (sizeof(uint64_t) - 1 - i) * 8) & 0xFFLL));
  }
}

class RaftMemTableTest : public testing::Test {};

TEST_F(RaftMemTableTest, InsertAndLookup) {
  const int R = 5000;
  const int L = 256;
  Random rnd(1000);
  std::set<Key> keys;
  Arena arena;
  RaftHashList table(&arena, "ab", 1024, 1);

  for (int i = 0; i < R; i++) {
    uint32_t region = rnd.Next() % R;
    uint32_t start_index = (rnd.Next() % L) << 16;
    uint64_t key = static_cast<uint64_t>(region) << 32 | start_index;
    if (keys.insert(key).second) {
      for (int j = 0; j < L; j ++) {
        std::string log = "ab";
        Serialize(region, &log);
        log.append(1, static_cast<char>(1));
        Serialize(j + start_index, &log);
        const uint32_t encoded_len = VarintLength(log.length()) + log.length();
        char* buf = table.Allocate(encoded_len);
        char* p = EncodeVarint32(buf, log.length());
        memcpy(p, log.data(), log.length());
        table.Insert(buf);
      }
    }
  }
  for (int i = 0; i < R; i++) {
    uint32_t region = rnd.Next() % R;
    uint32_t start_index = (rnd.Next() % L) << 16;
    uint64_t key = static_cast<uint64_t>(region) << 32 | start_index;
    if (keys.count(key) > 0) {
      for (int j = 0; j < L; j ++) {
        std::string log = "ab";
        Serialize(region, &log);
        log.append(1, static_cast<char>(1));
        Serialize(j + start_index, &log);
        const uint32_t encoded_len = VarintLength(log.length()) + log.length();
        char* buf = new char[encoded_len];
        char* p = EncodeVarint32(buf, log.length());
        memcpy(p, log.data(), log.length());
        delete []buf;
      }
    }
  }
}


}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

