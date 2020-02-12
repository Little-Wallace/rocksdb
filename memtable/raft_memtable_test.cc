//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <set>

#include "memory/arena.h"
#include "raft_hash_list.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/random.h"

namespace rocksdb {

typedef uint64_t Key;

class RaftMemTableTest : public testing::Test {
 protected:
  void Serialize(uint64_t t, std::string* dest) {
    for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
      dest->append(
          1, static_cast<char>((t >> (sizeof(uint64_t) - 1 - i) * 8) & 0xFFLL));
    }
  }
  bool IsEqual(const char* key, const std::string& expect) {
    return GetLengthPrefixedSlice(key) == expect;
  }
  void AssertEqual(const char* key, const std::string& expect) {
    ASSERT_TRUE(IsEqual(key, expect));
  }

  const char* EncodeKey(const std::string& key) {
    uint32_t encoded_len = VarintLength(key.length()) + key.length();
    char* buf = arena_.Allocate(encoded_len);
    char* p = EncodeVarint32(buf, key.length());
    memcpy(p, key.data(), key.length());
    return buf;
  }

  std::string MakeKey(const std::string& prefix, uint64_t region, uint8_t flag,
                      int index = -1) {
    std::string key = prefix;
    Serialize(region, &key);
    key.append(1, static_cast<char>(flag));
    if (index >= 0) {
      Serialize(static_cast<uint64_t>(index), &key);
    }
    return key;
  }

 private:
  Arena arena_;
};

TEST_F(RaftMemTableTest, InsertAndLookup) {
  const int R = 12;
  const int L = 10000;
  Random rnd(1000);
  std::set<Key> keys;
  Arena arena;
  const std::string prefix = "ab";

  RaftHashList table(&arena, prefix, 24, 1);
  std::vector<std::string> datas;

  for (int i = 0; i < R; i++) {
    uint32_t region = rnd.Next() % R;
    uint32_t start_index = rnd.Next() % R * (L + 1);
    uint64_t key = static_cast<uint64_t>(region) << 32 | start_index;
    if (keys.count(key) == 0) {
      for (int j = 0; j < L; j++) {
        std::string log = MakeKey(prefix, region, 1, j + start_index);
        Serialize(region, &log);
        table.InsertString(log);
        datas.emplace_back(std::move(log));
      }
      for (int j = 0; j < 3; j++) {
        uint8_t meta = (start_index + j) % 128 + 2;
        std::string metaKey = MakeKey(prefix, region, meta);
        Serialize(region, &metaKey);
        table.InsertString(metaKey);
        datas.emplace_back(std::move(metaKey));
      }
    }
    keys.insert(key);
  }
  for (auto it = keys.begin(); it != keys.end(); it++) {
    uint64_t key = *it;
    uint32_t region = key >> 32;
    uint32_t start_index = key & ((1LL << 32) - 1);
    for (int j = 0; j < L; j++) {
      std::string log = MakeKey(prefix, region, 1, j + start_index);
      const char* value = table.GetKey(EncodeKey(log));
      assert(value);
      Serialize(region, &log);
      AssertEqual(value, log);
    }
    RaftHashList::Iterator iter(table, 0);
    iter.Seek(EncodeKey(MakeKey(prefix, region, 1, start_index + 1)));
    for (int j = 1; j < L; j++) {
      ASSERT_TRUE(iter.Valid());
      const char* value = iter.key();
      assert(value);
      std::string log = MakeKey(prefix, region, 1, j + start_index);
      Serialize(region, &log);
      AssertEqual(value, log);
      iter.Next();
    }
    iter.SeekForPrev(
        EncodeKey(MakeKey(prefix, region, 1, start_index + L - 2)));
    for (int j = L - 2; j >= 0; j--) {
      ASSERT_TRUE(iter.Valid());
      const char* value = iter.key();
      assert(value);
      std::string log = MakeKey(prefix, region, 1, j + start_index);
      Serialize(region, &log);
      AssertEqual(value, log);
      iter.Prev();
    }
  }
  std::sort(datas.begin(), datas.end());
  datas.erase(std::unique(datas.begin(), datas.end()), datas.end());

  RaftHashList::Iterator iter(table, 0);
  iter.SeekToFirst();
  for (size_t i = 0; i < datas.size(); i++) {
    ASSERT_TRUE(iter.Valid());
    const char* value = iter.key();
    assert(value);
    AssertEqual(value, datas[i]);
    iter.Next();
  }
  ASSERT_FALSE(iter.Valid());
  iter.SeekToLast();
  for (size_t i = datas.size(); i > 0; i--) {
    ASSERT_TRUE(iter.Valid());
    const char* value = iter.key();
    assert(value);
    iter.Prev();
  }
  ASSERT_FALSE(iter.Valid());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
