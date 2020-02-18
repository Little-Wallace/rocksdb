#pragma once
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//
//  RaftHashList is for special key-format as following:
//      [prefix, region_id, log_flag, log_index]
//      [prefix, region_id, meta_flag]
//
//  It only accepts keys which have the same prefix, and the key with
//  the same region would be store in a linked-list. Every node of a list
//  has a array, and RaftHashList could store the log-entry by index.
//

#include "memory/allocator.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/random.h"

#define ARRAY_SIZE 256
#define ARRAY_MASK 255
#define ARRAY_BIT 8
#define META_SIZE 256

namespace rocksdb {

void DEBUG_print(const char* name, const Slice& transformed) {
  printf("%s:", name);
  for (size_t i = 0; i < transformed.size(); ++i) {
    printf("%d,", static_cast<int>(static_cast<uint8_t>(transformed[i])));
  }
  printf("\n");
}

// If the length of key is less than sizeof(uint64_t), we add '\0' for it.
static uint64_t DeserializeBigEndian(const char* src, size_t len) {
  uint64_t result = 0;
  for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
    uint64_t bit = 0;
    if (i < len) {
      bit = static_cast<uint64_t>(static_cast<unsigned char>(src[i]));
    }
    result |= bit << ((7 - i) * 8);
  }
  return result;
}
class RaftHashList {
  struct ListNode {
    uint64_t start_index;
    std::atomic<ListNode*> next_;
    std::atomic<const char*> data[ARRAY_SIZE];
    ListNode() : start_index(0), next_(nullptr) {
      memset(data, 0, sizeof data);
    }
  };
  struct Bucket {
    uint64_t hash_id_;
    std::atomic<ListNode*> head_;
    std::atomic<ListNode*> tail_;
    Allocator* const allocator_;
    std::mutex mu_;
    std::atomic<const char*> meta[META_SIZE];

    Bucket(uint64_t hash_id, Allocator* allocator)
        : hash_id_(hash_id),
          head_(nullptr),
          tail_(nullptr),
          allocator_(allocator) {
      memset(meta, 0, sizeof meta);
    }
    void InsertLogEntry(uint64_t index, const char* key);
    const char* GetLogEntry(uint64_t index);
  };
  struct HashTableArray {
    size_t bucket_size_;
    std::atomic<Bucket*> buckets_[1];
    explicit HashTableArray(size_t bucket_size) : bucket_size_(bucket_size) {
      for (size_t i = 0; i < bucket_size; i++) {
        buckets_[i].store(nullptr, std::memory_order_relaxed);
      }
    }
    Bucket* GetBucket(uint64_t hash) const {
      for (size_t i = 0; i < bucket_size_; i++) {
        size_t j = (hash + i) % bucket_size_;
        auto bucket = buckets_[j].load(std::memory_order_acquire);
        if (bucket == nullptr || bucket->hash_id_ == hash) {
          return bucket;
        }
      }
      return nullptr;
    }
    Bucket* GetBucketOrCreate(uint64_t hash, Allocator* allocator) {
      for (size_t i = 0; i < bucket_size_; i++) {
        size_t j = (hash + i) % bucket_size_;
        Bucket* bucket = buckets_[j].load(std::memory_order_acquire);
        if (bucket == nullptr) {
          auto mem = allocator->AllocateAligned(sizeof(Bucket));
          bucket = new (mem) Bucket(hash, allocator);
          buckets_[j].store(bucket, std::memory_order_release);
          return bucket;
        } else if (bucket->hash_id_ == hash) {
          return bucket;
        }
      }
      return nullptr;
    }
  };
  const uint8_t log_flag_;
  // size_t bucket_size_;
  Allocator* const allocator_;
  const std::string prefix_;
  // std::atomic<Bucket*>* buckets_;
  std::atomic<HashTableArray*> table_;
  std::mutex mu_;

 public:
  RaftHashList(Allocator* allocator, std::string prefix, size_t bucket_size,
               uint64_t log_flag)
      : log_flag_(log_flag),
        allocator_(allocator),
        prefix_(prefix) {
    auto mem = allocator_->AllocateAligned(sizeof(HashTableArray) +
                                           sizeof(std::atomic<void*>) *
                                               (bucket_size - 1));
    // auto t = new (mem) std::atomic<Bucket*>[bucket_size];
    auto t = new (mem) HashTableArray(bucket_size);
    table_.store(t, std::memory_order_relaxed);
  }
  ~RaftHashList() {}

  static inline Slice decode_key(const char* key) {
    // The format of key is frozen and can be terated as a part of the API
    // contract. Refer to MemTable::Add for details.
    return GetLengthPrefixedSlice(key);
  }

  static inline uint8_t GetFlag(const std::string& prefix, const Slice& slice) {
    return static_cast<uint8_t>(slice[prefix.length() + sizeof(uint64_t)]);
  }

  char* Allocate(const size_t len) { return allocator_->Allocate(len); }

  static inline uint64_t GetLogIndex(const std::string& prefix,
                                     const Slice& slice) {
    // sizeof(uint64_t) + sizeof(flag) == 9
    assert(slice.size() > prefix.length() + 16);
    return DeserializeBigEndian(slice.data() + prefix.length() + 9,
                                slice.size() - prefix.length() - 9);
  }

  void InsertString(const std::string& key) {
    const uint32_t encoded_len = VarintLength(key.length()) + key.length();
    char* buf = Allocate(encoded_len);
    char* p = EncodeVarint32(buf, key.length());
    memcpy(p, key.data(), key.length());
    Insert(buf);
  }

  // Insert key into the list.
  void Insert(const char* key) {
    auto transformed = decode_key(key);
    if (!transformed.starts_with(prefix_)) {
      throw std::runtime_error("This prefix is not supported");
      return;
    }

    HashTableArray* table = table_.load(std::memory_order_seq_cst);

    uint64_t hashVal =
        DeserializeBigEndian(transformed.data() + prefix_.length(),
                             transformed.size() - prefix_.length());
    uint8_t flag = GetFlag(prefix_, transformed);
    auto bucket = table->GetBucket(hashVal);
    if (bucket == nullptr) {
      std::lock_guard<std::mutex> lockGuard(mu_);
      table = table_.load(std::memory_order_seq_cst);
      bucket = table->GetBucketOrCreate(hashVal, allocator_);
      if (bucket == nullptr) {
        auto mem = allocator_->AllocateAligned(
            sizeof(HashTableArray) +
            sizeof(std::atomic<void*>) * (table->bucket_size_ * 2 - 1));
        HashTableArray* array =
            new (mem) HashTableArray(table->bucket_size_ * 2);
        for (size_t i = 0; i < table->bucket_size_; i++) {
          Bucket* b = table->buckets_[i].load(std::memory_order_acquire);
          for (size_t j = 0; j < array->bucket_size_; j++) {
            size_t k = (j + b->hash_id_) % array->bucket_size_;
            if (array->buckets_[k].load(std::memory_order_relaxed) == nullptr) {
              array->buckets_[k].store(b, std::memory_order_relaxed);
              break;
            }
          }
        }
        bucket = array->GetBucketOrCreate(hashVal, allocator_);
        table_.store(array, std::memory_order_release);
      }
    }

    assert(bucket);
    if (flag == log_flag_) {
      assert(transformed.size() > prefix_.length() + 9);
      uint64_t index = GetLogIndex(prefix_, transformed);
      bucket->InsertLogEntry(index, key);
    } else {
      bucket->meta[flag].store(key, std::memory_order_release);
    }
  }

  const char* GetKey(const char* key) const {
    auto transformed = decode_key(key);
    if (!transformed.starts_with(prefix_)) {
      return nullptr;
    }
    uint64_t hash = DeserializeBigEndian(transformed.data() + prefix_.length(),
                                         transformed.size() - prefix_.length());

    HashTableArray* table = table_.load(std::memory_order_seq_cst);
    auto bucket = table->GetBucket(hash);
    if (bucket == nullptr) {
      return nullptr;
    }
    uint8_t flag = GetFlag(prefix_, transformed);
    if (flag == log_flag_) {
      return bucket->GetLogEntry(GetLogIndex(prefix_, transformed));
    } else {
      return bucket->meta[flag];
    }
  }

  class BucketIterator {
   public:
    BucketIterator()
        : log_flag_(0), cur_node_(nullptr), bucket_(nullptr), key_(nullptr){};
    explicit BucketIterator(Bucket* bucket, uint8_t log_flag)
        : log_flag_(log_flag),
          is_log_(false),
          index_(META_SIZE),
          bucket_(bucket),
          key_(nullptr) {
      cur_node_ = bucket_->head_.load(std::memory_order_relaxed);
    }
    void SeekToFirst();
    void SeekToLast();
    void Seek(uint64_t region, uint8_t log, uint64_t index);
    void SeekForPrev(uint64_t region, uint8_t log, uint64_t index);
    void Next();
    void Prev();
    const char* key() const { return key_; }
    bool Valid() const { return key_ != nullptr; }

   private:
    void SeekLog(uint64_t index);
    void SeekMeta(uint8_t index);
    void SeekLogForPrev(uint64_t index);
    void SeekMetaForPrev(uint8_t index);
    const char* SeekLogNodeForPrev(ListNode* node, uint64_t array_index);
    uint8_t log_flag_;
    bool is_log_;
    ListNode* cur_node_;
    size_t index_;
    Bucket* bucket_;
    const char* key_;
  };

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator(const RaftHashList& table, size_t suffix_len)
        : log_flag_(table.log_flag_),
          prefix_(table.prefix_),
          suffix_len_(suffix_len) {
      HashTableArray* array = table.table_.load(std::memory_order_acquire);
      for (size_t i = 0; i < array->bucket_size_; i++) {
        auto bucket = array->buckets_[i].load(std::memory_order_seq_cst);
        if (bucket != nullptr) {
          buckets_.push_back(bucket);
        }
      }
      cur_bucket_ = buckets_.size();
      std::sort(buckets_.begin(), buckets_.end(),
                [=](const Bucket* a, const Bucket* b) {
                  return a->hash_id_ < b->hash_id_;
                });
    }

    ~Iterator() {}

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const {
      return cur_bucket_ < buckets_.size() && iter_.Valid();
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const { return iter_.key(); }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() {
      iter_.Next();
      while (cur_bucket_ + 1 < buckets_.size() && !iter_.Valid()) {
        cur_bucket_++;
        iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
        iter_.SeekToFirst();
        if (iter_.Valid()) {
          return;
        }
      }
      if (!iter_.Valid()) {
        cur_bucket_ = buckets_.size();
      }
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() {
      iter_.Prev();
      while (cur_bucket_ > 0 && !iter_.Valid()) {
        cur_bucket_--;
        iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
        iter_.SeekToLast();
        if (iter_.Valid()) {
          return;
        }
      }
      if (!iter_.Valid()) {
        cur_bucket_ = buckets_.size();
      }
    }

    // Advance to the first entry with a key >= target
    void Seek(const char* raw_key) {
      Slice user_key = decode_key(raw_key);
      size_t data_len = user_key.size() - suffix_len_;
      if (data_len < prefix_.length() + sizeof(uint64_t)) {
        if (user_key.compare(Slice(prefix_)) <= 0) {
          SeekToFirst();
        } else {
          cur_bucket_ = buckets_.size();
        }
        return;
      }
      uint64_t hash = DeserializeBigEndian(user_key.data() + prefix_.length(),
                                           user_key.size() - prefix_.length());
      if (!CanReuseIterator(hash)) {
        cur_bucket_ = buckets_.size();
        thread_local Bucket tmp_bucket(hash, nullptr);
        tmp_bucket.hash_id_ = hash;
        auto iter = std::lower_bound(
            buckets_.begin(), buckets_.end(), &tmp_bucket,
            [](Bucket* a, Bucket* b) { return a->hash_id_ < b->hash_id_; });
        if (iter == buckets_.end()) {
          cur_bucket_ = buckets_.size();
          return;
        }
        cur_bucket_ = iter - buckets_.begin();
        iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
      }
      if (data_len == prefix_.length() + sizeof(uint64_t)) {
        iter_.SeekToFirst();
        while (cur_bucket_ + 1 < buckets_.size() && !iter_.Valid()) {
          cur_bucket_++;
          iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
          iter_.SeekToFirst();
        }
        return;
      }
      uint8_t flag = user_key[prefix_.length() + sizeof(uint64_t)];
      uint64_t index = 0;
      if (flag == log_flag_) {
        index = GetLogIndex(prefix_, user_key);
      }
      iter_.Seek(hash, flag, index);

      while (cur_bucket_ + 1 < buckets_.size() && !iter_.Valid()) {
        cur_bucket_++;
        iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
        iter_.SeekToFirst();
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const char* raw_key) {
      Slice user_key = decode_key(raw_key);
      size_t data_len = user_key.size() - suffix_len_;
      if (data_len < prefix_.length() + sizeof(uint64_t)) {
        if (user_key.compare(Slice(prefix_)) >= 0) {
          SeekToLast();
        } else {
          cur_bucket_ = buckets_.size();
        }
        return;
      }
      cur_bucket_ = buckets_.size();
      uint64_t hash = DeserializeBigEndian(user_key.data() + prefix_.length(),
                                           user_key.size() - prefix_.length());
      for (size_t i = buckets_.size(); i > 0; i--) {
        if (buckets_[i - 1]->hash_id_ <= hash) {
          cur_bucket_ = i - 1;
          break;
        }
      }
      if (cur_bucket_ >= buckets_.size()) {
        return;
      }

      iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
      if (data_len == prefix_.length() + sizeof(uint64_t)) {
        while (cur_bucket_ > 0) {
          cur_bucket_--;
          iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
          iter_.SeekToLast();
          if (iter_.Valid()) {
            return;
          }
        }
        cur_bucket_ = buckets_.size();
        return;
      }
      uint8_t flag = user_key[prefix_.length() + sizeof(uint64_t)];
      uint64_t index = 0;
      if (flag == log_flag_) {
        index = GetLogIndex(prefix_, user_key);
      }
      iter_.SeekForPrev(hash, flag, index);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() {
      cur_bucket_ = 0;
      while (cur_bucket_ < buckets_.size()) {
        iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
        iter_.SeekToFirst();
        if (iter_.Valid()) {
          break;
        }
        cur_bucket_++;
      }
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() {
      if (!buckets_.empty()) {
        cur_bucket_ = buckets_.size() - 1;
        iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
        iter_.SeekToLast();
        while (cur_bucket_ > 0 && !iter_.Valid()) {
          cur_bucket_--;
          iter_ = BucketIterator(buckets_[cur_bucket_], log_flag_);
          iter_.SeekToLast();
        }
      } else {
        cur_bucket_ = buckets_.size();
      }
    }

   private:
    bool CanReuseIterator(uint64_t hash) {
      return cur_bucket_ < buckets_.size() && iter_.Valid() &&
             buckets_[cur_bucket_]->hash_id_ == hash;
    }

   protected:
    const char log_flag_;
    const std::string& prefix_;
    const size_t suffix_len_;

    size_t cur_bucket_;
    std::vector<Bucket*> buckets_;
    BucketIterator iter_;
  };
};

void RaftHashList::Bucket::InsertLogEntry(uint64_t index, const char* key) {
  ListNode* cur = tail_.load(std::memory_order_acquire);
  uint64_t start_index = (index >> ARRAY_BIT);
  uint64_t array_index = index & ARRAY_MASK;
  if (cur != nullptr && cur->start_index == start_index) {
    cur->data[array_index].store(key, std::memory_order_release);
    return;
  }

  std::lock_guard<std::mutex> lockGuard(mu_);
  cur = head_.load(std::memory_order_acquire);
  ListNode* last = nullptr;
  while (cur != nullptr) {
    if (cur->start_index >= start_index) {
      break;
    }
    last = cur;
    cur = cur->next_.load(std::memory_order_acquire);
  }
  if (cur != nullptr && cur->start_index == start_index) {
    cur->data[array_index].store(key, std::memory_order_release);
  } else {
    auto mem = allocator_->AllocateAligned(sizeof(ListNode));
    auto x = new (mem) ListNode;
    x->next_.store(cur, std::memory_order_relaxed);
    x->start_index = start_index;
    x->data[array_index].store(key, std::memory_order_release);
    if (cur == nullptr) {
      tail_.store(x, std::memory_order_release);
    }
    if (last == nullptr) {
      head_.store(x, std::memory_order_release);
    } else {
      last->next_.store(x, std::memory_order_release);
    }
  }
}

const char* RaftHashList::Bucket::GetLogEntry(uint64_t index) {
  ListNode* cur = head_;
  uint64_t start_index = (index >> ARRAY_BIT);
  while (cur != nullptr) {
    if (cur->start_index >= start_index) {
      break;
    }
    cur = cur->next_.load(std::memory_order_acquire);
  }
  if (cur != nullptr && cur->start_index == start_index) {
    return cur->data[index & ARRAY_MASK];
  } else {
    return nullptr;
  }
}

void RaftHashList::BucketIterator::SeekToFirst() {
  cur_node_ = bucket_->head_.load(std::memory_order_acquire);
  if (cur_node_) {
    SeekLog(0);
  } else {
    SeekMeta(0);
  }
}

void RaftHashList::BucketIterator::SeekToLast() {
  is_log_ = true;
  key_ = nullptr;
  for (size_t i = META_SIZE; i > 0; i--) {
    key_ = bucket_->meta[i - 1].load(std::memory_order_acquire);
    if (key_ != nullptr) {
      index_ = i - 1;
      is_log_ = false;
      return;
    }
  }
  cur_node_ = bucket_->head_.load(std::memory_order_relaxed);
  if (cur_node_ == nullptr) {
    return;
  }
  while (cur_node_->next_.load(std::memory_order_relaxed)) {
    cur_node_ = cur_node_->next_.load(std::memory_order_relaxed);
  }
  for (size_t i = ARRAY_SIZE; i > 0; i--) {
    key_ = cur_node_->data[i - 1].load(std::memory_order_acquire);
    if (key_ != nullptr) {
      index_ = cur_node_->start_index << ARRAY_BIT | (i - 1);
      return;
    }
  }
}

void RaftHashList::BucketIterator::SeekLog(uint64_t index) {
  uint64_t start_index = (index >> ARRAY_BIT);
  index_ = index;
  is_log_ = true;
  key_ = nullptr;

  uint64_t array_index = index & ARRAY_MASK;
  while (cur_node_ != nullptr) {
    if (cur_node_->start_index >= start_index) {
      if (cur_node_->start_index > start_index) {
        array_index = 0;
      }
      while (array_index < ARRAY_SIZE) {
        key_ = cur_node_->data[array_index].load(std::memory_order_acquire);
        if (key_ != nullptr) {
          index_ = cur_node_->start_index << ARRAY_BIT | array_index;
          return;
        }
        array_index++;
      }
      array_index = 0;
    }
    cur_node_ = cur_node_->next_.load(std::memory_order_acquire);
  }
}

const char* RaftHashList::BucketIterator::SeekLogNodeForPrev(
    ListNode* node, uint64_t array_index) {
  cur_node_ = node;
  if (cur_node_ == nullptr) {
    return nullptr;
  }
  key_ = cur_node_->data[array_index].load(std::memory_order_acquire);
  while (key_ == nullptr && array_index > 0) {
    array_index--;
    key_ = cur_node_->data[array_index].load(std::memory_order_acquire);
  };
  index_ =
      cur_node_->start_index << ARRAY_BIT | static_cast<uint64_t>(array_index);
  return key_;
}

void RaftHashList::BucketIterator::Seek(uint64_t region, uint8_t flag,
                                        uint64_t index) {
  if (flag == log_flag_) {
    cur_node_ = bucket_->head_.load(std::memory_order_acquire);
    SeekLog(index);
    if (key_ != nullptr) {
      return;
    }
    flag = 0;
  }
  SeekMeta(flag);
}

void RaftHashList::BucketIterator::SeekForPrev(uint64_t region, uint8_t flag,
                                               uint64_t index) {
  if (flag != log_flag_) {
    SeekMetaForPrev(flag);
    if (key_ != nullptr) {
      return;
    }
    index_ = std::numeric_limits<size_t>::max();
  }
  cur_node_ = bucket_->head_.load(std::memory_order_acquire);
  SeekLogForPrev(index);
}

void RaftHashList::BucketIterator::SeekLogForPrev(uint64_t index) {
  uint64_t start_index = (index >> ARRAY_BIT);
  is_log_ = true;
  key_ = nullptr;
  ListNode* last = nullptr;
  ListNode* lastlast = nullptr;

  if (cur_node_ != nullptr && cur_node_->start_index == start_index) {
    if (SeekLogNodeForPrev(cur_node_, index & ARRAY_MASK) != nullptr) {
      return;
    }
  }

  cur_node_ = bucket_->head_.load(std::memory_order_acquire);
  while (cur_node_ != nullptr && cur_node_->start_index <= start_index) {
    lastlast = last;
    last = cur_node_;
    cur_node_ = cur_node_->next_.load(std::memory_order_acquire);
  }
  cur_node_ = last;
  if (last != nullptr) {
    if (last->start_index == start_index) {
      if (SeekLogNodeForPrev(last, index & ARRAY_MASK) == nullptr) {
        SeekLogNodeForPrev(lastlast, ARRAY_MASK);
      }
    } else {
      SeekLogNodeForPrev(last, ARRAY_MASK);
    }
  }
}

void RaftHashList::BucketIterator::SeekMeta(uint8_t index) {
  is_log_ = false;
  index_ = index;
  key_ = bucket_->meta[index_].load(std::memory_order_acquire);
  while (key_ == nullptr && index_ + 1 < META_SIZE) {
    index_++;
    key_ = bucket_->meta[index_].load(std::memory_order_acquire);
  }
}

void RaftHashList::BucketIterator::SeekMetaForPrev(uint8_t index) {
  is_log_ = false;
  index_ = index;
  key_ = bucket_->meta[index_].load(std::memory_order_acquire);
  while (key_ == nullptr && index_ > 0) {
    index_--;
    key_ = bucket_->meta[index_].load(std::memory_order_acquire);
  }
}

void RaftHashList::BucketIterator::Next() {
  if (is_log_) {
    SeekLog(index_ + 1);
    if (key_ != nullptr) {
      return;
    }
    SeekMeta(0);
  } else {
    if (index_ + 1 < META_SIZE) {
      SeekMeta(index_ + 1);
    } else {
      key_ = nullptr;
    }
  }
}

void RaftHashList::BucketIterator::Prev() {
  if (!is_log_) {
    if (index_ > 0) {
      SeekMetaForPrev(index_ - 1);
      if (key_ != nullptr) {
        return;
      }
    }
    index_ = std::numeric_limits<size_t>::max();
  }
  if (index_ == 0) {
    key_ = nullptr;
    return;
  }
  SeekLogForPrev(index_ - 1);
}

}  // namespace rocksdb
