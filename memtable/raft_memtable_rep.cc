//
// Created by 刘玮 on 2020-02-07.
//

#include <atomic>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/skiplist.h"
#include "rocksdb/memtablerep.h"
#define ARRAY_SIZE 256
#define ARRAY_MASK 255
#define ARRAY_BIT 8

namespace rocksdb {

class RaftMemTableRep : public MemTableRep {
  //  struct BlockArray {
  //    std::atomic<size_t> size_;
  //    const char* data_[ARRAY_MASK+1];
  //  };
  struct ListNode {
    uint64_t start_index;
    std::atomic<ListNode*> next_;
    std::atomic<const char*> data[ARRAY_SIZE];
    ListNode() : start_index(0), next_(nullptr) {
      memset(data, 0, sizeof data);
    }
  };
  struct Bucket {
    uint64_t hash_id;
    std::atomic<ListNode*> head_;
    Allocator* const allocator_;
    std::atomic<const char*> data[256];

    Bucket(uint64_t hash_id, const MemTableRep::KeyComparator& compare,
           Allocator* allocator)
        : hash_id(hash_id),
          head_(nullptr),
          allocator_(allocator) {
      memset(data, 0, sizeof data);
    }
    void InsertLogEntry(uint64_t index, const char* key);
    const char* GetLogEntry(uint64_t index);
  };
  const MemTableRep::KeyComparator& cmp_;
  const uint8_t log_flag_;
  size_t bucket_size_;
  Allocator* const allocator_;
  const std::string prefix_;
  std::atomic<Bucket*>* buckets_;
  std::mutex mu_;

 public:
  explicit RaftMemTableRep(const MemTableRep::KeyComparator& compare,
                           Allocator* allocator, std::string prefix,
                           size_t bucket_size, uint64_t log_flag)
      : MemTableRep(allocator),
        cmp_(compare),
        log_flag_(log_flag),
        bucket_size_(bucket_size),
        allocator_(allocator),
        prefix_(prefix) {
    auto mem =
        allocator_->AllocateAligned(sizeof(std::atomic<void*>) * bucket_size);
    buckets_ = new (mem) std::atomic<Bucket*>[bucket_size];
    for (size_t i = 0; i < bucket_size_; ++i) {
      buckets_[i].store(nullptr, std::memory_order_relaxed);
    }
  }
  ~RaftMemTableRep() override {}

  static inline uint64_t GetHash(const char* src) {
    uint64_t result = 0;
    for (unsigned long i = 0; i < sizeof(uint64_t); i++) {
      result |= static_cast<uint64_t>(static_cast<unsigned char>(src[i]))
                << ((7 - i) * 8);
    }
    return result;
  }

  inline size_t GetBucket(uint64_t hash) const {
    for (size_t i = 0; i < bucket_size_; i++) {
      size_t j = (hash + i) % bucket_size_;
      auto bucket = buckets_[j].load(std::memory_order_acquire);
      if (bucket == nullptr || bucket->hash_id == hash) {
        return i;
      }
    }
    throw std::runtime_error("there has no bucket");
    return 0;
  }

  static inline uint8_t GetFlag(const std::string& prefix, const Slice& slice) {
    return slice[prefix.length() + sizeof(uint64_t)];
  }

  KeyHandle Allocate(const size_t len, char** buf) override {}

  static inline uint64_t GetLogIndex(const std::string& prefix, const Slice& slice) {
    return GetHash(slice.data() + prefix.length() + sizeof(uint64_t) + 1);
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(KeyHandle handle) override {
    const char* key = static_cast<const char*>(handle);
    assert(!Contains(key));
    auto transformed = UserKey(key);
    if (transformed.size() <= prefix_.length()) {
      return;
    }
    uint64_t hash = GetHash(transformed.data() + prefix_.length());
    uint8_t flag = GetFlag(prefix_, transformed);
    auto bucketId = GetBucket(hash);
    auto bucket = buckets_[bucketId].load(std::memory_order_acquire);
    if (bucket == nullptr) {
      // std::lock_guard<std::mutex> lockGuard(mu_);
      auto mem = allocator_->AllocateAligned(sizeof(Bucket));
      bucket = new (mem) Bucket(hash, cmp_, allocator_);
      if (flag == log_flag_) {
        bucket->InsertLogEntry(GetLogIndex(prefix_, transformed), key);
      } else {
        bucket->data[(unsigned char)flag] = key;
      }
      buckets_[bucketId].store(bucket, std::memory_order_release);
      return;
    }
    if (flag == log_flag_) {
      bucket->InsertLogEntry(GetLogIndex(prefix_, transformed), key);
    } else {
      bucket->data[(unsigned char)flag] = key;
    }
  }

  bool InsertKey(KeyHandle handle) override {
    Insert(handle);
    return true;
  }

  void InsertWithHint(KeyHandle handle, void** hint) override {
    Insert(handle);
  }

  bool InsertKeyWithHint(KeyHandle handle, void** hint) override {
    Insert(handle);
    return true;
  }

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const override {
    return GetKey(key) != nullptr;
  }

  size_t ApproximateMemoryUsage() override {
    // All memory is allocated through allocator; nothing to report here
    return 0;
  }

  const char* GetKey(const char* key) const {
    auto transformed = UserKey(key);
    if (transformed.size() <= prefix_.length()) {
      return nullptr;
    }
    uint64_t hash = GetHash(transformed.data() + prefix_.length());
    auto bucketId = GetBucket(hash);
    auto bucket = buckets_[bucketId].load(std::memory_order_acquire);
    if (bucket == nullptr) {
      return nullptr;
    }
    uint8_t flag = GetFlag(prefix_, transformed);
    if (flag == log_flag_) {
      return bucket->GetLogEntry(GetLogIndex(prefix_, transformed));
    } else {
      return bucket->data[flag];
    }
  }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    const char* key = k.memtable_key().data();
    callback_func(callback_args, GetKey(key));
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    return 0;
  }
  class BucketIterator {
  public:
      BucketIterator() {};
      explicit BucketIterator(Bucket* bucket) : is_log_(false), index_(ARRAY_SIZE), bucket_(bucket), key_(nullptr) {
        cur_node_ = bucket_->head_.load(std::memory_order_relaxed);
      }
      void SeekToFirst();
      void SeekToLast();
      void SeekLog(uint64_t index);
      void SeekMeta(uint8_t index);
      void SeekLogForPrev(uint64_t index);
      void SeekMetaForPrev(uint8_t index);
      void Next();
      void Prev();
      const char* key() const { return key_; }
      bool Valid() {
        return index_ < ARRAY_SIZE;
      }

  private:
      bool is_log_;
      ListNode* cur_node_;
      size_t index_;
      Bucket* bucket_;
      const char* key_;
  };

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator(std::atomic<Bucket*>* buckets, size_t bucket_size, uint8_t log_flag, const std::string& prefix) :
      log_flag_(log_flag), prefix_(prefix) {
      for (size_t i = 0; i < bucket_size; i ++) {
        auto bucket = buckets->load(std::memory_order_seq_cst);
        if (bucket != nullptr) {
          buckets_.push_back(bucket);
        }
      }
      std::sort(buckets_.begin(), buckets_.end(), [=](const Bucket* a, const Bucket* b){ return a->hash_id < b->hash_id; });
    }

    ~Iterator() override {}

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override {
      return cur_bucket_ < buckets_.size();
    }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override { return iter_.key(); }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override {
      iter_.Next();
      while (cur_bucket_ + 1 < buckets_.size() && !iter_.Valid()) {
        cur_bucket_ ++;
        iter_ = BucketIterator(buckets_[cur_bucket_]);
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
    void Prev() override {
    }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* _memtable_key) override {
      const char* key = EncodeKey(&tmp_, user_key);
      if (user_key.size() < prefix_.length() + sizeof(uint64_t)) {
        if (user_key.compare(Slice(prefix_)) <= 0) {
          SeekToFirst();
        } else {
          cur_bucket_ = buckets_.size();
        }
        return;
      }
      uint64_t hash = GetHash(key + prefix_.length());
      for (size_t i = 0; i < buckets_.size(); i ++) {
        if (buckets_[i]->hash_id >= hash) {
          cur_bucket_ = i;
          iter_ = BucketIterator(buckets_[cur_bucket_]);
          break;
        }
      }
      if (user_key.size() == prefix_.length() + sizeof(uint64_t)) {
        iter_.SeekToFirst();
        return;
      }
      uint8_t flag = user_key[prefix_.length() + sizeof(uint64_t)];
      uint64_t index = ARRAY_SIZE;
      if (user_key.size() > prefix_.length() + 8) {
        index = GetLogIndex(prefix_, user_key);
        iter_.SeekLog(index);
        if (iter_.Valid()) {
          return;
        }
      }
      iter_.SeekMeta(flag);

      while (cur_bucket_ + 1 < buckets_.size() && !iter_.Valid()) {
        cur_bucket_++;
        iter_ = BucketIterator(buckets_[cur_bucket_]);
        iter_.SeekToFirst();
      };
      if (!iter_.Valid()) {
        cur_bucket_ = buckets_.size();
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override {
      if (!buckets_.empty()) {
        cur_bucket_ = 0;
        iter_ = BucketIterator(buckets_[cur_bucket_]);
        iter_.SeekToFirst();
      }
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() override {
      if (!buckets_.empty()) {
        cur_bucket_ = buckets_.size() - 1;
        iter_ = BucketIterator(buckets_[cur_bucket_]);
        iter_.SeekToLast();
      } else {
        cur_bucket_ = buckets_.size();
      }
    }
   protected:
    const char log_flag_;
    const std::string& prefix_;

    size_t cur_bucket_;
    std::string tmp_;
    std::vector<Bucket*> buckets_;
    BucketIterator iter_;
  };

  // Iterator over the contents of a skip list which also keeps track of the
  // previously visited node. In Seek(), it examines a few nodes after it
  // first, falling back to O(log n) search from the head of the list only if
  // the target key hasn't been found.
  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {}
};

void RaftMemTableRep::Bucket::InsertLogEntry(uint64_t index, const char* key) {
  ListNode* cur = head_;
  ListNode* last = nullptr;
  uint64_t start_index = (index >> ARRAY_BIT) << ARRAY_BIT;
  while (cur != nullptr) {
    if (cur->start_index >= start_index) {
      break;
    }
    last = cur;
    cur = cur->next_.load(std::memory_order_acquire);
  }
  if (cur != nullptr && cur->start_index == start_index) {
    cur->data[index & ARRAY_MASK].store(key, std::memory_order_release);
  } else {
    auto mem = allocator_->AllocateAligned(sizeof(ListNode));
    auto x = new (mem) ListNode;
    x->next_.store(cur, std::memory_order_relaxed);
    x->data[index & ARRAY_MASK].store(key, std::memory_order_release);
    if (last == nullptr) {
      head_.store(x, std::memory_order_release);
    } else {
      last->next_.store(x, std::memory_order_release);
    }
  }
}

const char* RaftMemTableRep::Bucket::GetLogEntry(uint64_t index) {
  ListNode* cur = head_;
  uint64_t start_index = (index >> ARRAY_BIT) << ARRAY_BIT;
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

void RaftMemTableRep::BucketIterator::SeekToFirst() {
  cur_node_ = bucket_->head_.load(std::memory_order_acquire);
  if (cur_node_) {
    SeekLog(0);
  } else {
    SeekMeta(0);
  }
}

void RaftMemTableRep::BucketIterator::SeekToLast() {
  is_log_ = true;
  for (size_t i = 255; i >= 0; i --) {
    key_ = bucket_->data[i].load(std::memory_order_acquire);
    if (key_ != nullptr) {
      index_ = i;
      is_log_ = false;
      return;
    }
  }
  cur_node_ = bucket_->head_.load(std::memory_order_relaxed);
  while (cur_node_->next_.load(std::memory_order_relaxed)) {
    cur_node_ = cur_node_->next_.load(std::memory_order_relaxed);
  }
  for (size_t i = ARRAY_SIZE; i >= 0; i --) {
    key_ = cur_node_->data[i].load(std::memory_order_acquire);
    if (key_ != nullptr) {
      index_ = i;
      return;
    }
  }
}

void RaftMemTableRep::BucketIterator::SeekLog(uint64_t index) {
  uint64_t start_index = (index >> ARRAY_BIT) << ARRAY_BIT;
  index_ = index & ARRAY_MASK;
  is_log_ = true;
  while (cur_node_ != nullptr) {
    if (cur_node_->start_index >= start_index) {
      while (index_ < ARRAY_SIZE) {
        key_ = cur_node_->data[index_].load(std::memory_order_acquire);
        if (key_ != nullptr) {
          return;
        }
        index_ ++;
      }
      index_ = 0;
    }
    cur_node_ = cur_node_->next_.load(std::memory_order_acquire);
  }
  if (cur_node_ == nullptr) {
    index_ = ARRAY_SIZE;
  }
}

void RaftMemTableRep::BucketIterator::SeekLogForPrev(uint64_t index) {
  uint64_t start_index = (index >> ARRAY_BIT) << ARRAY_BIT;
  is_log_ = true;
  key_ = nullptr;
  cur_node_ = bucket_->head_.load(std::memory_order_acquire);
  ListNode* last = cur_node_;
  while (cur_node_ != nullptr && cur_node_->start_index <= start_index) {
    index_ = index & ARRAY_MASK;
    do {
      key_ = cur_node_->data[index_].load(std::memory_order_acquire);
      if (key_ != nullptr) {
        break;
      }
      index_--;
    } while (index_ > 0);
    last = cur_node_;
    cur_node_ = cur_node_->next_.load(std::memory_order_acquire);
  }
  cur_node_ = last;
  if (cur_node_ == nullptr) {
    index_ = ARRAY_SIZE;
  }
}

void RaftMemTableRep::BucketIterator::SeekMeta(uint8_t index) {
  is_log_ = false;
  index_ = index;
  while (index_ < ARRAY_SIZE) {
    key_ = bucket_->data[index_].load(std::memory_order_acquire);
    if (key_ != nullptr) {
      return;
    }
  }
}

void RaftMemTableRep::BucketIterator::SeekMetaForPrev(uint8_t index) {
  is_log_ = false;
  index_ = index;
  while (index_ >= 0) {
    key_ = bucket_->data[index_].load(std::memory_order_acquire);
    if (key_ != nullptr) {
      return;
    }
  }
}

void RaftMemTableRep::BucketIterator::Next() {
  if (is_log_) {
    SeekLog(index_ + 1);
    if (Valid()) {
      return;
    }
    index_ = 0;
  }
  SeekMeta(index_ + 1);
}

void RaftMemTableRep::BucketIterator::Prev() {
  if (!is_log_ && index_ > 0) {
    SeekMetaForPrev(index_ - 1);
    if (Valid()) {
      return;
    }
    index_ = std::numeric_limits<size_t>::max();
  }
  SeekLogForPrev(index_ - 1);
}


}  // namespace rocksdb
