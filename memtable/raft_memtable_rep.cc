#include <atomic>

#include "db/memtable.h"
#include "memory/arena.h"
#include "raft_hash_list.h"
#include "rocksdb/raft_memtable_factory.h"
#include "rocksdb/memtablerep.h"

namespace rocksdb {

class RaftMemTableRep : public MemTableRep {
 private:
  Allocator* const allocator_;
  RaftHashList table_;

 public:
  explicit RaftMemTableRep(const MemTableRep::KeyComparator& compare,
                           Allocator* allocator, std::string prefix,
                           size_t bucket_size, uint64_t log_flag)
      : MemTableRep(allocator),
        allocator_(allocator),
        table_(allocator, prefix, bucket_size, log_flag) {}
  ~RaftMemTableRep() override {}

  KeyHandle Allocate(const size_t len, char** buf) override {
    *buf = allocator_->Allocate(len);
    return static_cast<KeyHandle>(*buf);
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(KeyHandle handle) override {
    table_.Insert(static_cast<char*>(handle));
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
  void InsertConcurrently(KeyHandle handle) override { Insert(handle); }

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const override {
    return table_.GetKey(key) != nullptr;
  }

  size_t ApproximateMemoryUsage() override {
    // All memory is allocated through allocator; nothing to report here
    return 0;
  }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    const char* key = k.memtable_key().data();
    const char* value = table_.GetKey(key);
    if (value != nullptr) {
      callback_func(callback_args, value);
    }
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    return 0;
  }

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    Iterator(const RaftHashList& table) : iter_(table, sizeof(uint64_t)) {}

    ~Iterator() override {}

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override { return iter_.key(); }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override { iter_.Next(); }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override { iter_.Prev(); }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.Seek(memtable_key);
      } else {
        iter_.Seek(EncodeKey(&tmp_, user_key));
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.SeekForPrev(memtable_key);
      } else {
        iter_.SeekForPrev(EncodeKey(&tmp_, user_key));
      }
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() override { iter_.SeekToLast(); }

   protected:
    RaftHashList::Iterator iter_;
    std::string tmp_;
  };

  // Iterator over the contents of a skip list which also keeps track of the
  // previously visited node. In Seek(), it examines a few nodes after it
  // first, falling back to O(log n) search from the head of the list only if
  // the target key hasn't been found.
  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    void* mem =
        arena ? arena->AllocateAligned(sizeof(RaftMemTableRep::Iterator)) :
              operator new(sizeof(RaftMemTableRep::Iterator));
    return new (mem) RaftMemTableRep::Iterator(table_);
  }
};

MemTableRep* RaftMemTableFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, Logger* /*logger*/) {
  return new RaftMemTableRep(compare, allocator, prefix_, bucket_size_,
                             log_flag_);
}

}  // namespace rocksdb
