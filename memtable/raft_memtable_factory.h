#pragma once

#include <rocksdb/memtablerep.h>
// This uses a doubly skip list to store keys, which is similar to skip list,
// but optimize for prev seek.
namespace rocksdb {

class RaftMemTableFactory : public MemTableRepFactory {
 public:
  RaftMemTableFactory(const std::string& prefix, uint8_t log_flag,
                      size_t bucket_size = 1024)
      : prefix_(prefix), log_flag_(log_flag), bucket_size_(bucket_size) {}

  using MemTableRepFactory::CreateMemTableRep;
  virtual MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator&,
                                         Allocator*, const SliceTransform*,
                                         Logger* logger) override;
  const char* Name() const override { return "RaftMemTableFactory"; }

  bool IsInsertConcurrentlySupported() const override { return false; }

  bool CanHandleDuplicatedKey() const override { return true; }

 private:
  const std::string prefix_;
  uint8_t log_flag_;
  const size_t bucket_size_;
};
}  // namespace rocksdb
