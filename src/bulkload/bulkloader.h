#pragma once

#include "oneapi/tbb/concurrent_queue.h"
#include "rocksdb/sst_file_writer.h"
#include "types/redis_string.h"

namespace redis {

class Bulkloader {
 public:
  Bulkloader(engine::Storage* storage, std::string ns)
      : storage_(storage),
        ns_(std::move(ns)),
        writer_(rocksdb::EnvOptions(), rocksdb::Options(), storage_->GetCFHandle("metadata"), rocksdb::Env::Default()),
        encoder_(storage_, ns_){};
  ~Bulkloader() = default;

  void AddString(std::string&& key, std::string&& value, u_int64_t ttl);

  bool Ingest();

 private:
  engine::Storage* storage_;
  std::string ns_;
  rocksdb::SstFileWriter writer_;
  tbb::concurrent_bounded_queue<std::tuple<std::string, std::string, uint64_t>> task_queue_;
  redis::String encoder_;
};
}  // namespace redis