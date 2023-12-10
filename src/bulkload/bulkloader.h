#pragma once

#include "rocksdb/sst_file_writer.h"
#include "storage/redis_db.h"

namespace redis {

class bulkloader {
 public:
  bulkloader(engine::Storage *storage, std::string ns)
      : storage_(storage),
        ns_(std::move(ns)),
        writer_(rocksdb::EnvOptions(), rocksdb::Options(), storage_->GetCFHandle("metadata"),
                rocksdb::Env::Default()){};
  ~bulkloader() = default;

 private:
  engine::Storage *storage_;
  std::string ns_;
  rocksdb::SstFileWriter writer_;
};
}  // namespace redis