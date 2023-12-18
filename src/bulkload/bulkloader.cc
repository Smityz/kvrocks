#include "bulkloader.h"

#include "rocksdb/sst_file_reader.h"

namespace redis {

Bulkloader::Bulkloader(engine::Storage* storage, std::string ns)
    : storage_(storage),
      ns_(std::move(ns)),
      writer_(rocksdb::EnvOptions(), rocksdb::Options(), storage_->GetCFHandle("metadata"), rocksdb::Env::Default()),
      encoder_(storage_, ns_) {
  dir_ = "/tmp/kvrocks/test.sst";
  auto cf = storage_->GetCFHandle("metadata");
  std::cout << "Yes" << std::endl;
  if (cf == nullptr) {
    std::cout << "cf is null" << std::endl;
  } else {
    std::cout << cf->GetName() << std::endl;
    std::cout << cf->GetID() << std::endl;
  }
};

void Bulkloader::AddString(std::string&& key, std::string&& value, const u_int64_t ttl) {
  task_queue_.push(std::move(std::make_tuple(std::move(key), std::move(value), ttl)));
}

bool cmp(const std::pair<std::string, std::string>& a, const std::pair<std::string, std::string>& b) {
  return a.first < b.first;
}

bool Bulkloader::Ingest() {
  std::vector<std::pair<std::string, std::string>> v(task_queue_.size());
  std::tuple<std::string, std::string, uint64_t> t;
  auto start = std::chrono::steady_clock::now();
  while (task_queue_.try_pop(t)) {
    auto tt = encoder_.EncodeToStr(std::get<0>(t), std::get<1>(t), std::get<2>(t));
    v.emplace_back(tt);
  }
  auto queue_time = std::chrono::steady_clock::now() - start;
  std::cout << "queue time: " << std::chrono::duration_cast<std::chrono::milliseconds>(queue_time).count() << "ms"
            << std::endl;
  std::sort(v.begin(), v.end(), cmp);
  auto sort_time = std::chrono::steady_clock::now() - start;
  std::cout << "sort time: " << std::chrono::duration_cast<std::chrono::milliseconds>(sort_time).count() << "ms"
            << std::endl;
  writer_.Open(dir_);
  for (auto& i : v) {
    writer_.Put(i.first, i.second);
  }
  auto write_time = std::chrono::steady_clock::now() - start;
  std::cout << "write time: " << std::chrono::duration_cast<std::chrono::milliseconds>(write_time).count() << "ms"
            << std::endl;

  for (auto& i : v) {
    storage_->db_->Put(rocksdb::WriteOptions(), storage_->GetCFHandle("metadata"), i.first, i.second);
  }
  auto write_db_time = std::chrono::steady_clock::now() - start;
  std::cout << "write db time: " << std::chrono::duration_cast<std::chrono::milliseconds>(write_db_time).count() << "ms"
            << std::endl;

  rocksdb::ExternalSstFileInfo info;
  writer_.Finish(&info);
  auto finish_time = std::chrono::steady_clock::now() - start;
  std::cout << "finish time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish_time).count() << "ms"
            << std::endl;
  std::cout << info.file_path << std::endl;
  std::cout << "file size: " << writer_.FileSize() << std::endl;

  std::vector<std::string> column_families;
  auto s = storage_->db_->ListColumnFamilies(rocksdb::DBOptions(), "testdb", &column_families);
  if (s.ok()) {
    std::cout << "Number of column families: " << column_families.size() << std::endl;
    for (const auto& cf_name : column_families) {
      std::cout << "Column family: " << cf_name << std::endl;
    }
  } else {
    std::cout << "Failed to list column families: " << s.ToString() << std::endl;
  }

  // Open the SST file
  rocksdb::SstFileReader sst_file_reader{rocksdb::Options()};
  s = sst_file_reader.Open(dir_);

  std::cout << "Open SST file: " << s.ToString() << std::endl;
  if (s.ok()) {
    auto p = sst_file_reader.GetTableProperties();
    std::cout << p->column_family_id << std::endl;
    std::cout << p->data_size << std::endl;
    std::cout << p->column_family_name << std::endl;
  } else {
    std::cout << "Failed to open SST file: " << s.ToString() << std::endl;
  }

  rocksdb::IngestExternalFileOptions ifo;
  // ifo.move_files = true;
  storage_->db_->DisableFileDeletions();
  auto res = storage_->db_->IngestExternalFile(storage_->GetCFHandle("metadata"), {info.file_path}, ifo);
  std::cout << res.ToString() << std::endl;
  auto ingest_time = std::chrono::steady_clock::now() - start;
  std::cout << "ingest time: " << std::chrono::duration_cast<std::chrono::milliseconds>(ingest_time).count() << "ms"
            << std::endl;
  return true;
}

}  // namespace redis