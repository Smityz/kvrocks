#include "bulkloader.h"

namespace redis {

void Bulkloader::AddString(std::string&& key, std::string&& value, const u_int64_t ttl) {
  task_queue_.push(std::move(std::make_tuple(std::move(key), std::move(value), ttl)));
}

bool cmp(const std::pair<std::string, std::string>& a, const std::pair<std::string, std::string>& b) {
  return a.first < b.first;
}

bool Bulkloader::Ingest() {
  std::vector<std::pair<std::string, std::string>> v(task_queue_.size());
  std::tuple<std::string, std::string, uint64_t> t;
  while (task_queue_.try_pop(t)) {
    auto tt = encoder_.EncodeToStr(std::get<0>(t), std::get<1>(t), std::get<2>(t));
    v.emplace_back(tt);
  }
  std::sort(v.begin(), v.end(), cmp);
  for (auto& i : v) {
    writer_.Put(i.first, i.second);
  }
  rocksdb::ExternalSstFileInfo* info;
  writer_.Finish(info);
  std::cout << info->file_path << std::endl;
  return true;
}

}  // namespace redis