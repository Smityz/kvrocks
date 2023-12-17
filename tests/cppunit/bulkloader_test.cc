#include <bulkloader/bulkloader.h>
#include <gtest/gtest.h>

#include <memory>

#include "test_base.h"

class BulkloaderTest : public TestBase {
 protected:
  explicit BulkloaderTest() { loader_ = new Bulkloader(storage_, "bulkloader_ns"); }
  ~RedisBloomChainTest() override = default;

  void SetUp() override { key_ = "test_sb_chain_key"; }
  void TearDown() override {}

  Bulkloader* loader_;
};

TEST_F(BulkloaderTest, addString) {
  std::string key = "test_key";
  std::string value = "test_value";
  uint64_t ttl = 1000;

  for (int i = 0; i < 100; i++) {
    loader_->AddString(key + std::to_string(i), value + std::to_string(i), ttl);
  }
  loader_->Ingest();
  ASSERT(false);
}