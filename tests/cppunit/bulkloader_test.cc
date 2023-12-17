#include <bulkload/bulkloader.h>
#include <gtest/gtest.h>

#include <memory>

#include "test_base.h"

class BulkloaderTest : public TestBase {
 protected:
  explicit BulkloaderTest() { loader_ = new redis::Bulkloader(storage_, "bulkloader_ns"); }
  ~BulkloaderTest() override = default;

  void SetUp() override {}
  void TearDown() override {}

  redis::Bulkloader* loader_;
};

TEST_F(BulkloaderTest, addString) {
  std::string key = "test_key";
  std::string value = "test_value";
  uint64_t ttl = 1000;

  std::string one_kb_value(1024, 'a');

  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 100000; i++) {
    loader_->AddString(key + std::to_string(i), std::to_string(i) + one_kb_value, ttl);
  }
  auto add_time = std::chrono::steady_clock::now() - start;
  std::cout << "add time: " << std::chrono::duration_cast<std::chrono::milliseconds>(add_time).count() << "ms"
            << std::endl;
  loader_->Ingest();
  EXPECT_TRUE(false);
}