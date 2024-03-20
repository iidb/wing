#include "common/stopwatch.hpp"
#include "gtest/gtest.h"
#include "storage/lsm/block.hpp"
#include "storage/lsm/compaction_job.hpp"
#include "storage/lsm/file.hpp"
#include "storage/lsm/iterator_heap.hpp"
#include "storage/lsm/level.hpp"
#include "storage/lsm/lsm.hpp"
#include "storage/lsm/memtable.hpp"
#include "storage/lsm/sst.hpp"
#include "storage/lsm/stats.hpp"
#include "storage/lsm/version.hpp"
#include "test.hpp"

using namespace wing::lsm;
using namespace wing::wing_testing;

TEST(LSMTest, MemtableBasicTest) {
  MemTable t;
  size_t seq = 0;
  ASSERT_TRUE(ParsedKey("abc", 10, RecordType::Value) <
              ParsedKey("abc", 1, RecordType::Value));
  ASSERT_TRUE(ParsedKey("abc", 10, RecordType::Value) <=
              ParsedKey("abc", 1, RecordType::Value));
  ASSERT_TRUE(ParsedKey("abc", 1, RecordType::Value) >
              ParsedKey("abc", 10, RecordType::Value));
  ASSERT_TRUE(ParsedKey("abc", 1, RecordType::Value) >=
              ParsedKey("abc", 10, RecordType::Value));
  ASSERT_TRUE(ParsedKey("abc", 1, RecordType::Value) <
              ParsedKey("abd", 10, RecordType::Value));
  // Entries in Table t: (abc, abc2, 1, Value)
  t.Put("abc", ++seq, "abc2");
  std::string value;
  ASSERT_EQ(t.Get("abc", ++seq, &value), GetResult::kFound);
  ASSERT_EQ(value, "abc2");
  ASSERT_EQ(t.size(), strlen("abc") + strlen("abc2") + sizeof(RecordType) +
                          sizeof(seq_t) + sizeof(offset_t) * 2);
  // Entries in Table t: (abc, abc2, 1, Value) (abc, , 3, Deletion)
  t.Del("abc", ++seq);
  ASSERT_EQ(t.Get("abc", ++seq, &value), GetResult::kDelete);
  // Entries in Table t: (abc, abc2, 1, Value) (abc, , 3, Deletion) (abc, abc3,
  // 5, Value)
  t.Put("abc", ++seq, "abc3");
  ASSERT_EQ(t.Get("abc", ++seq, &value), GetResult::kFound);
  ASSERT_EQ(value, "abc3");
}

TEST(LSMTest, MemTableSmallTest) {
  MemTable t;
  size_t n = 1000, TH = 4;
  std::vector<std::future<void>> pool;
  for (uint32_t i = 0; i < TH; i++) {
    pool.push_back(std::async([&, seed = i]() {
      auto kv = GenKVData(0x202403152237 + seed, n, 13, 129);
      size_t seq = 0;
      for (uint32_t i = 0; i < n; i++) {
        t.Put(kv[i].key(), ++seq, kv[i].value());
        std::string value;
        ASSERT_EQ(t.Get(kv[i].key(), ++seq, &value), GetResult::kFound);
        ASSERT_EQ(value, kv[i].value());
      }

      for (uint32_t i = 0; i < n; i++) {
        std::string value;
        ASSERT_EQ(t.Get(kv[i].key(), i * 2, &value), GetResult::kNotFound);
        ASSERT_EQ(t.Get(kv[i].key(), i * 2 + 1, &value), GetResult::kFound);
        ASSERT_EQ(value, kv[i].value());
      }

      for (uint32_t i = 0; i < n; i++) {
        t.Del(kv[i].key(), ++seq);
        std::string value;
        ASSERT_EQ(t.Get(kv[i].key(), ++seq, &value), GetResult::kDelete);
        ASSERT_EQ(value.size(), 0);
      }
    }));
  }

  for (auto& f : pool)
    f.get();
}

TEST(LSMTest, FileWriterTest) {
  FileWriter writer(
      std::make_unique<SeqWriteFile>("__tmpLSMFileWriterTest", false), 4096);
  size_t write_n = 1e6;
  // Write 64-bit integers and strings
  {
    std::mt19937_64 rgen(0x202403152244);
    for (uint32_t i = 0; i < write_n; i++) {
      writer.AppendValue<uint64_t>(rgen());
      std::uniform_int_distribution<> dis(1, 13);
      std::string s(dis(rgen), 0);
      for (auto& ch : s)
        ch = rgen() % 256;
      writer.AppendString(s);
    }
    writer.Flush();
  }
  // Read integers and strings.
  {
    auto buf = std::unique_ptr<char[]>(new char[writer.size()]);
    ReadFile("__tmpLSMFileWriterTest", false).Read(buf.get(), writer.size(), 0);
    std::mt19937_64 rgen(0x202403152244);
    for (uint32_t i = 0, off = 0; i < write_n; i++) {
      uint64_t ans = *reinterpret_cast<uint64_t*>(buf.get() + off);
      off += sizeof(uint64_t);
      ASSERT_EQ(ans, rgen());
      std::uniform_int_distribution<> dis(1, 13);
      size_t len = dis(rgen);
      for (uint32_t i = 0; i < len; i++) {
        ASSERT_EQ(rgen() % 256, (uint8_t)(buf[off]));
        off += 1;
      }
    }
  }
  std::remove("__tmpLSMFileWriterTest");
}

TEST(LSMTest, BlockTest) {
  FileWriter writer(
      std::make_unique<SeqWriteFile>("__tmpLSMBlockTest", false), 4096);
  BlockBuilder builder(16384, &writer);
  uint32_t N = 512, klen = 5, vlen = 6;
  auto kv = GenKVData(0x202403152328, N, klen, vlen);
  /* Store the key-value pairs into the block */
  std::sort(kv.begin(), kv.end());
  for (uint32_t i = 0; i < N; i++) {
    ASSERT_TRUE(builder.Append(
        ParsedKey(kv[i].key(), 1, RecordType::Value), kv[i].value()));
  }
  builder.Finish();
  /* Flush the block data to disk */
  writer.Flush();
  /* Read the block from the file */
  auto buf = std::unique_ptr<char[]>(new char[writer.size()]);
  ReadFile("__tmpLSMBlockTest", false).Read(buf.get(), writer.size(), 0);
  for (uint32_t i = 0; i < N; i++) {
    BlockHandle handle;
    handle.offset_ = 0;
    handle.size_ = builder.size();
    handle.count_ = builder.count();
    BlockIterator it(buf.get(), handle);
    it.Seek(kv[i].key(), 1);
    for (uint32_t j = i; j < N; j++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(ParsedKey(it.key()).user_key_, kv[j].key());
      ASSERT_EQ(it.value(), kv[j].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }
  std::remove("__tmpLSMBlockTest");
}

TEST(LSMTest, SSTableTest) {
  SSTableBuilder builder(
      std::make_unique<FileWriter>(
          std::make_unique<SeqWriteFile>("__tmpLSMSSTableTest", false), 4096),
      4096);
  uint32_t klen = 9, vlen = 13, N = 1e5;
  auto kv =
      GenKVDataWithRandomLen(0x202403152328, N, {klen - 1, klen}, {1, vlen});
  std::sort(kv.begin(), kv.end());
  /* Store the sorted key-value pairs into the SSTable */
  for (uint32_t i = 0; i < N; i++) {
    builder.Append(ParsedKey(kv[i].key(), 1, RecordType::Value), kv[i].value());
  }
  /* Flush the SSTable data to disk */
  builder.Finish();
  DB_INFO("SSTable size: {} bytes", builder.size());
  /* Read the SSTable from the file */
  SSTInfo info;
  info.count_ = N;
  info.size_ = builder.size();
  info.filename_ = "__tmpLSMSSTableTest";
  info.index_offset_ = builder.GetIndexOffset();
  info.sst_id_ = 0;
  SSTable sst(info, 4096, false);
  /* Test SSTable::Get */
  for (uint32_t i = 0; i < N; i++) {
    std::string value;
    ASSERT_EQ(sst.Get(kv[i].key(), 1, &value), GetResult::kFound);
    ASSERT_EQ(value, kv[i].value());
  }
  /* Test SSTable::Seek */
  for (uint32_t i = 0; i < 500; i++) {
    size_t id = i * (N / 499);
    auto it = sst.Seek(kv[id].key(), 1);
    for (uint32_t j = id; j < N; j++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(ParsedKey(it.key()).user_key_, kv[j].key());
      ASSERT_EQ(it.value(), kv[j].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }
  std::remove("__tmpLSMSSTableTest");
}

TEST(LSMTest, SortedRunTest) {
  uint32_t klen = 9, vlen = 13, N = 3e6, fileN = 10;
  auto kv =
      GenKVDataWithRandomLen(0x202403191425, N, {klen - 1, klen}, {1, vlen});
  std::sort(kv.begin(), kv.end());
  /* Create 10 SSTables */
  wing::StopWatch sw;
  std::vector<SSTInfo> sst_infos;
  for (uint32_t i = 0; i < fileN; i++) {
    auto L = (i) * (N / fileN);
    auto R = std::min((i + 1) * (N / fileN), N);
    SSTableBuilder builder(
        std::make_unique<FileWriter>(
            std::make_unique<SeqWriteFile>(
                fmt::format("__tmpSortedRunTest{}", i), false),
            1 << 20),
        4096);
    /* Build the SSTable */
    for (uint32_t j = L; j < R; j++) {
      builder.Append(
          ParsedKey(kv[j].key(), 1, RecordType::Value), kv[j].value());
    }
    builder.Finish();
    /* Add the SSTable*/
    SSTInfo info;
    info.count_ = builder.count();
    info.filename_ = fmt::format("__tmpSortedRunTest{}", i);
    info.index_offset_ = builder.GetIndexOffset();
    info.size_ = builder.size();
    info.sst_id_ = i;
    sst_infos.emplace_back(info);
  }
  DB_INFO("Create Sorted Run Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  SortedRun sr(sst_infos, 4096, false);
  /* Test SortedRun::Get */
  for (uint32_t i = 0; i < N; i++) {
    std::string value;
    ASSERT_EQ(sr.Get(kv[i].key(), 1, &value), GetResult::kFound);
    ASSERT_EQ(value, kv[i].value());
  }
  DB_INFO("SortedRun::Get Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  /* Test SortedRun::Seek */
  for (uint32_t i = 0; i < 500; i++) {
    size_t step = (N / 499);
    size_t id = i * step;
    auto it = sr.Seek(kv[id].key(), 1);
    for (uint32_t j = id; j < N && j < id + step * 10; j++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(ParsedKey(it.key()).user_key_, kv[j].key());
      ASSERT_EQ(it.value(), kv[j].value());
      it.Next();
    }
  }
  DB_INFO("Short Range Scan Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  /* Test SortedRun::Begin */
  {
    auto it = sr.Begin();
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(ParsedKey(it.key()).user_key_, kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }
  DB_INFO("Full Scan Cost: {}s", sw.GetTimeInSeconds());
  /* Delete all SSTables */
  for (uint32_t i = 0; i < fileN; i++) {
    std::remove(fmt::format("__tmpSortedRunTest{}", i).c_str());
  }
}

TEST(LSMTest, IteratorHeapTest) {
  uint32_t klen = 9, vlen = 50, N = 3e6, fileN = 10;
  auto kv = GenKVData(0x202403152328, N, klen, vlen);
  std::vector<std::shared_ptr<SSTable>> ssts;
  /* Create 10 SSTables */
  for (uint32_t i = 0; i < fileN; i++) {
    auto L = (i) * (N / fileN);
    auto R = std::min((i + 1) * (N / fileN), N);
    std::sort(kv.begin() + L, kv.begin() + R);
    SSTableBuilder builder(
        std::make_unique<FileWriter>(
            std::make_unique<SeqWriteFile>(
                fmt::format("__tmpLSMIteratorHeapTest{}", i), false),
            4096),
        4096);
    /* Build the SSTable */
    for (uint32_t j = L; j < R; j++) {
      builder.Append(
          ParsedKey(kv[j].key(), 1, RecordType::Value), kv[j].value());
    }
    builder.Finish();
    /* Add the SSTable*/
    SSTInfo info;
    info.count_ = builder.count();
    info.filename_ = fmt::format("__tmpLSMIteratorHeapTest{}", i);
    info.index_offset_ = builder.GetIndexOffset();
    info.size_ = builder.size();
    info.sst_id_ = i;
    ssts.emplace_back(std::make_shared<SSTable>(info, 4096, false));
  }
  /* Add the iterators of the SSTables */
  IteratorHeap<SSTableIterator> its;
  std::vector<SSTableIterator> sst_its;
  for (auto& sst : ssts) {
    sst_its.push_back(sst->Begin());
  }
  for (auto& sst : sst_its) {
    its.Push(&sst);
  }
  its.Build();
  std::sort(kv.begin(), kv.end());
  /* Do merge-sort. */
  for (uint32_t i = 0; i < N; i++) {
    ASSERT_TRUE(its.Valid());
    ASSERT_EQ(ParsedKey(its.key()).user_key_, kv[i].key());
    ASSERT_EQ(its.value(), kv[i].value());
    its.Next();
  }
  ASSERT_FALSE(its.Valid());
  /* Delete all SSTables */
  for (uint32_t i = 0; i < fileN; i++) {
    std::remove(fmt::format("__tmpLSMIteratorHeapTest{}", i).c_str());
  }
}

TEST(LSMTest, CompactionBasicTest) {
  class Iterator {
   public:
    Iterator(std::vector<CompressedKVPair>& kv) : kv_(kv) {}

    Slice key() {
      key_ = InternalKey(kv_[id_].key(), 1, RecordType::Value);
      return key_.GetSlice();
    }

    Slice value() { return kv_[id_].value(); }

    void Next() { id_ += 1; }

    bool Valid() { return id_ < kv_.size(); }

   private:
    std::vector<CompressedKVPair>& kv_;
    uint32_t id_{0};
    InternalKey key_;
  };
  auto filegen =
      std::make_unique<FileNameGenerator>("__tmpCompactionBasicTest", 0);
  CompactionJob worker(filegen.get(), 4096, 1048576, 16384, false);

  uint32_t klen = 9, vlen = 13, N = 1e6;
  auto kv = GenKVData(0x202403170002, N, klen, vlen);
  std::sort(kv.begin(), kv.end());
  auto ssts = worker.Run(Iterator(kv));
  SortedRun level(ssts, 4096, false);
  auto it = level.Begin();
  for (uint32_t i = 0; i < N; i++) {
    ASSERT_TRUE(it.Valid());
    ASSERT_EQ(ParsedKey(it.key()).user_key_, kv[i].key());
    ASSERT_EQ(it.value(), kv[i].value());
    it.Next();
  }
  ASSERT_FALSE(it.Valid());
  /* Delete all SSTables */
  for (uint32_t i = 0; i < ssts.size(); i++) {
    std::remove(ssts[i].filename_.c_str());
  }
}

//////////////// LSM Tests

TEST(LSMTest, LSMBasicTest) {
  Options options;
  options.db_path = "__tmpLSMBasicTest/";
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);
  // Entries in Table t: (abc, abc2, 1, Value)
  lsm->Put("abc", "abc2");
  std::string value;
  ASSERT_TRUE(lsm->Get("abc", &value));
  ASSERT_EQ(value, "abc2");
  // Entries in Table t: (abc, abc2, 1, Value) (abc, , 3, Deletion)
  lsm->Del("abc");
  ASSERT_FALSE(lsm->Get("abc", &value));
  // Entries in Table t: (abc, abc2, 1, Value) (abc, , 3, Deletion) (abc, abc3,
  // 5, Value)
  lsm->Put("abc", "abc3");
  ASSERT_TRUE(lsm->Get("abc", &value));
  ASSERT_EQ(value, "abc3");
}

TEST(LSMTest, LSMSmallGetTest) {
  Options options;
  options.sst_file_size = 8 << 20;
  options.db_path = "__tmpLSMSmallGetTest/";
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);

  uint32_t klen = 10, vlen = 15, N = 3e6;
  auto kv =
      GenKVDataWithRandomLen(0x202403170002, N, {klen - 1, klen}, {1, vlen});
  /* Insert all key-value pairs into LSM tree */
  wing::StopWatch sw;
  for (uint32_t i = 0; i < N; i++) {
    lsm->Put(kv[i].key(), kv[i].value());
  }
  DB_INFO("Put Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  /* Get all key-value pairs, and delete all of them */
  for (uint32_t i = 0; i < N; i++) {
    std::string value;
    ASSERT_TRUE(lsm->Get(kv[i].key(), &value));
    ASSERT_EQ(value, kv[i].value());
    lsm->Del(kv[i].key());
  }
  DB_INFO("Get&Del Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  /* Get all key-value pairs again and expect to get nothing. */
  for (uint32_t i = 0; i < N; i++) {
    std::string value;
    ASSERT_FALSE(lsm->Get(kv[i].key(), &value));
  }
  DB_INFO("Second Get Cost: {}s", sw.GetTimeInSeconds());
  lsm.reset();
  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, LSMSmallScanTest) {
  Options options;
  options.sst_file_size = 8 << 20;
  options.db_path = "__tmpLSMSmallScanTest/";
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);

  uint32_t klen = 10, vlen = 13, N = 1e7;
  auto kv =
      GenKVDataWithRandomLen(0x202403180237, N, {klen - 1, klen}, {1, vlen});
  std::mt19937_64 rgen(0x202403180238);

  /* Insert all key-value pairs into LSM tree */
  wing::StopWatch sw;
  for (uint32_t i = 0; i < N; i++) {
    lsm->Put(kv[i].key(), kv[i].value());
  }
  DB_INFO("Put Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  std::sort(kv.begin(), kv.end());
  /* Test DBImpl::Begin. */
  {
    auto it = lsm->Begin();
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(it.key(), kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }
  DB_INFO("Scan All Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  /* Test DBImpl::Seek. */
  {
    for (uint32_t i = 0; i < 1000; i++) {
      uint32_t begin = rgen() % N;
      auto it = lsm->Seek(kv[begin].key());
      for (uint32_t j = begin; j < (N / 1000) + begin && j < N; j++) {
        ASSERT_TRUE(it.Valid());
        ASSERT_EQ(it.key(), kv[j].key());
        ASSERT_EQ(it.value(), kv[j].value());
        it.Next();
      }
    }
  }

  DB_INFO("Short Range Scan Cost: {}s", sw.GetTimeInSeconds());
  /* Test the snapshot mechanism. */
  {
    auto it = lsm->Begin();
    /* The delete operations should not affect the data in the snapshot. */
    for (uint32_t i = 0; i < N; i++) {
      lsm->Del(kv[i].key());
    }
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(it.key(), kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }
  DB_INFO("Second Scan All Cost: {}s", sw.GetTimeInSeconds());
  /* Test if the iterator skips deleted elements. */
  {
    auto it = lsm->Begin();
    /* Because all the data are deleted. */
    ASSERT_FALSE(it.Valid());
    auto it2 = lsm->Seek(kv[10].key());
    ASSERT_FALSE(it2.Valid());
  }
  lsm.reset();
  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, LSMSmallMultithreadGetPutTest) {
  uint32_t TH = 4;
  Options options;
  options.sst_file_size = 8 << 20;
  options.db_path = "__tmpLSMMultithreadGetPutTest/";
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);

  std::vector<std::future<void>> pool;
  for (uint32_t i = 0; i < TH; i++) {
    pool.push_back(std::async([&, seed = i]() -> void {
      uint32_t klen = 10, vlen = 13, N = 1e6;
      double read_prob = 0.5;
      auto kv = GenKVDataWithRandomLen(
          0x202403180327 + seed, N, {klen - 1, klen}, {1, vlen});
      std::mt19937_64 rgen(0x202403180327 + seed);
      for (uint32_t i = 0; i < N; i++) {
        std::uniform_real_distribution<> dis(0, 1);
        lsm->Put(kv[i].key(), kv[i].value());
        if (dis(rgen) < read_prob) {
          std::string value;
          std::uniform_int_distribution<> dis2(0, i);
          int ri = dis2(rgen);
          ASSERT_TRUE(lsm->Get(kv[ri].key(), &value));
          ASSERT_EQ(value, kv[ri].value());
        }
      }
    }));
  }
  for (auto& f : pool)
    f.get();

  lsm.reset();
  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, LSMSaveTest) {
  Options options;
  options.sst_file_size = 8 << 20;
  options.db_path = "__tmpLSMSaveTest/";
  std::filesystem::create_directories(options.db_path);
  uint32_t klen = 10, vlen = 13, N = 1e6;
  auto kv =
      GenKVDataWithRandomLen(0x202403181246, N, {klen - 1, klen}, {1, vlen});
  {
    auto lsm = DBImpl::Create(options);
    for (uint32_t i = 0; i < N; i++) {
      lsm->Put(kv[i].key(), kv[i].value());
    }
    lsm->Save();
  }
  std::sort(kv.begin(), kv.end());
  {
    options.create_new = false;
    auto lsm = DBImpl::Create(options);
    auto it = lsm->Begin();
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(it.key(), kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }

  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, LSMBigScanTest) {
  Options options;
  options.sst_file_size = 1 << 20;
  options.db_path = "__tmpLSMBigScanTest/";
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);

  uint32_t klen = 10, vlen = 514, N = 3e6;
  auto kv =
      GenKVDataWithRandomLen(0x202403180237, N, {klen - 1, klen}, {1, vlen});
  std::mt19937_64 rgen(0x202403180238);

  /* Insert all key-value pairs into LSM tree */
  wing::StopWatch sw;
  for (uint32_t i = 0; i < N; i++) {
    lsm->Put(kv[i].key(), kv[i].value());
  }
  DB_INFO("Put Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  std::sort(kv.begin(), kv.end());
  /* Test DBImpl::Begin. */
  {
    auto it = lsm->Begin();
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(it.key(), kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }
  DB_INFO("Scan All Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  /* Test DBImpl::Seek. */
  {
    for (uint32_t i = 0; i < 1000; i++) {
      uint32_t begin = rgen() % N;
      auto it = lsm->Seek(kv[begin].key());
      for (uint32_t j = begin; j < (N / 1000) + begin && j < N; j++) {
        ASSERT_TRUE(it.Valid());
        ASSERT_EQ(it.key(), kv[j].key());
        ASSERT_EQ(it.value(), kv[j].value());
        it.Next();
      }
    }
  }
  /* Test Save */
  lsm.reset();
  {
    options.create_new = false;
    lsm = DBImpl::Create(options);
    auto it = lsm->Begin();
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(it.key(), kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }
  DB_INFO("Short Range Scan Cost: {}s", sw.GetTimeInSeconds());
  /* Test the snapshot mechanism. */
  {
    auto it = lsm->Begin();
    /* The delete operations should not affect the data in the snapshot. */
    for (uint32_t i = 0; i < N; i++) {
      lsm->Del(kv[i].key());
    }
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(it.key(), kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }
  DB_INFO("Second Scan All Cost: {}s", sw.GetTimeInSeconds());
  /* Test if the iterator skips deleted elements. */
  {
    auto it = lsm->Begin();
    /* Because all the data are deleted. */
    ASSERT_FALSE(it.Valid());
    auto it2 = lsm->Seek(kv[10].key());
    ASSERT_FALSE(it2.Valid());
  }
  lsm.reset();
  DB_INFO("Write amplification: {}",
      GetStatsContext()->total_write_bytes.load() /
          GetStatsContext()->total_input_bytes.load());
  std::filesystem::remove_all(options.db_path);
}
