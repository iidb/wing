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
      4096, 10);
  uint32_t klen = 9, vlen = 13, N = 1e5;
  auto kv =
      GenKVDataWithRandomLen(0x202403152328, N, {klen - 1, klen}, {1, vlen});
  auto kv_notinsert =
      GenKVDataWithRandomLen(0x202403220115, 5e6, {klen - 1, klen}, {1, vlen});
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
  info.bloom_filter_offset_ = builder.GetBloomFilterOffset();
  info.sst_id_ = 0;
  SSTable sst(info, 4096, false);
  /* Test SSTable::Get */
  for (uint32_t i = 0; i < N; i++) {
    std::string value;
    ASSERT_EQ(sst.Get(kv[i].key(), 1, &value), GetResult::kFound);
    ASSERT_EQ(value, kv[i].value());
    ASSERT_EQ(sst.Get(kv[i].key(), 0, &value), GetResult::kNotFound);
  }
  /* Test bloom filter */
  wing::wing_testing::TestTimeout(
      [&]() {
        wing::StopWatch sw;
        for (uint32_t i = 0; i < kv_notinsert.size(); i++) {
          std::string value;
          ASSERT_EQ(
              sst.Get(kv_notinsert[i].key(), 1, &value), GetResult::kNotFound);
        }
        DB_INFO("Cost: {}s", sw.GetTimeInSeconds());
      },
      1000, "Did you utilize bloom filter? ");
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
        4096, 10);
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
    info.bloom_filter_offset_ = builder.GetBloomFilterOffset();
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
  for (uint32_t i = 0; i < fileN * 2; i++) {
    std::remove(fmt::format("__tmpSortedRunTest{}", i).c_str());
  }
}

TEST(LSMTest, IteratorHeapTest) {
  uint32_t klen = 9, vlen = 50, N = 1e6, fileN = 10;
  auto kv = GenKVData(0x202403152328, N, klen, vlen);
  std::vector<std::shared_ptr<SSTable>> ssts;
  /* Create 20 SSTables */
  for (uint32_t i = 0; i < fileN * 2; i++) {
    auto L = (i >= fileN ? i - fileN : i) * (N / fileN);
    auto R = std::min(L + (N / fileN), N);
    std::sort(kv.begin() + L, kv.begin() + R);
    SSTableBuilder builder(
        std::make_unique<FileWriter>(
            std::make_unique<SeqWriteFile>(
                fmt::format("__tmpLSMIteratorHeapTest{}", i), false),
            4096),
        4096, 10);
    /* Build the SSTable */
    for (uint32_t j = L; j < R; j++) {
      builder.Append(
          ParsedKey(kv[j].key(), i >= fileN ? 114514 : 1, RecordType::Value),
          kv[j].value());
    }
    builder.Finish();
    /* Add the SSTable*/
    SSTInfo info;
    info.count_ = builder.count();
    info.filename_ = fmt::format("__tmpLSMIteratorHeapTest{}", i);
    info.index_offset_ = builder.GetIndexOffset();
    info.bloom_filter_offset_ = builder.GetBloomFilterOffset();
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
    ASSERT_EQ(ParsedKey(its.key()).seq_, 114514);
    ASSERT_EQ(its.value(), kv[i].value());
    its.Next();
    ASSERT_TRUE(its.Valid());
    ASSERT_EQ(ParsedKey(its.key()).user_key_, kv[i].key());
    ASSERT_EQ(ParsedKey(its.key()).seq_, 1);
    ASSERT_EQ(its.value(), kv[i].value());
    its.Next();
  }
  ASSERT_FALSE(its.Valid());
  /* Delete all SSTables */
  ssts.clear();
  for (uint32_t i = 0; i < fileN * 2; i++) {
    std::remove(fmt::format("__tmpLSMIteratorHeapTest{}", i).c_str());
  }
}

TEST(LSMTest, SuperVersionTest) {
  auto mt = std::make_shared<MemTable>();
  auto imms = std::make_shared<std::vector<std::shared_ptr<MemTable>>>();
  auto version = std::make_shared<Version>();
  std::filesystem::create_directories("__tmpSuperVersionTest");
  uint32_t klen = 9, vlen = 50;
  auto kv_mt = GenKVData(0x202403220134, 1e4, klen, vlen);
  for (auto& k : kv_mt) {
    mt->Put(k.key(), 114514, k.value());
  }
  auto kv_imm1 = GenKVData(0x202403220135, 1e4, klen, vlen);
  auto kv_imm2 = GenKVData(0x202403220136, 1e4, klen, vlen);
  auto imm1 = std::make_shared<MemTable>();
  auto imm2 = std::make_shared<MemTable>();
  for (auto& k : kv_imm1) {
    imm1->Put(k.key(), 114514, k.value());
  }
  for (auto& k : kv_imm2) {
    imm2->Put(k.key(), 114514, k.value());
  }
  imms->push_back(imm1);
  imms->push_back(imm2);
  int sst_id = 0;
  auto gen_sr = [&](uint32_t fileN, auto& kv, int seq,
                    RecordType type) -> std::shared_ptr<SortedRun> {
    std::vector<SSTInfo> sst_infos;
    std::sort(kv.begin(), kv.end());
    /* Create SSTables */
    wing::StopWatch sw;
    uint32_t N = kv.size();
    for (uint32_t i = 0; i < fileN; i++) {
      auto L = (i) * (N / fileN);
      auto R = std::min((i + 1) * (N / fileN), N);
      SSTableBuilder builder(
          std::make_unique<FileWriter>(
              std::make_unique<SeqWriteFile>(
                  fmt::format("__tmpSuperVersionTest/{}.sst", ++sst_id), false),
              1 << 20),
          4096, 10);
      /* Build the SSTable */
      for (uint32_t j = L; j < R; j++) {
        builder.Append(ParsedKey(kv[j].key(), seq, type), kv[j].value());
      }
      builder.Finish();
      /* Add the SSTable*/
      SSTInfo info;
      info.count_ = builder.count();
      info.filename_ = fmt::format("__tmpSuperVersionTest/{}.sst", sst_id);
      info.index_offset_ = builder.GetIndexOffset();
      info.bloom_filter_offset_ = builder.GetBloomFilterOffset();
      info.size_ = builder.size();
      info.sst_id_ = i;
      sst_infos.emplace_back(info);
    }
    DB_INFO("Cost: {}", sw.GetTimeInSeconds());
    return std::make_shared<SortedRun>(sst_infos, 4096, false);
  };

  auto kv_sr1 = GenKVData(0x202403220137, 5e5, klen, vlen);
  auto kv_sr2 = GenKVData(0x202403220138, 5e5, klen, vlen);
  auto kv_sr3 = GenKVData(0x202403220139, 5e5, klen, vlen);
  auto kv_sr4 = GenKVData(0x202403220140, 5e5, klen, vlen);
  version->Append(0, gen_sr(10, kv_sr1, 2, RecordType::Value));
  version->Append(0, gen_sr(10, kv_sr2, 2, RecordType::Value));
  version->Append(1, gen_sr(10, kv_sr3, 2, RecordType::Value));
  version->Append(2, gen_sr(10, kv_sr4, 2, RecordType::Value));
  version->Append(3, gen_sr(10, kv_sr1, 1, RecordType::Deletion));
  version->Append(3, gen_sr(10, kv_sr2, 1, RecordType::Deletion));
  auto sv = std::make_shared<SuperVersion>(mt, imms, version);

  auto merge_kv = [&](auto& dst, auto& src) {
    dst.insert(dst.end(), src.begin(), src.end());
  };

  auto kv = std::vector<CompressedKVPair>();
  merge_kv(kv, kv_mt);
  merge_kv(kv, kv_imm1);
  merge_kv(kv, kv_imm2);
  merge_kv(kv, kv_sr1);
  merge_kv(kv, kv_sr2);
  merge_kv(kv, kv_sr3);
  merge_kv(kv, kv_sr4);

  auto kv_seq1 = std::vector<CompressedKVPair>();
  merge_kv(kv_seq1, kv_sr1);
  merge_kv(kv_seq1, kv_sr2);

  auto kv_seq2 = std::vector<CompressedKVPair>();
  merge_kv(kv_seq2, kv_sr1);
  merge_kv(kv_seq2, kv_sr2);
  merge_kv(kv_seq2, kv_sr3);
  merge_kv(kv_seq2, kv_sr4);

  auto kv_seq114514 = std::vector<CompressedKVPair>();
  merge_kv(kv_seq114514, kv_mt);
  merge_kv(kv_seq114514, kv_imm1);
  merge_kv(kv_seq114514, kv_imm2);

  /* Test SuperVersion::Get */
  wing::StopWatch sw;
  std::vector<std::future<void>> pool;
  for (uint32_t T = 0; T < 4; T++) {
    pool.push_back(std::async([&, T]() {
      uint32_t step = kv.size() / 4;
      auto L = step * T, R = step * T + step;
      for (uint32_t i = L; i < kv.size() && i < R; i++) {
        std::string value;
        ASSERT_TRUE(sv->Get(kv[i].key(), 114514, &value));
        ASSERT_EQ(value, kv[i].value());
      }
    }));
  }
  wing::wing_testing::TestTimeout(
      [&]() {
        for (auto& f : pool)
          f.get();
        pool.clear();
      },
      6000, "Your get is too slow!");
  DB_INFO("SuperVersion::Get Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  wing::wing_testing::TestTimeout(
      [&]() {
        for (uint32_t i = 0; i < kv_seq1.size(); i++) {
          std::string value;
          ASSERT_FALSE(sv->Get(kv_seq1[i].key(), 1, &value));
        }
      },
      4000, "Your get is too slow!");
  wing::wing_testing::TestTimeout(
      [&]() {
        for (uint32_t i = 0; i < kv_seq114514.size(); i++) {
          std::string value;
          ASSERT_FALSE(sv->Get(kv_seq114514[i].key(), 1, &value));
        }
      },
      6000, "Your get is too slow!");

  DB_INFO("SuperVersion::Get Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  std::sort(kv.begin(), kv.end());
  std::sort(kv_seq2.begin(), kv_seq2.end());
  /* Test SuperVersionIterator::SeekToFirst */
  wing::wing_testing::TestTimeout(
      [&]() {
        auto it = DBIterator(sv, 114514);
        it.SeekToFirst();
        for (uint32_t j = 0; j < kv.size(); j++) {
          ASSERT_TRUE(it.Valid());
          ASSERT_EQ(it.key(), kv[j].key());
          ASSERT_EQ(it.value(), kv[j].value());
          it.Next();
        }
        ASSERT_FALSE(it.Valid());
      },
      5000, "Your scan is too slow!");
  DB_INFO("Full Scan Cost: {}s", sw.GetTimeInSeconds());
  /* Test SuperVersionIterator::SeekToFirst using seq = 1 */
  wing::wing_testing::TestTimeout(
      [&]() {
        auto it = DBIterator(sv, 1);
        it.SeekToFirst();
        ASSERT_FALSE(it.Valid());
      },
      5000, "Your scan is too slow!");
  DB_INFO("Full Scan Cost: {}s", sw.GetTimeInSeconds());
  /* Test SuperVersionIterator::SeekToFirst using seq = 2 */
  wing::wing_testing::TestTimeout(
      [&]() {
        auto it = DBIterator(sv, 2);
        it.SeekToFirst();
        for (uint32_t j = 0; j < kv_seq2.size(); j++) {
          ASSERT_TRUE(it.Valid());
          ASSERT_EQ(it.key(), kv_seq2[j].key());
          ASSERT_EQ(it.value(), kv_seq2[j].value());
          it.Next();
        }
        ASSERT_FALSE(it.Valid());
      },
      5000, "Your scan is too slow!");
  DB_INFO("Full Scan Cost: {}s", sw.GetTimeInSeconds());
  /* Test SuperVersionIterator::Seek */
  wing::wing_testing::TestTimeout(
      [&]() {
        for (uint32_t i = 0; i < 250; i++) {
          size_t step = (kv.size() / 249);
          size_t id = i * step;
          auto it = DBIterator(sv, 114514);
          it.Seek(kv[id].key());
          for (uint32_t j = id; j < kv.size() && j < id + step * 10; j++) {
            ASSERT_TRUE(it.Valid());
            ASSERT_EQ(it.key(), kv[j].key());
            ASSERT_EQ(it.value(), kv[j].value());
            it.Next();
          }
        }
      },
      15000, "Your seek is too slow!");
  DB_INFO("Short Range Scan Cost: {}s", sw.GetTimeInSeconds());
  sv.reset();
  std::filesystem::remove_all("__tmpSuperVersionTest");
}

//// CompactionJob Test

TEST(LSMTest, CompactionBasicTest) {
  class Iterator {
   public:
    Iterator(std::vector<CompressedKVPair>& kv, seq_t seq, RecordType type)
      : kv_(kv), seq_(seq), type_(type) {
      if (kv_.size() > 0) {
        key_ = InternalKey(kv_[0].key(), seq_, type_);
      }
    }

    Slice key() { return key_.GetSlice(); }

    Slice value() {
      current_v_ = kv_[id_].value();
      return current_v_;
    }

    void Next() {
      id_ += 1;
      if (Valid()) {
        key_ = InternalKey(kv_[id_].key(), seq_, type_);
      }
    }

    bool Valid() { return id_ < kv_.size(); }

   private:
    std::vector<CompressedKVPair>& kv_;
    uint32_t id_{0};
    seq_t seq_{0};
    InternalKey key_;
    RecordType type_;
    std::string current_v_;
  };
  {
    auto sst_size = 104857;
    auto filegen =
        std::make_unique<FileNameGenerator>("__tmpCompactionBasicTest", 0);
    CompactionJob worker(filegen.get(), 4096, sst_size, 16384, 10, false);

    uint32_t klen = 9, vlen = 13, N = 1e6;
    auto kv = GenKVData(0x202403170002, N, klen, vlen);
    std::sort(kv.begin(), kv.end());
    auto ssts = worker.Run(Iterator(kv, 1, RecordType::Value));
    SortedRun level(ssts, 4096, false);
    auto it = level.Begin();
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(ParsedKey(it.key()).user_key_, kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    for (uint32_t i = 0; i + 1 < level.GetSSTs().size(); i++) {
      auto& L = level.GetSSTs()[i];
      auto& R = level.GetSSTs()[i + 1];
      ASSERT_TRUE(L->GetSSTInfo().size_ <= sst_size * 1.1);
      ASSERT_TRUE(
          L->GetSmallestKey().user_key_ < R->GetSmallestKey().user_key_ &&
          L->GetLargestKey().user_key_ != R->GetSmallestKey().user_key_);
    }
    ASSERT_FALSE(it.Valid());
    /* Delete all SSTables */
    for (uint32_t i = 0; i < ssts.size(); i++) {
      std::remove(ssts[i].filename_.c_str());
    }
  }
  // Test duplicate keys.
  {
    auto sst_size = 104857;
    auto filegen =
        std::make_unique<FileNameGenerator>("__tmpCompactionBasicTest", 0);
    CompactionJob worker(filegen.get(), 4096, sst_size, 16384, 10, false);

    uint32_t klen = 9, vlen = 13, N = 1e6;
    auto kv = GenKVData(0x202403282017, N, klen, vlen);
    std::sort(kv.begin(), kv.end());
    auto kv2 = std::vector<CompressedKVPair>(kv.begin(), kv.begin() + N / 2);
    std::vector<Iterator> iters;
    iters.push_back(Iterator(kv, 1, RecordType::Value));
    iters.push_back(Iterator(kv2, 2, RecordType::Deletion));
    IteratorHeap<Iterator> heap;
    for (auto& it : iters) {
      heap.Push(&it);
    }
    heap.Build();
    auto ssts = worker.Run(heap);
    SortedRun level(ssts, 4096, false);
    auto it = level.Begin();
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(ParsedKey(it.key()).user_key_, kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      ASSERT_EQ(ParsedKey(it.key()).seq_, (i < N / 2 ? 2 : 1));
      it.Next();
    }
    for (uint32_t i = 0; i + 1 < level.GetSSTs().size(); i++) {
      auto& L = level.GetSSTs()[i];
      auto& R = level.GetSSTs()[i + 1];
      ASSERT_TRUE(L->GetSSTInfo().size_ <= sst_size * 1.1);
      ASSERT_TRUE(
          L->GetSmallestKey().user_key_ < R->GetSmallestKey().user_key_ &&
          L->GetLargestKey().user_key_ != R->GetSmallestKey().user_key_);
    }
    ASSERT_FALSE(it.Valid());
    /* Delete all SSTables */
    for (uint32_t i = 0; i < ssts.size(); i++) {
      std::remove(ssts[i].filename_.c_str());
    }
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
  lsm.reset();
  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, LSMSmallGetTest) {
  Options options;
  options.compaction_strategy_name = "leveled";
  options.sst_file_size = 1 << 20;
  options.compaction_size_ratio = 4;
  options.db_path = "__tmpLSMSmallGetTest/";
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);

  uint32_t klen = 10, vlen = 15, N = 1e6;
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
  wing::wing_testing::TestTimeout(
      [&]() {
        for (uint32_t i = 0; i < N; i++) {
          std::string value;
          ASSERT_TRUE(lsm->Get(kv[i].key(), &value));
          ASSERT_EQ(value, kv[i].value());
          lsm->Del(kv[i].key());
        }
      },
      15000, "Get&Del is too slow!");
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
  options.compaction_strategy_name = "leveled";
  options.sst_file_size = 1 << 20;
  options.compaction_size_ratio = 4;
  options.db_path = "__tmpLSMSmallScanTest/";
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);

  uint32_t klen = 10, vlen = 13, N = 5e6;
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

  wing::wing_testing::TestTimeout(
      [&]() {
        auto it = lsm->Begin();
        for (uint32_t i = 0; i < N; i++) {
          ASSERT_TRUE(it.Valid());
          ASSERT_EQ(it.key(), kv[i].key());
          ASSERT_EQ(it.value(), kv[i].value());
          it.Next();
        }
        ASSERT_FALSE(it.Valid());
      },
      10000, "Your scan is too slow!");
  DB_INFO("Scan All Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  /* Test DBImpl::Seek. */
  wing::wing_testing::TestTimeout(
      [&]() {
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
      },
      5000, "Your Seek&Short Range Scan is too slow!");

  DB_INFO("Short Range Scan Cost: {}s", sw.GetTimeInSeconds());
  /* Test the snapshot mechanism (snapshot for a Version). */
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

bool SanityCheck(DBImpl* lsm) {
  auto options = lsm->GetOptions();
  auto it = std::filesystem::directory_iterator(options.db_path);
  auto it_end = std::filesystem::directory_iterator();
  size_t sst_cnt = 0;
  while (it != it_end) {
    if (it->is_regular_file() &&
        it->path().filename().string().find(".sst") != std::string::npos) {
      if (it->file_size() > options.sst_file_size * 1.1) {
        DB_INFO("SSTable size exceeds limit {}", options.sst_file_size * 1.1);
        return false;
      }
      sst_cnt++;
    }
    it++;
  }

  size_t sst_cnt_lsm = 0;
  auto sv = lsm->GetSV();
  auto version = sv->GetVersion();
  for (auto& level : version->GetLevels()) {
    size_t level_size = 0;
    for (auto& sr : level.GetRuns()) {
      size_t sr_size = 0;
      for (size_t i = 0; auto& sst : sr->GetSSTs()) {
        sst_cnt_lsm += 1;
        sr_size += sst->GetSSTInfo().size_;
        if (std::filesystem::file_size(sst->GetSSTInfo().filename_) !=
            sst->GetSSTInfo().size_) {
          DB_INFO("SSTable size information is wrong! Actual {}, expected {}",
              std::filesystem::file_size(sst->GetSSTInfo().filename_),
              sst->GetSSTInfo().size_);
          return false;
        }
        if (i + 1 < sr->GetSSTs().size()) {
          auto& L = sr->GetSSTs()[i];
          auto& R = sr->GetSSTs()[i + 1];
          if (L->GetSSTInfo().size_ > lsm->GetOptions().sst_file_size * 1.1) {
            DB_INFO("SSTable size {} exceeds limit {} x 1.1",
                L->GetSSTInfo().size_, lsm->GetOptions().sst_file_size);
            return false;
          }
          if (!(L->GetSmallestKey().user_key_ < R->GetSmallestKey().user_key_ &&
                  L->GetLargestKey().user_key_ !=
                      R->GetSmallestKey().user_key_)) {
            DB_INFO(
                "SSTable order is wrong! Range of the SSTable is ({}, {}) and "
                "({}, {})",
                L->GetSmallestKey().user_key_, L->GetLargestKey().user_key_,
                R->GetSmallestKey().user_key_, R->GetLargestKey().user_key_);
            return false;
          }
        }
        i += 1;
      }
      if (sr_size != sr->size()) {
        DB_INFO("SortedRun size information is wrong! Actual {}, expected {}",
            sr->size(), sr_size);
        return false;
      }
      level_size += sr_size;
    }
    if (level_size != level.size()) {
      DB_INFO("Level size information is wrong! Actual {}, expected {}",
          level.size(), level_size);
      return false;
    }
  }
  // The number of SSTables in the directory
  // must equal to the number of SSTables in the LSM tree.
  if (sst_cnt != sst_cnt_lsm) {
    DB_INFO(
        "the number of SSTables in LSM-tree ({}) is not equal to the number of "
        "SSTable files ({})",
        sst_cnt_lsm, sst_cnt);
    return false;
  }
  return true;
}

TEST(LSMTest, LSMSmallMultithreadGetPutTest) {
  uint32_t TH = 4;
  Options options;
  options.compaction_strategy_name = "leveled";
  options.sst_file_size = 1 << 20;
  options.compaction_size_ratio = 4;
  options.db_path = "__tmpLSMMultithreadGetPutTest/";
  std::filesystem::remove_all(options.db_path);
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);

  std::vector<std::thread> pool;
  for (uint32_t i = 0; i < TH; i++) {
    pool.emplace_back([&, seed = i]() -> void {
      uint32_t klen = 10, vlen = 130, N = 1e5;
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
    });
  }
  for (auto& f : pool)
    f.join();

  lsm->WaitForFlushAndCompaction();
  ASSERT_TRUE(SanityCheck(lsm.get()));
  lsm.reset();
  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, LSMSaveTest) {
  Options options;
  options.compaction_strategy_name = "leveled";
  options.sst_file_size = 1 << 20;
  options.db_path = "__tmpLSMSaveTest/";
  std::filesystem::remove_all(options.db_path);
  std::filesystem::create_directories(options.db_path);
  uint32_t klen = 10, vlen = 130, N = 1e6;
  auto kv =
      GenKVDataWithRandomLen(0x202403181246, N, {klen - 1, klen}, {1, vlen});
  {
    auto lsm = DBImpl::Create(options);
    for (uint32_t i = 0; i < N; i++) {
      lsm->Put(kv[i].key(), kv[i].value());
    }
    lsm->Save();
  }

  {
    options.create_new = false;
    auto lsm = DBImpl::Create(options);
    for (uint32_t i = 0; i < 1000 && i < N; i++) {
      std::string value;
      ASSERT_TRUE(lsm->Get(kv[i].key(), &value));
      ASSERT_EQ(value, kv[i].value());
    }
    auto it = lsm->Begin();
    std::sort(kv.begin(), kv.end());
    for (uint32_t i = 0; i < N; i++) {
      ASSERT_TRUE(it.Valid());
      ASSERT_EQ(it.key(), kv[i].key());
      ASSERT_EQ(it.value(), kv[i].value());
      it.Next();
    }
    ASSERT_FALSE(it.Valid());
  }

  // Check the number/size of SSTables in the directory
  {
    options.create_new = false;
    auto lsm = DBImpl::Create(options);
    ASSERT_TRUE(SanityCheck(lsm.get()));
  }

  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, LSMBigScanTest) {
  Options options;
  options.compaction_strategy_name = "leveled";
  options.sst_file_size = 1 << 20;
  options.db_path = "__tmpLSMBigScanTest/";
  std::filesystem::remove_all(options.db_path);
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);
  GetStatsContext()->Reset();

  uint32_t klen = 10, vlen = 514, N = 3e6;
  auto kv =
      GenKVDataWithRandomLen(0x202403180237, N, {klen - 1, klen}, {1, vlen});
  std::mt19937_64 rgen(0x202403180238);

  /* Insert all key-value pairs into LSM tree */
  wing::StopWatch sw;
  wing::wing_testing::TestTimeout(
      [&]() {
        for (uint32_t i = 0; i < N; i++) {
          lsm->Put(kv[i].key(), kv[i].value());
        }
      },
      25000, "Your put is slow!");
  DB_INFO("Put Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  std::sort(kv.begin(), kv.end());
  /* Test DBImpl::Begin. */
  wing::wing_testing::TestTimeout(
      [&]() {
        auto it = lsm->Begin();
        for (uint32_t i = 0; i < N; i++) {
          ASSERT_TRUE(it.Valid());
          ASSERT_EQ(it.key(), kv[i].key());
          ASSERT_EQ(it.value(), kv[i].value());
          it.Next();
        }
        ASSERT_FALSE(it.Valid());
      },
      10000, "Your Scan is too slow!");
  DB_INFO("Scan All Cost: {}s", sw.GetTimeInSeconds());
  sw.Reset();
  /* Test DBImpl::Seek. */
  wing::wing_testing::TestTimeout(
      [&]() {
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
      },
      7000, "Your Seek is too slow!");
  DB_INFO("Seek Cost: {}s", sw.GetTimeInSeconds());
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
  DB_INFO("Second Scan All Cost: {}s", sw.GetTimeInSeconds());
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
  DB_INFO("Third Scan All Cost: {}s", sw.GetTimeInSeconds());
  /* Test if the iterator skips deleted elements. */
  {
    auto it = lsm->Begin();
    /* Because all the data are deleted. */
    ASSERT_FALSE(it.Valid());
    auto it2 = lsm->Seek(kv[10].key());
    ASSERT_FALSE(it2.Valid());
  }
  lsm->FlushAll();
  lsm->WaitForFlushAndCompaction();
  ASSERT_TRUE(SanityCheck(lsm.get()));
  lsm.reset();
  auto wa = GetStatsContext()->total_write_bytes.load() /
            (double)GetStatsContext()->total_input_bytes.load();
  DB_INFO("Write amplification: {}", wa);
  ASSERT_TRUE(wa <= 12);
  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, LSMDuplicateKeyTest) {
  Options options;
  options.compaction_strategy_name = "leveled";
  options.sst_file_size = 1 << 20;
  options.compaction_size_ratio = 10;
  options.db_path = "__tmpLSMBigScanTest/";
  std::filesystem::remove_all(options.db_path);
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);

  uint32_t klen = 10, vlen = 514, N = 3e5;
  auto kv =
      GenKVDataWithRandomLen(0x202403291504, N, {klen - 1, klen}, {1, vlen});
  std::mt19937_64 rgen(0x202403291506);
  for (uint32_t i = 0; i < 10; i++) {
    std::shuffle(kv.begin(), kv.end(), rgen);
    for (auto& k : kv) {
      lsm->Put(k.key(), k.value());
    }
  }
  lsm->WaitForFlushAndCompaction();
  ASSERT_TRUE(SanityCheck(lsm.get()));

  // Check the size of each level of the LSM tree.
  // The size of each level should <= total kv size * 1.1.
  size_t kv_size = 0;
  for (auto& k : kv) {
    kv_size += k.key().size() + k.vlen_ + sizeof(offset_t) * 3;
  }
  kv_size *= 1.1;
  auto level_size_limit =
      options.level0_compaction_trigger * options.sst_file_size;
  for (auto& level : lsm->GetSV()->GetVersion()->GetLevels()) {
    ASSERT_TRUE(level.size() <= kv_size);
    ASSERT_TRUE(level.size() <= level_size_limit);
    level_size_limit *= options.compaction_size_ratio;
  }
}

TEST(LSMTest, LeveledCompactionTest) {
  Options options;
  options.sst_file_size = 1 << 20;
  options.compaction_size_ratio = 8;
  options.compaction_strategy_name = "leveled";
  options.db_path = "__tmpLeveledCompactionTest/";
  std::filesystem::remove_all(options.db_path);
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);
  GetStatsContext()->Reset();

  uint32_t klen = 10, vlen = 514, N = 5e6;
  auto kv =
      GenKVDataWithRandomLen(0x202403282311, N, {klen - 1, klen}, {1, vlen});

  /* Insert all key-value pairs into LSM tree */
  wing::StopWatch sw;
  wing::wing_testing::TestTimeout(
      [&]() {
        for (uint32_t i = 0; i < N; i++) {
          lsm->Put(kv[i].key(), kv[i].value());
        }
        lsm->FlushAll();
        lsm->WaitForFlushAndCompaction();
      },
      40000, "Your compaction is too slow!");
  DB_INFO("Put Cost {}s.", sw.GetTimeInSeconds());
  // Check the number of sorted runs at each level
  {
    auto sv = lsm->GetSV();
    auto version = sv->GetVersion();
    auto level_size_limit =
        options.level0_compaction_trigger * options.sst_file_size;
    for (auto& level : version->GetLevels()) {
      if (level.GetID() == 0) {
        ASSERT_TRUE(
            level.GetRuns().size() <= options.level0_stop_writes_trigger);
      } else {
        ASSERT_TRUE(level.GetRuns().size() == 1);
      }
      size_t level_size = 0;
      for (auto& sr : level.GetRuns()) {
        for (auto& sst : sr->GetSSTs()) {
          level_size += sst->GetSSTInfo().size_;
        }
      }
      ASSERT_TRUE(level_size <= level_size_limit);
      level_size_limit *= options.compaction_size_ratio;
    }
  }
  ASSERT_TRUE(SanityCheck(lsm.get()));
  lsm.reset();
  auto wa = GetStatsContext()->total_write_bytes.load() /
            (double)GetStatsContext()->total_input_bytes.load();
  DB_INFO("Write amplification: {}", wa);
  ASSERT_TRUE(wa <= 12);
  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, TieredCompactionTest) {
  Options options;
  options.sst_file_size = 1 << 20;
  options.compaction_size_ratio = 8;
  options.max_immutable_count = 20;
  options.level0_stop_writes_trigger = 4;
  options.compaction_strategy_name = "tiered";
  options.db_path = "__tmpLeveledCompactionTest/";
  std::filesystem::remove_all(options.db_path);
  std::filesystem::create_directories(options.db_path);
  auto lsm = DBImpl::Create(options);
  GetStatsContext()->Reset();

  uint32_t klen = 10, vlen = 514, N = 5e6;
  auto kv =
      GenKVDataWithRandomLen(0x202403282311, N, {klen - 1, klen}, {1, vlen});

  /* Insert all key-value pairs into LSM tree */
  wing::StopWatch sw;
  wing::wing_testing::TestTimeout(
      [&]() {
        for (uint32_t i = 0; i < N; i++) {
          lsm->Put(kv[i].key(), kv[i].value());
        }
        lsm->FlushAll();
        lsm->WaitForFlushAndCompaction();
      },
      25000, "Your compaction is too slow!");
  DB_INFO("Put Cost {}s.", sw.GetTimeInSeconds());
  // Check the number of sorted runs at each level
  {
    auto sv = lsm->GetSV();
    auto version = sv->GetVersion();
    auto level_size_limit =
        options.level0_compaction_trigger * options.sst_file_size;
    for (auto& level : version->GetLevels()) {
      if (level.GetID() == 0) {
        ASSERT_TRUE(
            level.GetRuns().size() <= options.level0_stop_writes_trigger);
      } else {
        ASSERT_TRUE(level.GetRuns().size() <= options.compaction_size_ratio);
      }
      size_t level_size = 0;
      for (auto& sr : level.GetRuns()) {
        for (auto& sst : sr->GetSSTs()) {
          level_size += sst->GetSSTInfo().size_;
        }
      }
      ASSERT_TRUE(level_size <= level_size_limit);
      level_size_limit *= options.compaction_size_ratio;
    }
  }
  ASSERT_TRUE(SanityCheck(lsm.get()));
  lsm.reset();
  auto wa = GetStatsContext()->total_write_bytes.load() /
            (double)GetStatsContext()->total_input_bytes.load();
  DB_INFO("Write amplification: {}", wa);
  ASSERT_TRUE(wa <= 4.1);
  std::filesystem::remove_all(options.db_path);
}

TEST(LSMTest, Benchmark1) {}
