#include <gtest/gtest.h>
#include <cstdlib>
#include <optional>
#include <random>

#include "storage/bplus-tree.hpp"
#include "storage/blob.hpp"

namespace fs = std::filesystem;

#define FAIL_NO_RETURN() \
  GTEST_MESSAGE_("Failed", ::testing::TestPartResult::kFatalFailure)

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
// explicit deduction guide (not needed as of C++20)
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

template <typename Val, typename... Ts>
auto match(Val val, Ts... ts) {
  return std::visit(overloaded{ts...}, val);
}

template <typename T>
T pow(T base, int ex) {
  T ans = 1;
  for (; ex; ex >>= 1, base *= base)
    if (ex & 1)
      ans *= base;
  return ans;
}

static inline std::string test_name() {
  auto info = ::testing::UnitTest::GetInstance()->current_test_info();
  return std::string(info->test_suite_name()) + '.' + info->name();
}

template <typename Engine>
std::string rand_digits(Engine& e, size_t n) {
  std::uniform_int_distribution<char> dist(0, 9);
  std::string ret;
  ret.reserve(n);
  while (n--) {
    ret.push_back(dist(e) + '0');
  }
  return ret;
}

typedef wing::BPlusTree<std::compare_three_way> tree_t;
typedef std::map<std::string, std::string> map_t;

// 128MB of buffer
constexpr size_t MAX_BUF_PAGES = 32 * 1024;

struct Env {
  std::minstd_rand& e;
  tree_t& tree;
  map_t& m;
  size_t max_key_len;
  size_t max_val_len;
  bool rand_len = false;
  size_t max_scan_len = 50;
};

static std::string gen_key(Env& env) {
  size_t key_len;
  if (env.rand_len)
    key_len = std::uniform_int_distribution<size_t>(1, env.max_key_len)(env.e);
  else
    key_len = env.max_key_len;
  return rand_digits(env.e, key_len);
}
static std::string gen_value(Env& env) {
  size_t value_len;
  if (env.rand_len)
    value_len =
      std::uniform_int_distribution<size_t>(1, env.max_val_len)(env.e);
  else
    value_len = env.max_val_len;
  return rand_digits(env.e, value_len);
}

static void try_insert(Env& env) {
  std::string key = gen_key(env);
  std::string value = gen_value(env);
  bool succeed = env.tree.Insert(key, value);
  auto std_ret = env.m.insert({key, value});
  ASSERT_EQ(std_ret.second, succeed);
}

static void try_update(Env& env) {
  std::string key = gen_key(env);
  std::string value = gen_value(env);
  bool exists = env.tree.Update(key, value);
  auto std_ret = env.m.find(key);
  if (std_ret == env.m.end()) {
    ASSERT_FALSE(exists);;
  } else {
    ASSERT_TRUE(exists);
    std_ret->second = value;
  }
}

static void get(Env& env) {
  std::string key = gen_key(env);
  auto ret = env.tree.Get(key);
  auto it = env.m.find(key);
  if (it != env.m.end()) {
    ASSERT_TRUE(ret.has_value());
    ASSERT_EQ(ret.value(), it->second);
  } else {
    ASSERT_FALSE(ret);
  }
}

static void take(Env& env) {
  if (env.m.empty()) {
    ASSERT_TRUE(env.tree.IsEmpty());
    return;
  }
  std::string key = gen_key(env);
  auto std_it = env.m.find(key);
  auto taken = env.tree.Take(key);
  if (std_it == env.m.end()) {
    ASSERT_EQ(taken, std::nullopt);
  } else {
    ASSERT_EQ(taken, std::make_optional(std_it->second));
    env.m.erase(std_it);
  }
}
static void take_nearby(Env& env) {
  if (env.m.empty()) {
    ASSERT_TRUE(env.tree.IsEmpty());
    return;
  }
  std::string key = gen_key(env);
  auto std_it = env.m.lower_bound(key);
  if (std_it == env.m.end()) {
    --std_it;
  }
  ASSERT_EQ(env.tree.Take(std_it->first), std_it->second);
  env.m.erase(std_it);
}

static void lower_bound(Env& env) {
  std::string key = gen_key(env);
  auto std_it = env.m.lower_bound(key);
  auto it = env.tree.LowerBound(key);
  if (std_it == env.m.end()) {
    ASSERT_FALSE(it.Cur().has_value());
  } else {
    auto ret = it.Cur();
    ASSERT_TRUE(ret.has_value());
    ASSERT_EQ(ret.value().second, std_it->second);
  }
}

static void upper_bound(Env& env) {
  std::string key = gen_key(env);
  auto std_it = env.m.upper_bound(key);
  auto it = env.tree.UpperBound(key);
  if (std_it == env.m.end()) {
    ASSERT_FALSE(it.Cur().has_value());
  } else {
    auto ret = it.Cur();
    ASSERT_TRUE(ret.has_value());
    ASSERT_EQ(ret.value().second, std_it->second);
  }
}

static void scan(Env& env) {
  std::uniform_int_distribution<size_t> scan_len_dist(0, env.max_scan_len + 1);
  std::string key = gen_key(env);
  size_t scan_len = scan_len_dist(env.e);
  auto std_it = env.m.lower_bound(key);
  auto it = env.tree.LowerBound(key);
  while (scan_len) {
    scan_len -= 1;
    auto ret = it.Cur();
    if (!ret.has_value()) {
      ASSERT_EQ(std_it, env.m.end());
      break;
    }
    ASSERT_NE(std_it, env.m.end());
    ASSERT_EQ(std_it->first, ret.value().first);
    ASSERT_EQ(std_it->second, ret.value().second);
    ++std_it;
    it.Next();
  }
}

struct OPNum {
  size_t insert = 0;
  size_t update = 0;
  size_t get = 0;
  size_t take = 0;
  size_t take_nearby = 0;
  size_t lower_bound = 0;
  size_t upper_bound = 0;
  size_t scan = 0;
};

static void rand_op(Env& env, OPNum num) {
  size_t tot = num.insert + num.update + num.get + num.take +
    num.take_nearby + num.lower_bound + num.upper_bound + num.scan;
  for (; tot; tot -= 1) {
    std::uniform_int_distribution<size_t> dist(0, tot - 1);
    size_t rand_num = dist(env.e);
    if (rand_num < num.insert) {
      num.insert -= 1;
      ASSERT_NO_FATAL_FAILURE(try_insert(env));
      continue;
    }
    rand_num -= num.insert;
    if (rand_num < num.update) {
      num.update -= 1;
      ASSERT_NO_FATAL_FAILURE(try_update(env));
      continue;
    }
    rand_num -= num.update;
    if (rand_num < num.get) {
      num.get -= 1;
      ASSERT_NO_FATAL_FAILURE(get(env));
      continue;
    }
    rand_num -= num.get;
    if (rand_num < num.take) {
      num.take -= 1;
      ASSERT_NO_FATAL_FAILURE(take(env));
      continue;
    }
    rand_num -= num.take;
    if (rand_num < num.take_nearby) {
      num.take_nearby -= 1;
      ASSERT_NO_FATAL_FAILURE(take_nearby(env));
      continue;
    }
    rand_num -= num.take_nearby;
    if (rand_num < num.lower_bound) {
      num.lower_bound -= 1;
      ASSERT_NO_FATAL_FAILURE(lower_bound(env));
      continue;
    }
    rand_num -= num.lower_bound;
    if (rand_num < num.upper_bound) {
      num.upper_bound -= 1;
      ASSERT_NO_FATAL_FAILURE(upper_bound(env));
      continue;
    }
    rand_num -= num.upper_bound;
    if (rand_num < num.scan) {
      num.scan -= 1;
      ASSERT_NO_FATAL_FAILURE(scan(env));
      continue;
    }
    rand_num -= num.scan;
    FAIL() << "Here should be unreachable";
  }
  ASSERT_EQ(num.insert, 0);
  ASSERT_EQ(num.update, 0);
  ASSERT_EQ(num.get, 0);
  ASSERT_EQ(num.take_nearby, 0);
  ASSERT_EQ(num.lower_bound, 0);
  ASSERT_EQ(num.upper_bound, 0);
  ASSERT_EQ(num.scan, 0);
}

static void test_rand_op(const std::filesystem::path& path, const OPNum& num,
    size_t key_len, size_t val_len) {
  {
    std::minstd_rand e(233);
    map_t m;
    auto pgm = wing::PageManager::Create(path, MAX_BUF_PAGES);
    auto tree = tree_t::Create(*pgm);
    Env env {
      .e = e,
      .tree = tree,
      .m = m,
      .max_key_len = key_len,
      .max_val_len = val_len,
    };
    ASSERT_NO_FATAL_FAILURE(rand_op(env, num));
  }
  ASSERT_TRUE(fs::remove(path));
}

static auto Create(
  const std::filesystem::path& path
) -> std::pair<
  std::unique_ptr<wing::PageManager>,
  wing::BPlusTree<std::compare_three_way>
> {
  auto pgm = wing::PageManager::Create(path, MAX_BUF_PAGES);
  assert(pgm->PageNum() == pgm->SuperPageID() + 1);
  auto tree = wing::BPlusTree<std::compare_three_way>::Create(*pgm);
  wing::pgid_t meta = tree.MetaPageID();
  pgm->GetPlainPage(pgm->SuperPageID()).Write(0,
    std::string_view(reinterpret_cast<const char *>(&meta), sizeof(meta)));
  return std::make_pair(std::move(pgm), std::move(tree));
}
static auto Open(
  const std::filesystem::path& path
) -> wing::Result<
  std::pair<
    std::unique_ptr<wing::PageManager>,
    wing::BPlusTree<std::compare_three_way>>,
  wing::io::Error
> {
  auto pgm = EXTRACT_RESULT(wing::PageManager::Open(path, MAX_BUF_PAGES));
  wing::pgid_t meta;
  pgm->GetPlainPage(pgm->SuperPageID()).Read(&meta, 0, sizeof(meta));
  auto tree = wing::BPlusTree<std::compare_three_way>::Open(*pgm, meta);
  return std::make_pair(std::move(pgm), std::move(tree));
}

struct SeqEnv {
  std::minstd_rand& e;
  tree_t& tree;
  map_t& m;
  size_t max_key;
  size_t max_val_len;
};
struct SeqOpNum {
  size_t insert = 0;
  size_t insert_rand_len = 0;
  size_t get = 0;
};

static std::string_view key_to_string_view(size_t& key) {
  return std::string_view(reinterpret_cast<const char *>(&key), sizeof(key));
}
static void seq_insert(SeqEnv& env, std::string&& value) {
  env.max_key += 1;
  size_t key = env.max_key;
  std::string_view key_view = key_to_string_view(key);
  bool succeed = env.tree.Insert(key_view, value);
  auto std_ret = env.m.emplace(std::string(key_view), std::move(value));
  ASSERT_EQ(succeed, std_ret.second);
}
static void seq_insert(SeqEnv& env) {
  seq_insert(env, rand_digits(env.e, env.max_val_len));
}
static void seq_insert_rand_len(SeqEnv& env) {
  size_t val_len =
    std::uniform_int_distribution<size_t>(1, env.max_val_len)(env.e);
  seq_insert(env, rand_digits(env.e, val_len));
}
static void seq_get(SeqEnv& env) {
  size_t k = std::uniform_int_distribution<size_t>(1, env.max_key)(env.e);
  std::string_view k_view = key_to_string_view(k);
  auto ret = env.tree.Get(k_view);
  auto std_ret = env.m.find(std::string(k_view));
  if (!ret.has_value()) {
    ASSERT_EQ(std_ret, env.m.end());
  } else {
    ASSERT_NE(std_ret, env.m.end());
    ASSERT_EQ(std_ret->second, ret.value());
  }
}

static void seq_op(SeqEnv& env, SeqOpNum num) {
  for (size_t tot = num.insert + num.insert_rand_len + num.get; tot; tot -= 1) {
    size_t rand_num = std::uniform_int_distribution<size_t>(0, tot - 1)(env.e);
    if (rand_num < num.insert) {
      num.insert -= 1;
      ASSERT_NO_FATAL_FAILURE(seq_insert(env));
      continue;
    }
    rand_num -= num.insert;
    if (rand_num < num.insert_rand_len) {
      num.insert_rand_len -= 1;
      ASSERT_NO_FATAL_FAILURE(seq_insert_rand_len(env));
      continue;
    }
    rand_num -= num.insert_rand_len;
    if (rand_num < num.get) {
      num.get -= 1;
      ASSERT_NO_FATAL_FAILURE(seq_get(env));
      continue;
    }
    rand_num -= num.get;
    FAIL() << "Here should be unreachable";
  }
  ASSERT_EQ(num.insert, 0);
  ASSERT_EQ(num.insert_rand_len, 0);
  ASSERT_EQ(num.get, 0);
}

static void test_seq_op(const fs::path& path, const SeqOpNum& num,
    size_t val_len) {
  {
    std::minstd_rand e(233);
    map_t m;
    auto [pgm, tree] = Create(path);
    SeqEnv env{
      .e = e,
      .tree = tree,
      .m = m,
      .max_key = 0,
      .max_val_len = val_len,
    };
    ASSERT_NO_FATAL_FAILURE(seq_op(env, num));
  }
  ASSERT_TRUE(fs::remove(path));
}

static auto create_rand_insert(
  const std::filesystem::path& path, std::minstd_rand& e, size_t magnitude
) -> std::tuple<std::unique_ptr<wing::PageManager>, tree_t, map_t> {
  size_t n = pow<size_t>(10, magnitude);
  map_t m;
  auto [pgm, tree] = Create(path);
  Env env {
    .e = e,
    .tree = tree,
    .m = m,
    .max_key_len = magnitude,
    .max_val_len = magnitude,
  };
  rand_op(env, OPNum{
    .insert = n,
  });
  return std::make_tuple(std::move(pgm), std::move(tree), std::move(m));
}
static auto create_seq_op(
  const fs::path& path, std::minstd_rand& e, size_t max_val_len,
  const SeqOpNum& num
) -> std::tuple<std::unique_ptr<wing::PageManager>, tree_t, map_t> {
  map_t m;
  auto [pgm, tree] = Create(path);
  SeqEnv env{
    .e = e,
    .tree = tree,
    .m = m,
    .max_key = 0,
    .max_val_len = max_val_len,
  };
  seq_op(env, num);
  return std::make_tuple(std::move(pgm), std::move(tree), std::move(m));
}
static auto create_seq_insert(
  const fs::path& path, std::minstd_rand& e, size_t magnitude, size_t val_len
) -> std::tuple<std::unique_ptr<wing::PageManager>, tree_t, map_t> {
  size_t n = pow<size_t>(10, magnitude);
  return create_seq_op(path, e, val_len, SeqOpNum{
    .insert = n,
  });
}
static auto create_seq_insert_rand_len(
  const fs::path& path, std::minstd_rand& e, size_t magnitude,
  size_t max_val_len
) -> std::tuple<std::unique_ptr<wing::PageManager>, tree_t, map_t> {
  size_t n = pow<size_t>(10, magnitude);
  return create_seq_op(path, e, max_val_len, SeqOpNum{
    .insert_rand_len = n,
  });
}

// Insert, Update, Get, MaxKey
TEST(BPlusTreeTest, Basic1) {
  std::string name = test_name();
  {
    auto pgm = wing::PageManager::Create(name, MAX_BUF_PAGES);
    auto tree = tree_t::Create(*pgm);
    ASSERT_TRUE(tree.IsEmpty());
    ASSERT_EQ(tree.MaxKey(), std::nullopt);

    ASSERT_TRUE(tree.Insert("123", "456"));
    ASSERT_FALSE(tree.Insert("123", "233"));
    ASSERT_EQ(tree.Get("123"), std::optional<std::string>("456"));
    ASSERT_EQ(tree.MaxKey(), std::optional<std::string>("123"));

    ASSERT_FALSE(tree.Update("114", "514"));
    ASSERT_TRUE(tree.Insert("114", "514"));
    ASSERT_EQ(tree.Get("114"), std::optional<std::string>("514"));
    ASSERT_EQ(tree.Get("123"), std::optional<std::string>("456"));
    ASSERT_TRUE(tree.Update("123", "233"));
    ASSERT_EQ(tree.Get("123"), std::optional<std::string>("233"));
    ASSERT_EQ(tree.MaxKey(), std::optional<std::string>("123"));

    ASSERT_FALSE(tree.Update("2333", "123"));
    ASSERT_FALSE(tree.Get("2333").has_value());
    ASSERT_TRUE(tree.Insert("2333", "123"));
    ASSERT_EQ(tree.MaxKey(), std::optional<std::string>("2333"));
  }
  ASSERT_TRUE(fs::remove(name));
}

static void rand_insert_get(const std::filesystem::path& path,
    size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .get = n,
  };
  ASSERT_NO_FATAL_FAILURE(test_rand_op(path, num, magnitude, magnitude));
}
TEST(BPlusTreeTest, RandInsertGet1e1) {
  rand_insert_get(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertGet1e2) {
  rand_insert_get(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertGet1e3) {
  rand_insert_get(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertGet1e4) {
  rand_insert_get(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertGet1e5) {
  rand_insert_get(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertGet1e6) {
  rand_insert_get(test_name(), 6);
}

static void seq_insert_get_value_16B(const fs::path& path, size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  SeqOpNum num{
    .insert = n,
    .get = n,
  };
  test_seq_op(path, num, 16);
}
TEST(BPlusTreeTest, SeqInsertGetValue16B1e1) {
  seq_insert_get_value_16B(test_name(), 1);
}
TEST(BPlusTreeTest, SeqInsertGetValue16B1e2) {
  seq_insert_get_value_16B(test_name(), 2);
}
TEST(BPlusTreeTest, SeqInsertGetValue16B1e3) {
  seq_insert_get_value_16B(test_name(), 3);
}
TEST(BPlusTreeTest, SeqInsertGetValue16B1e4) {
  seq_insert_get_value_16B(test_name(), 4);
}
TEST(BPlusTreeTest, SeqInsertGetValue16B1e5) {
  seq_insert_get_value_16B(test_name(), 5);
}
TEST(BPlusTreeTest, SeqInsertGetValue16B1e6) {
  seq_insert_get_value_16B(test_name(), 6);
}

static void seq_insert_get_value_1024B(const fs::path& path, size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  SeqOpNum num{
    .insert = n,
    .get = n,
  };
  test_seq_op(path, num, 1024);
}
TEST(BPlusTreeTest, SeqInsertGetValue1024B1e1) {
  seq_insert_get_value_1024B(test_name(), 1);
}
TEST(BPlusTreeTest, SeqInsertGetValue1024B1e2) {
  seq_insert_get_value_1024B(test_name(), 2);
}
TEST(BPlusTreeTest, SeqInsertGetValue1024B1e3) {
  seq_insert_get_value_1024B(test_name(), 3);
}
TEST(BPlusTreeTest, SeqInsertGetValue1024B1e4) {
  seq_insert_get_value_1024B(test_name(), 4);
}
TEST(BPlusTreeTest, SeqInsertGetValue1024B1e5) {
  seq_insert_get_value_1024B(test_name(), 5);
}

static void rand_insert_update_get(const std::filesystem::path& path,
    size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .update = n,
    .get = n,
  };
  ASSERT_NO_FATAL_FAILURE(test_rand_op(path, num, magnitude, magnitude));
}
TEST(BPlusTreeTest, RandInsertUpdateGet1e1) {
  rand_insert_update_get(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertUpdateGet1e2) {
  rand_insert_update_get(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertUpdateGet1e3) {
  rand_insert_update_get(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertUpdateGet1e4) {
  rand_insert_update_get(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertUpdateGet1e5) {
  rand_insert_update_get(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertUpdateGet1e6) {
  rand_insert_update_get(test_name(), 6);
}

TEST(BPlusTreeTest, Delete1) {
  std::string name = test_name();
  auto pgm = wing::PageManager::Create(name, MAX_BUF_PAGES);
  auto tree = tree_t::Create(*pgm);
  ASSERT_TRUE(tree.IsEmpty());
  ASSERT_EQ(tree.TupleNum(), 0);
  ASSERT_FALSE(tree.Delete("123"));
  ASSERT_EQ(tree.TupleNum(), 0);
  ASSERT_TRUE(tree.Insert("123", "456"));
  ASSERT_FALSE(tree.IsEmpty());
  ASSERT_EQ(tree.TupleNum(), 1);
  ASSERT_TRUE(tree.Delete("123"));
  ASSERT_EQ(tree.TupleNum(), 0);
  ASSERT_FALSE(tree.Delete("123"));
  ASSERT_TRUE(tree.IsEmpty());
  ASSERT_EQ(tree.TupleNum(), 0);
  ASSERT_TRUE(fs::remove(name));
}

static void rand_insert_get_take_nearby(const std::filesystem::path& path,
    size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .get = n,
    .take_nearby = n,
  };
  ASSERT_NO_FATAL_FAILURE(test_rand_op(path, num, magnitude, magnitude));
}
TEST(BPlusTreeTest, RandInsertGetDelete1e1) {
  rand_insert_get_take_nearby(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertGetDelete1e2) {
  rand_insert_get_take_nearby(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertGetDelete1e3) {
  rand_insert_get_take_nearby(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertGetDelete1e4) {
  rand_insert_get_take_nearby(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertGetDelete1e5) {
  rand_insert_get_take_nearby(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertGetDelete1e6) {
  rand_insert_get_take_nearby(test_name(), 6);
}

static void rand_insert_get_take(const std::filesystem::path& path,
    size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .get = n,
    .take = n,
  };
  ASSERT_NO_FATAL_FAILURE(test_rand_op(path, num, magnitude, magnitude));
}
TEST(BPlusTreeTest, RandInsertGetTake1e1) {
  rand_insert_get_take(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertGetTake1e2) {
  rand_insert_get_take(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertGetTake1e3) {
  rand_insert_get_take(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertGetTake1e4) {
  rand_insert_get_take(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertGetTake1e5) {
  rand_insert_get_take(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertGetTake1e6) {
  rand_insert_get_take(test_name(), 6);
}

static void rand_insert_delete_all(const std::filesystem::path& path,
    size_t magnitude) {
  std::minstd_rand e(233);
  {
    auto [pgm, tree, m] = create_rand_insert(path, e, magnitude);
    if (::testing::Test::HasFatalFailure())
      return;
    for (auto& kv : m)
      ASSERT_TRUE(tree.Delete(kv.first));
    ASSERT_TRUE(tree.IsEmpty());
  }
  ASSERT_TRUE(fs::remove(path));
}
TEST(BPlusTreeTest, RandInsertDeleteAll1e1) {
  rand_insert_delete_all(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertDeleteAll1e2) {
  rand_insert_delete_all(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertDeleteAll1e3) {
  rand_insert_delete_all(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertDeleteAll1e4) {
  rand_insert_delete_all(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertDeleteAll1e5) {
  rand_insert_delete_all(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertDeleteAll1e6) {
  rand_insert_delete_all(test_name(), 6);
}

TEST(BPlusTreeTest, Scan1) {
  std::string name = test_name();
  {
    auto pgm = wing::PageManager::Create(name, MAX_BUF_PAGES);
    auto tree = wing::BPlusTree<std::compare_three_way>::Create(*pgm);
    auto it = tree.Begin();
    ASSERT_FALSE(it.Cur().has_value());

    ASSERT_TRUE(tree.Insert("123", "456"));
    it = tree.Begin();
    ASSERT_EQ(it.Cur(),
      std::make_pair(std::string_view("123"), std::string_view("456")));
    it.Next();
    ASSERT_FALSE(it.Cur().has_value());

    ASSERT_TRUE(tree.Insert("233", "332"));
    it = tree.Begin();
    ASSERT_EQ(it.Cur(),
      std::make_pair(std::string_view("123"), std::string_view("456")));
    it.Next();
    ASSERT_EQ(it.Cur(),
      std::make_pair(std::string_view("233"), std::string_view("332")));
    it.Next();
    ASSERT_FALSE(it.Cur().has_value());
  }
  ASSERT_TRUE(fs::remove_all(name) > 0);
}

void scan_all(wing::BPlusTree<std::compare_three_way>& tree, map_t& m) {
  auto it = tree.Begin();
  auto std_it = m.begin();
  while (true) {
    auto ret = it.Cur();
    if (!ret.has_value())
      break;
    auto kv = ret.value();
    ASSERT_EQ(kv.first, std_it->first);
    ASSERT_EQ(kv.second, std_it->second);
    it.Next();
    ++std_it;
  }
  ASSERT_EQ(std_it, m.end());
}
static void rand_insert_scan_all(const std::filesystem::path& path,
    size_t magnitude) {
  std::minstd_rand e(233);
  {
    auto [pgm, tree, m] = create_rand_insert(path, e, magnitude);
    if (::testing::Test::HasFatalFailure())
      return;
    scan_all(tree, m);
  }
  ASSERT_TRUE(fs::remove(path));
}
TEST(BPlusTreeTest, RandInsertScanAll1e1) {
  rand_insert_scan_all(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertScanAll1e2) {
  rand_insert_scan_all(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertScanAll1e3) {
  rand_insert_scan_all(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertScanAll1e4) {
  rand_insert_scan_all(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertScanAll1e5) {
  rand_insert_scan_all(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertScanAll1e6) {
  rand_insert_scan_all(test_name(), 6);
}

static void seq_insert_scan_all(const fs::path& path, size_t magnitude,
    size_t val_len) {
  std::minstd_rand e(233);
  {
    auto [pgm, tree, m] = create_seq_insert(path, e, magnitude, val_len);
    if (::testing::Test::HasFatalFailure())
      return;
    scan_all(tree, m);
  }
  ASSERT_TRUE(fs::remove(path));
}
static void seq_insert_scan_all_value_16B(const fs::path& path,
    size_t magnitude) {
  seq_insert_scan_all(path, magnitude, 16);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue16B1e1) {
  seq_insert_scan_all_value_16B(test_name(), 1);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue16B1e2) {
  seq_insert_scan_all_value_16B(test_name(), 2);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue16B1e3) {
  seq_insert_scan_all_value_16B(test_name(), 3);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue16B1e4) {
  seq_insert_scan_all_value_16B(test_name(), 4);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue16B1e5) {
  seq_insert_scan_all_value_16B(test_name(), 5);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue16B1e6) {
  seq_insert_scan_all_value_16B(test_name(), 6);
}

static void seq_insert_scan_all_value_1024B(const fs::path& path,
    size_t magnitude) {
  seq_insert_scan_all(path, magnitude, 1024);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue1024B1e1) {
  seq_insert_scan_all_value_1024B(test_name(), 1);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue1024B1e2) {
  seq_insert_scan_all_value_1024B(test_name(), 2);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue1024B1e3) {
  seq_insert_scan_all_value_1024B(test_name(), 3);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue1024B1e4) {
  seq_insert_scan_all_value_1024B(test_name(), 4);
}
TEST(BPlusTreeTest, SeqInsertScanAllValue1024B1e5) {
  seq_insert_scan_all_value_1024B(test_name(), 5);
}

static void seq_insert_rand_len_scan_all(const fs::path& path, size_t magnitude,
    size_t max_val_len) {
  std::minstd_rand e(233);
  {
    auto [pgm, tree, m] =
      create_seq_insert_rand_len(path, e, magnitude, max_val_len);
    if (::testing::Test::HasFatalFailure())
      return;
    scan_all(tree, m);
  }
  ASSERT_TRUE(fs::remove(path));
}
static void seq_insert_scan_all_value_max_1024B(const fs::path& path, size_t magnitude) {
  seq_insert_rand_len_scan_all(path, magnitude, 1024);
}
TEST(BPlusTreeTest, SeqInsertScanAllValueMax1024B1e1) {
  seq_insert_scan_all_value_max_1024B(test_name(), 1);
}
TEST(BPlusTreeTest, SeqInsertScanAllValueMax1024B1e2) {
  seq_insert_scan_all_value_max_1024B(test_name(), 2);
}
TEST(BPlusTreeTest, SeqInsertScanAllValueMax1024B1e3) {
  seq_insert_scan_all_value_max_1024B(test_name(), 3);
}
TEST(BPlusTreeTest, SeqInsertScanAllValueMax1024B1e4) {
  seq_insert_scan_all_value_max_1024B(test_name(), 4);
}
TEST(BPlusTreeTest, SeqInsertScanAllValueMax1024B1e5) {
  seq_insert_scan_all_value_max_1024B(test_name(), 5);
}

TEST(BPlusTreeTest, LowerBound1) {
  std::string name = test_name();
  auto pgm = wing::PageManager::Create(name, MAX_BUF_PAGES);
  auto tree = wing::BPlusTree<std::compare_three_way>::Create(*pgm);
  auto it = tree.LowerBound("123");
  ASSERT_FALSE(it.Cur().has_value());

  ASSERT_TRUE(tree.Insert("123", "456"));
  it = tree.LowerBound("123");
  ASSERT_EQ(it.Cur(),
    std::make_pair(std::string_view("123"), std::string_view("456")));
  it.Next();
  ASSERT_FALSE(it.Cur().has_value());

  ASSERT_TRUE(tree.Insert("234", "567"));
  it = tree.LowerBound("200");
  ASSERT_EQ(it.Cur(),
    std::make_pair(std::string_view("234"), std::string_view("567")));
  it.Next();
  ASSERT_FALSE(it.Cur().has_value());

  it = tree.LowerBound("300");
  ASSERT_FALSE(it.Cur().has_value());

  ASSERT_TRUE(fs::remove(name));
}

static void rand_insert_lower_bound(const std::filesystem::path& path,
    size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .lower_bound = n,
  };
  ASSERT_NO_FATAL_FAILURE(test_rand_op(path, num, magnitude, magnitude));
}
TEST(BPlusTreeTest, RandInsertLowerBound1e1) {
  rand_insert_lower_bound(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertLowerBound1e2) {
  rand_insert_lower_bound(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertLowerBound1e3) {
  rand_insert_lower_bound(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertLowerBound1e4) {
  rand_insert_lower_bound(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertLowerBound1e5) {
  rand_insert_lower_bound(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertLowerBound1e6) {
  rand_insert_lower_bound(test_name(), 6);
}

TEST(BPlusTreeTest, UpperBound1) {
  std::string name = test_name();
  {
    auto pgm = wing::PageManager::Create(name, MAX_BUF_PAGES);
    auto tree = wing::BPlusTree<std::compare_three_way>::Create(*pgm);
    auto it = tree.UpperBound("123");
    ASSERT_FALSE(it.Cur().has_value());

    ASSERT_TRUE(tree.Insert("123", "456"));
    it = tree.UpperBound("123");
    ASSERT_FALSE(it.Cur().has_value());
    it = tree.UpperBound("122");
    ASSERT_EQ(it.Cur(),
      std::make_pair(std::string_view("123"), std::string_view("456")));
    it.Next();
    ASSERT_FALSE(it.Cur().has_value());

    ASSERT_TRUE(tree.Insert("234", "567"));
    it = tree.UpperBound("123");
    ASSERT_EQ(it.Cur(),
      std::make_pair(std::string_view("234"), std::string_view("567")));
    it.Next();
    ASSERT_FALSE(it.Cur().has_value());

    it = tree.UpperBound("234");
    ASSERT_FALSE(it.Cur().has_value());
  }
  ASSERT_TRUE(fs::remove(name));
}

static void rand_insert_upper_bound(const std::filesystem::path& path,
    size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .upper_bound = n,
  };
  ASSERT_NO_FATAL_FAILURE(test_rand_op(path, num, magnitude, magnitude));
}
TEST(BPlusTreeTest, RandInsertUpperBound1e1) {
  rand_insert_upper_bound(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertUpperBound1e2) {
  rand_insert_upper_bound(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertUpperBound1e3) {
  rand_insert_upper_bound(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertUpperBound1e4) {
  rand_insert_upper_bound(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertUpperBound1e5) {
  rand_insert_upper_bound(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertUpperBound1e6) {
  rand_insert_upper_bound(test_name(), 6);
}

static void rand_insert_scan(const std::filesystem::path& path,
    size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .scan = n,
  };
  ASSERT_NO_FATAL_FAILURE(test_rand_op(path, num, magnitude, magnitude));
}
TEST(BPlusTreeTest, RandInsertScan1e1) {
  rand_insert_scan(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertScan1e2) {
  rand_insert_scan(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertScan1e3) {
  rand_insert_scan(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertScan1e4) {
  rand_insert_scan(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertScan1e5) {
  rand_insert_scan(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertScan1e6) {
  rand_insert_scan(test_name(), 6);
}

static void rand_all_operations(
    const std::filesystem::path& path, size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .update = n,
    .get = n,
    .take_nearby = n,
    .lower_bound = n,
    .upper_bound = n,
    .scan = n,
  };
  {
    std::minstd_rand e(233);
    map_t m;
    auto pgm = wing::PageManager::Create(path, MAX_BUF_PAGES);
    auto tree = tree_t::Create(*pgm);
    Env env {
      .e = e,
      .tree = tree,
      .m = m,
      .max_key_len = magnitude,
      .max_val_len = magnitude,
    };
    ASSERT_NO_FATAL_FAILURE(rand_op(env, num));
    tree.Destroy();
    pgm->ShrinkToFit();
    ASSERT_EQ(pgm->PageNum(), pgm->SuperPageID() + 1);
  }
  ASSERT_TRUE(fs::remove(path));
}
TEST(BPlusTreeTest, RandAllOperations1e1) {
  rand_all_operations(test_name(), 1);
}
TEST(BPlusTreeTest, RandAllOperations1e2) {
  rand_all_operations(test_name(), 2);
}
TEST(BPlusTreeTest, RandAllOperations1e3) {
  rand_all_operations(test_name(), 3);
}
TEST(BPlusTreeTest, RandAllOperations1e4) {
  rand_all_operations(test_name(), 4);
}
TEST(BPlusTreeTest, RandAllOperations1e5) {
  rand_all_operations(test_name(), 5);
}
TEST(BPlusTreeTest, RandAllOperations1e6) {
  rand_all_operations(test_name(), 6);
}

static void rand_all_operations_fixed(
    const std::filesystem::path& path, size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .update = n,
    .get = n,
    .take = n,
    .take_nearby = n / 10,
    .lower_bound = n,
    .upper_bound = n,
    .scan = n,
  };
  {
    std::minstd_rand e(233);
    map_t m;
    auto pgm = wing::PageManager::Create(path, MAX_BUF_PAGES);
    auto tree = tree_t::Create(*pgm);
    Env env {
      .e = e,
      .tree = tree,
      .m = m,
      .max_key_len = magnitude,
      .max_val_len = magnitude,
    };
    ASSERT_NO_FATAL_FAILURE(rand_op(env, num));
    tree.Destroy();
    pgm->ShrinkToFit();
    ASSERT_EQ(pgm->PageNum(), pgm->SuperPageID() + 1);
  }
  ASSERT_TRUE(fs::remove(path));
}
TEST(BPlusTreeTest, RandAllOperationsFixed1e1) {
  rand_all_operations_fixed(test_name(), 1);
}
TEST(BPlusTreeTest, RandAllOperationsFixed1e2) {
  rand_all_operations_fixed(test_name(), 2);
}
TEST(BPlusTreeTest, RandAllOperationsFixed1e3) {
  rand_all_operations_fixed(test_name(), 3);
}
TEST(BPlusTreeTest, RandAllOperationsFixed1e4) {
  rand_all_operations_fixed(test_name(), 4);
}
TEST(BPlusTreeTest, RandAllOperationsFixed1e5) {
  rand_all_operations_fixed(test_name(), 5);
}
TEST(BPlusTreeTest, RandAllOperationsFixed1e6) {
  rand_all_operations_fixed(test_name(), 6);
}

static void rand_all_operations_rand_len(
    const std::filesystem::path& path, size_t magnitude) {
  size_t n = pow<size_t>(10, magnitude);
  OPNum num{
    .insert = n,
    .update = n,
    .get = n,
    .take = n,
    .take_nearby = n / 10,
    .lower_bound = n,
    .upper_bound = n,
    .scan = n,
  };
  {
    std::minstd_rand e(233);
    map_t m;
    auto pgm = wing::PageManager::Create(path, MAX_BUF_PAGES);
    auto tree = tree_t::Create(*pgm);
    Env env {
      .e = e,
      .tree = tree,
      .m = m,
      .max_key_len = magnitude * 2,
      .max_val_len = 1024,
      .rand_len = true,
    };
    ASSERT_NO_FATAL_FAILURE(rand_op(env, num));
    tree.Destroy();
    pgm->ShrinkToFit();
    ASSERT_EQ(pgm->PageNum(), pgm->SuperPageID() + 1);
  }
  ASSERT_TRUE(fs::remove(path));
}
TEST(BPlusTreeTest, RandAllOperationsRandLen1e1) {
  rand_all_operations_rand_len(test_name(), 1);
}
TEST(BPlusTreeTest, RandAllOperationsRandLen1e2) {
  rand_all_operations_rand_len(test_name(), 2);
}
TEST(BPlusTreeTest, RandAllOperationsRandLen1e3) {
  rand_all_operations_rand_len(test_name(), 3);
}
TEST(BPlusTreeTest, RandAllOperationsRandLen1e4) {
  rand_all_operations_rand_len(test_name(), 4);
}
TEST(BPlusTreeTest, RandAllOperationsRandLen1e5) {
  rand_all_operations_rand_len(test_name(), 5);
}

static void rand_insert_close_open_scan(const std::filesystem::path& path,
    size_t magnitude) {
  std::minstd_rand e(233);
  std::map<std::string, std::string> m;
  {
    auto [pgm, tree, map] = create_rand_insert(path, e, magnitude);
    m = std::move(map);
  }
  {
    auto ret = Open(path);
    if (ret.index() == 1)
      FAIL() << std::get<1>(ret);
    auto [pgm, tree] = std::move(std::get<0>(ret));
    scan_all(tree, m);
  }
  ASSERT_TRUE(fs::remove(path));
}
TEST(BPlusTreeTest, RandInsertCloseOpenScan1e1) {
  rand_insert_close_open_scan(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertCloseOpenScan1e2) {
  rand_insert_close_open_scan(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertCloseOpenScan1e3) {
  rand_insert_close_open_scan(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertCloseOpenScan1e4) {
  rand_insert_close_open_scan(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertCloseOpenScan1e5) {
  rand_insert_close_open_scan(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertCloseOpenScan1e6) {
  rand_insert_close_open_scan(test_name(), 6);
}

static void rand_insert_blob_close_open_scan_destroy(
    const std::filesystem::path& path, size_t key_len, size_t val_len,
    size_t insert_num, size_t seed) {
  std::minstd_rand e(seed);
  std::map<std::string, std::string> m;
  {
    auto [pgm, tree] = Create(path);
    while (insert_num--) {
      std::string key = rand_digits(e, key_len);
      std::string value = rand_digits(e, val_len);
      auto std_ret = m.insert({key, value});
      auto blob = wing::Blob::Create(*pgm);
      blob.Rewrite(value);
      wing::pgid_t page_id = blob.MetaPageID();
      std::string blob_value(reinterpret_cast<const char *>(&page_id),
        sizeof(page_id));
      bool succeed = tree.Insert(key, blob_value);
      ASSERT_EQ(succeed, std_ret.second);
      if (!succeed)
        blob.Destroy();
    }
  }
  {
    auto ret = Open(path);
    if (ret.index() == 1)
      FAIL() << std::get<1>(ret);
    auto [pgm, tree] = std::move(std::get<0>(ret));
    auto it = tree.Begin();
    auto std_it = m.begin();
    while (true) {
      auto ret = it.Cur();
      if (!ret.has_value())
        break;
      auto kv = ret.value();
      ASSERT_EQ(kv.first, std_it->first);
      ASSERT_EQ(kv.second.size(), sizeof(wing::pgid_t));
      auto page_id = *(wing::pgid_t *)kv.second.data();
      auto blob = wing::Blob::Open(*pgm, page_id);
      ASSERT_EQ(blob.Read(), std_it->second);
      blob.Destroy();
      it.Next();
      ++std_it;
    }
    ASSERT_EQ(std_it, m.end());
    tree.Destroy();
    pgm->ShrinkToFit();
    ASSERT_EQ(pgm->PageNum(), pgm->SuperPageID() + 1);
  }
  ASSERT_TRUE(fs::remove(path));
}

TEST(BPlusTreeTest, RandInsertBlobCloseOpenScanDestroy1e1) {
  rand_insert_blob_close_open_scan_destroy(test_name(), 1, 1, 10, 233);
}

TEST(BPlusTreeTest, RandInsertBlobCloseOpenScanDestroy1e2) {
  rand_insert_blob_close_open_scan_destroy(test_name(), 2, 2, 100, 233);
}

TEST(BPlusTreeTest, RandInsertBlobCloseOpenScanDestroy1e3) {
  rand_insert_blob_close_open_scan_destroy(test_name(), 3, 3, 1000, 233);
}

TEST(BPlusTreeTest, RandInsertBlobCloseOpenScanDestroy1e1Value23333) {
  rand_insert_blob_close_open_scan_destroy(test_name(), 1, 23333, 10, 233);
}

TEST(BPlusTreeTest, RandInsertBlobCloseOpenScanDestroy1e2Value23333) {
  rand_insert_blob_close_open_scan_destroy(test_name(), 2, 23333, 100, 233);
}

TEST(BPlusTreeTest, RandInsertBlobCloseOpenScanDestroy1e3Value23333) {
  rand_insert_blob_close_open_scan_destroy(test_name(), 3, 23333, 1000, 233);
}

static void rand_insert_destroy(const std::filesystem::path& path,
    size_t magnitude) {
  std::minstd_rand e(233);
  std::map<std::string, std::string> m;
  wing::pgid_t page_num;
  {
    auto [pgm, tree, map] = create_rand_insert(path, e, magnitude);
    m = std::move(map);
    page_num = pgm->PageNum();
    tree.Destroy();
  }
  {
    std::unique_ptr<wing::PageManager> pgm;
    ASSERT_NO_FATAL_FAILURE(match(wing::PageManager::Open(path, MAX_BUF_PAGES),
      [&pgm](std::unique_ptr<wing::PageManager>& pgm_ret) {
        pgm = std::move(pgm_ret);
      },
      [](wing::io::Error& err) {
        FAIL() << err;
      }
    ));
    ASSERT_EQ(pgm->PageNum(), page_num);
    pgm->ShrinkToFit();
    ASSERT_EQ(pgm->PageNum(), pgm->SuperPageID() + 1);
  }
  ASSERT_TRUE(fs::remove(path));
}
TEST(BPlusTreeTest, RandInsertDestroy1e1) {
  rand_insert_destroy(test_name(), 1);
}
TEST(BPlusTreeTest, RandInsertDestroy1e2) {
  rand_insert_destroy(test_name(), 2);
}
TEST(BPlusTreeTest, RandInsertDestroy1e3) {
  rand_insert_destroy(test_name(), 3);
}
TEST(BPlusTreeTest, RandInsertDestroy1e4) {
  rand_insert_destroy(test_name(), 4);
}
TEST(BPlusTreeTest, RandInsertDestroy1e5) {
  rand_insert_destroy(test_name(), 5);
}
TEST(BPlusTreeTest, RandInsertDestroy1e6) {
  rand_insert_destroy(test_name(), 6);
}
