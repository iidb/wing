#pragma once

#include <fmt/core.h>

#include <functional>
#include <future>
#include <random>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "instance/instance.hpp"

static wing::WingOptions WingTestOptions() {
  wing::WingOptions ret;
  ret.exec_options.style = "vec";
  return ret;
}

static wing::WingOptions wing_test_options = WingTestOptions();

namespace wing::wing_testing {

void TestTimeout(std::function<void()> function, size_t timeout_in_ms,
    std::string_view failure_message) {
  std::promise<bool> promisedFinished;
  auto futureResult = promisedFinished.get_future();
  std::thread t([&]() {
    function();
    promisedFinished.set_value(true);
  });
  if (futureResult.wait_for(std::chrono::milliseconds(timeout_in_ms)) ==
      std::future_status::timeout) {
    DB_INFO(
        "Failure\nTime Limit Exceeds ({} ms): "
        "{}",
        timeout_in_ms, failure_message);
    std::abort();
  }
  t.join();
}

#define ABORT_ON_TIMEOUT(...) TestTimeout(__VA_ARGS__, #__VA_ARGS__)

class Value {
 public:
  enum class Type { INT = 0, FLOAT, STRING };
  Value() { type_ = Type::INT; }
  Value(const Value& v) {
    type_ = v.type_;
    if (type_ == Type::STRING) {
      new (&str_data) std::string(v.str_data);
    } else {
      int_data = v.int_data;
    }
  }

  Value(Value&& v) {
    type_ = v.type_;
    if (type_ == Type::STRING) {
      new (&str_data) std::string(std::move(v.str_data));
    } else {
      int_data = v.int_data;
    }
  }
  ~Value() {
    if (type_ == Type::STRING) {
      str_data.std::string::~string();
    }
  }
  static Value CreateInt(int64_t x) {
    Value ret;
    ret.int_data = x;
    ret.type_ = Type::INT;
    return ret;
  }
  static Value CreateFloat(double x) {
    Value ret;
    ret.double_data = x;
    ret.type_ = Type::FLOAT;
    return ret;
  }
  static Value CreateString(std::string_view x) {
    Value ret;
    new (&ret.str_data) std::string(x);
    ret.type_ = Type::STRING;
    return ret;
  }

  std::string ToString() const {
    if (type_ == Type::INT) {
      return fmt::format("{}", int_data);
    } else if (type_ == Type::FLOAT) {
      return fmt::format("{}", double_data);
    } else if (type_ == Type::STRING) {
      return str_data;
    }
    DB_ERR("Error! Type: {}", size_t(type_));
  }

  int64_t ReadInt() const { return int_data; }
  double ReadFloat() const { return double_data; }
  std::string_view ReadString() const {
    if (type_ != Type::STRING) {
      DB_ERR("Error! Type: {}", size_t(type_));
    }
    return str_data;
  }
  Type type() const { return type_; }

 private:
  Type type_{Type::INT};
  union {
    std::string str_data;
    int64_t int_data;
    double double_data;
  };
};

bool TestEQ(const ResultSet::ResultData& tuple, int id, const Value v) {
  if (v.type() == Value::Type::STRING) {
    auto ret = tuple.ReadString(id) == v.ReadString();
    if (!ret) {
      DB_INFO("Wrong Answer comparing {}-th field: {}, {}", id + 1,
          tuple.ReadString(id), v.ReadString());
    }
    return ret;
  } else if (v.type() == Value::Type::INT) {
    auto ret = tuple.ReadInt(id) == v.ReadInt();
    if (!ret) {
      DB_INFO("Wrong Answer comparing {}-th field: {}, {}", id + 1,
          tuple.ReadInt(id), v.ReadInt());
    }
    return ret;
  } else if (v.type() == Value::Type::FLOAT) {
    auto ret =
        fabs(tuple.ReadFloat(id) - v.ReadFloat()) <= 1e-7 * fabs(v.ReadFloat());
    if (!ret) {
      DB_INFO("Wrong Answer comparing {}-th field: {}, {}", id + 1,
          tuple.ReadFloat(id), v.ReadFloat());
    }
    return ret;
  } else {
    return false;
  }
}

class ValueVector {
 public:
  ValueVector(size_t num_per_tuple, std::vector<Value>&& values)
    : num_per_tuple_(num_per_tuple), values_(std::move(values)) {}

  /* The id-th tuple, the i-th field. */
  Value& Get(size_t id, size_t i) { return values_[num_per_tuple_ * id + i]; }

  /* Get the id-th tuple. */
  std::span<Value> GetTuple(size_t id) {
    return {values_.begin() + id * num_per_tuple_,
        values_.begin() + (id + 1) * num_per_tuple_};
  }

  /* Get all tuples. */
  std::vector<Value>& GetVector() { return values_; }
  const std::vector<Value>& GetVector() const { return values_; }

  /* Get the number of fields of tuples. */
  size_t GetTupleSize() { return num_per_tuple_; }

  /* Get the number of tuples. */
  size_t GetSize() { return values_.size() / num_per_tuple_; }

 private:
  size_t num_per_tuple_;
  std::vector<Value> values_;
};

/** Generate tuples of template types
 * Constructor requires the ranges of fields.
 * For numeric fields, the range is a closed interval of the number.
 * For string fields, the range is a closed interval of the length of string.
 * */
class RandomTupleGen {
 public:
  RandomTupleGen(size_t seed) : rgen_(seed) {}

  struct GenParam {
    int64_t intL, intR;
    double floatL, floatR;
    int64_t strlenL, strlenR;
    Value::Type type;

    Value Gen(std::mt19937_64& rgen) const {
      if (type == Value::Type::INT) {
        std::uniform_int_distribution<int64_t> dis(intL, intR);
        return Value::CreateInt(dis(rgen));
      } else if (type == Value::Type::FLOAT) {
        std::uniform_real_distribution<double> dis(floatL, floatR);
        auto t = dis(rgen);
        t = floor(t * (1e9)) / (1e9);
        return Value::CreateFloat(t);
      } else {
        std::uniform_int_distribution<int64_t> dis(strlenL, strlenR);
        int64_t len = dis(rgen);
        std::uniform_int_distribution<> dis2(0, 51);
        std::string t;
        t.resize(len);
        for (auto& a : t) {
          auto x = dis2(rgen);
          a = x < 26 ? x + 'a' : 'A' + (x - 26);
        }
        return Value::CreateString(t);
      }
    }
  };

  RandomTupleGen& AddInt(int64_t L, int64_t R) {
    GenParam pa;
    pa.intL = L;
    pa.intR = R;
    pa.type = Value::Type::INT;
    params_.push_back(pa);
    return *this;
  }

  RandomTupleGen& AddInt() { return AddInt(INT64_MIN, INT64_MAX); }

  RandomTupleGen& AddInt32() { return AddInt(INT32_MIN, INT32_MAX); }

  RandomTupleGen& AddFloat(double L, double R) {
    GenParam pa;
    pa.floatL = L;
    pa.floatR = R;
    pa.type = Value::Type::FLOAT;
    params_.push_back(pa);
    return *this;
  }

  RandomTupleGen& AddFloat() { return AddFloat(-1e300, 1e300); }

  RandomTupleGen& AddString(int64_t L, int64_t R) {
    GenParam pa;
    pa.strlenL = L;
    pa.strlenR = R;
    pa.type = Value::Type::STRING;
    params_.push_back(pa);
    return *this;
  }

  /**
   * Generate something like:
   * ret.first: "2, \"xxx\", 0.123456"
   * ret.second: {std::unique_ptr<IntValue>, std::unique_ptr<StringValue>,
   * std::unique_ptr<FloatValue>}
   */
  std::pair<std::string, std::vector<Value>> Generate() {
    std::string str;
    std::vector<Value> v;
    for (auto& param : params_) {
      auto val = param.Gen(rgen_);
      if (!str.empty()) {
        str += ", ";
      }
      if (val.type() == Value::Type::STRING) {
        str += "\"";
      }
      str += val.ToString();
      if (val.type() == Value::Type::STRING) {
        str += "\"";
      }
      v.push_back(val);
    }
    return {str, std::move(v)};
  }

  /**
   * Generate something like:
   * GenerateValuesClause(3)
   * ret.first: "values(2,3.0),(4,5.0),(5,6.234)"
   * ret.second: {std::unique_ptr<IntValue>, std::unique_ptr<FloatValue>,
   * std::unique_ptr<IntValue>,
   * std::unique_ptr<FloatValue>,std::unique_ptr<IntValue>,
   * std::unique_ptr<FloatValue>}
   */
  std::pair<std::string, ValueVector> GenerateValuesClause(size_t x) {
    std::string clause = "values";
    std::vector<Value> v;
    for (size_t i = 0; i < x; i++) {
      clause += "(";
      bool flag = false;
      for (auto& param : params_) {
        auto val = param.Gen(rgen_);
        if (flag) {
          clause += ", ";
        }
        if (val.type() == Value::Type::STRING) {
          clause += "\'";
        }
        clause += val.ToString();
        if (val.type() == Value::Type::STRING) {
          clause += "\'";
        }
        v.push_back(val);
        flag = true;
      }
      clause += ")";
      if (i != x - 1) {
        clause += ", ";
      }
    }

    return {clause, ValueVector(params_.size(), std::move(v))};
  }

  std::string GenerateValuesClauseStmt(size_t x) {
    std::string clause = "values";
    for (size_t i = 0; i < x; i++) {
      clause += "(";
      bool flag = false;
      for (auto& param : params_) {
        auto val = param.Gen(rgen_);
        if (flag) {
          clause += ", ";
        }
        if (val.type() == Value::Type::STRING) {
          clause += "\'";
        }
        clause += val.ToString();
        if (val.type() == Value::Type::STRING) {
          clause += "\'";
        }
        flag = true;
      }
      clause += ")";
      if (i != x - 1) {
        clause += ", ";
      }
    }
    return clause;
  }

 private:
  std::mt19937_64 rgen_;
  std::vector<GenParam> params_;
};

template <typename K, typename V>
class SortedVec {
 public:
  template <typename T, typename U>
  void Append(T&& k, U&& v) {
    vec_.push_back(std::make_pair(std::forward<T>(k), values_.size()));
    values_.push_back(std::forward<U>(v));
  }
  void Sort() {
    std::sort(vec_.begin(), vec_.end(),
        [](auto& x, auto& y) { return x.first < y.first; });
  }
  template <typename T>
  V* find(T&& k) {
    auto it = std::lower_bound(vec_.begin(), vec_.end(), std::make_pair(k, 0),
        [](auto& x, auto& y) { return x.first < y.first; });
    if (it == vec_.end() || it->first != k)
      return nullptr;
    return &values_[it->second];
  }
  size_t size() { return vec_.size(); }

 private:
  std::vector<std::pair<K, uint32_t>> vec_;
  std::vector<V> values_;
};

using PVec = std::vector<Value>;

template <typename T>
using AnsMap = std::map<T, PVec, std::less<>>;

template <typename T>
using HashAnsMap = std::unordered_map<T, PVec>;

template <typename T>
using SortAnsMap = SortedVec<T, PVec>;

using AnsVec = std::vector<PVec>;

template <typename... T>
PVec MkVec(T&&... args) {
  PVec ret;
  (ret.push_back(std::forward<T>(args)), ...);
  return ret;
}

template <typename T, typename U>
concept IsStdMap = requires(T m, U key) {
  m.find(key);
  m.find(key) == m.end();
  m.find(key)->second;
};

template <typename U, typename T>
bool CheckAns(U&& key, T&& m, const ResultSet::ResultData& tuple,
    size_t sz) requires IsStdMap<T, U> {
  auto it = m.find(key);
  if (it == m.end()) {
    DB_INFO("{}", key);
    return 0;
  }
  auto& ans = it->second;
  for (size_t i = 0; i < sz; i++) {
    if (!TestEQ(tuple, i, ans[i]))
      return false;
  }
  return true;
}

template <typename U, typename T>
bool CheckAns(
    U&& key, SortAnsMap<T>& m, const ResultSet::ResultData& tuple, size_t sz) {
  auto ans_ptr = m.find(std::forward<U>(key));
  if (!ans_ptr)
    return false;
  for (size_t i = 0; i < sz; i++) {
    if (!TestEQ(tuple, i, (*ans_ptr)[i]))
      return false;
  }
  return true;
}

bool CheckVecAns(const PVec& m, const ResultSet::ResultData& tuple, size_t sz) {
  for (size_t i = 0; i < sz; i++) {
    if (!TestEQ(tuple, i, m[i]))
      return false;
  }
  return true;
}

#define CHECK_ALL_ANS(answer, result, key_stmt, num_field)       \
  ASSERT_TRUE(result.Valid());                                   \
  for (uint32_t __i = 0; __i < answer.size(); __i++) {           \
    auto tuple = result.Next();                                  \
    ASSERT_TRUE(bool(tuple));                                    \
    if (!tuple)                                                  \
      break;                                                     \
    ASSERT_TRUE(CheckAns((key_stmt), answer, tuple, num_field)); \
  }                                                              \
  ASSERT_FALSE(result.Next());

#define CHECK_ALL_SORTED_ANS(answer, result, num_field)      \
  ASSERT_TRUE(result.Valid());                               \
  for (uint32_t __i = 0; __i < answer.size(); __i++) {       \
    auto tuple = result.Next();                              \
    ASSERT_TRUE(bool(tuple));                                \
    if (!tuple)                                              \
      break;                                                 \
    ASSERT_TRUE(CheckVecAns(answer[__i], tuple, num_field)); \
  }                                                          \
  ASSERT_FALSE(result.Next());

struct CompressedKVPair {
  std::string key_;
  uint32_t vlen_;

  CompressedKVPair(std::string key, uint32_t vlen)
    : key_(std::move(key)), vlen_(vlen) {}

  std::string value() const {
    int klen = key_.size();
    std::string value(vlen_, 0);
    for (uint32_t j = 0; j < vlen_; j++)
      value[j] = key_[j % klen];
    return value;
  }

  const std::string& key() const { return key_; }

  std::strong_ordering operator<=>(const CompressedKVPair& p) const {
    return key_ <=> p.key_;
  }
};

std::vector<CompressedKVPair> GenKVData(
    size_t seed, size_t n, size_t keylen, size_t valuelen) {
  std::mt19937_64 rgen(seed);
  std::vector<CompressedKVPair> ret;
  for (uint32_t i = 0; i < n; i++) {
    std::string key(keylen, 0);
    for (uint32_t j = 0; j < keylen; j++) {
      int x = rgen() % 52;
      key[j] = x < 26 ? x + 'a' : x - 26 + 'A';
    }
    ret.emplace_back(std::move(key), valuelen);
  }
  return ret;
}

std::vector<CompressedKVPair> GenKVDataWithRandomLen(size_t seed, size_t n,
    std::pair<size_t, size_t> klen_range,
    std::pair<size_t, size_t> vlen_range) {
  std::mt19937_64 rgen(seed);
  std::vector<CompressedKVPair> ret;
  for (uint32_t i = 0; i < n; i++) {
    std::uniform_int_distribution<> dis1(klen_range.first, klen_range.second);
    uint32_t keylen = dis1(rgen);
    std::string key(keylen, 0);
    for (uint32_t j = 0; j < keylen; j++) {
      int x = rgen() % 52;
      key[j] = x < 26 ? x + 'a' : x - 26 + 'A';
    }
    std::uniform_int_distribution<> dis2(vlen_range.first, vlen_range.second);
    uint32_t valuelen = dis2(rgen);
    ret.emplace_back(std::move(key), valuelen);
  }
  return ret;
}

}  // namespace wing::wing_testing
