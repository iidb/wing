#ifndef SAKURA_TEST_H__
#define SAKURA_TEST_H__

#include <fmt/core.h>

#include <functional>
#include <future>
#include <random>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "instance/instance.hpp"

#define SAKURA_USE_JIT_FLAG 0

namespace wing::wing_testing {
bool test_timeout(std::function<void()> function, size_t timeout_in_ms) {
  std::promise<bool> promisedFinished;
  auto futureResult = promisedFinished.get_future();
  std::thread([&]() {
    function();
    promisedFinished.set_value(true);
  }).detach();
  return futureResult.wait_for(std::chrono::milliseconds(timeout_in_ms)) !=
         std::future_status::timeout;
}
class Value {
 public:
  enum { INT = 0, FLOAT, STRING };
  virtual ~Value() = default;
  int64_t virtual ReadInt() const { return 0; }
  double virtual ReadFloat() const { return 0; }
  std::string_view virtual ReadString() const { return ""; }
  int virtual type() const { return 0; }
};
class IntValue : public Value {
 public:
  int64_t data{0};
  static std::unique_ptr<IntValue> Create(int64_t d) {
    auto ret = std::make_unique<IntValue>();
    ret->data = d;
    return ret;
  }
  int64_t ReadInt() const override { return data; }
  int type() const override { return Value::INT; }
};
class FloatValue : public Value {
 public:
  double data{0.0};
  static std::unique_ptr<FloatValue> Create(double d) {
    auto ret = std::make_unique<FloatValue>();
    ret->data = d;
    return ret;
  }
  double ReadFloat() const override { return data; }
  int type() const override { return Value::FLOAT; }
};
class StringValue : public Value {
 public:
  std::string data;
  static std::unique_ptr<StringValue> Create(std::string_view d) {
    auto ret = std::make_unique<StringValue>();
    ret->data = d;
    return ret;
  }
  std::string_view ReadString() const override { return data; }
  int type() const override { return Value::STRING; }
};

bool TestEQ(const ResultSet::ResultData& tuple, int id, const Value* v) {
  if (v->type() == Value::STRING) {
    auto ret = tuple.ReadString(id) ==
               static_cast<const StringValue*>(v)->ReadString();
    if (!ret) {
      DB_INFO("Wrong Answer comparing {}-th field: {}, {}", id + 1,
          tuple.ReadString(id),
          static_cast<const FloatValue*>(v)->ReadString());
    }
    return ret;
  } else if (v->type() == Value::INT) {
    auto ret = tuple.ReadInt(id) == static_cast<const IntValue*>(v)->ReadInt();
    if (!ret) {
      DB_INFO("Wrong Answer comparing {}-th field: {}, {}", id + 1,
          tuple.ReadInt(id), static_cast<const FloatValue*>(v)->ReadInt());
    }
    return ret;
  } else if (v->type() == Value::FLOAT) {
    //
    // DB_INFO("{}, {}",fabs(tuple.ReadFloat(id) - static_cast<const
    // FloatValue*>(v)->ReadFloat()), 1e-9 * fabs(static_cast<const
    // FloatValue*>(v)->ReadFloat()));
    auto ret = fabs(tuple.ReadFloat(id) -
                    static_cast<const FloatValue*>(v)->ReadFloat()) <=
               1e-7 * fabs(static_cast<const FloatValue*>(v)->ReadFloat());
    if (!ret) {
      DB_INFO("Wrong Answer comparing {}-th field: {}, {}", id + 1,
          tuple.ReadFloat(id), static_cast<const FloatValue*>(v)->ReadFloat());
    }
    return ret;
  } else {
    return false;
  }
}

namespace __detail {
template <typename T>
struct __gen_random_tuple_pair {};

template <>
struct __gen_random_tuple_pair<int> {
  template <int FLAG>
  static std::pair<std::string, std::unique_ptr<Value>> Generate(
      std::mt19937_64& gen,
      const std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>& range) {
    std::uniform_int_distribution<int> dis(
        static_cast<const IntValue*>(range.first.get())->data,
        static_cast<const IntValue*>(range.second.get())->data);
    int t = dis(gen);
    auto v = std::make_unique<IntValue>();
    v->data = t;
    return {fmt::format("{}", t), std::move(v)};
  }
};

template <>
struct __gen_random_tuple_pair<int64_t> {
  template <int FLAG>
  static std::pair<std::string, std::unique_ptr<Value>> Generate(
      std::mt19937_64& gen,
      const std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>& range) {
    std::uniform_int_distribution<int64_t> dis(
        static_cast<const IntValue*>(range.first.get())->data,
        static_cast<const IntValue*>(range.second.get())->data);
    int64_t t = dis(gen);
    if (!FLAG) {
      return {fmt::format("{}", t), nullptr};
    }
    auto v = std::make_unique<IntValue>();
    v->data = t;
    return {fmt::format("{}", t), std::move(v)};
  }
};
template <>
struct __gen_random_tuple_pair<double> {
  template <int FLAG>
  static std::pair<std::string, std::unique_ptr<Value>> Generate(
      std::mt19937_64& gen,
      const std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>& range) {
    std::uniform_real_distribution<double> dis(
        static_cast<const FloatValue*>(range.first.get())->data,
        static_cast<const FloatValue*>(range.second.get())->data);
    double t = dis(gen);
    auto s = fmt::format("{:.10f}", t);
    bool flag = false;
    for (auto& a : s)
      flag |= a == '.' || a == 'e' || a == 'E';
    if (!flag)
      s += ".0";
    if (!FLAG) {
      return {std::move(s), nullptr};
    }
    auto v = std::make_unique<FloatValue>();
    v->data = std::stod(s);
    return {std::move(s), std::move(v)};
  }
};
template <>
struct __gen_random_tuple_pair<std::string> {
  template <int FLAG>
  static std::pair<std::string, std::unique_ptr<Value>> Generate(
      std::mt19937_64& gen,
      const std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>& range) {
    std::uniform_int_distribution<> dis(
        static_cast<const IntValue*>(range.first.get())->data,
        static_cast<const IntValue*>(range.second.get())->data);
    int64_t len = dis(gen);
    std::uniform_int_distribution<> dis2(0, 51);
    std::string t;
    t.resize(len);
    for (auto& a : t) {
      auto x = dis2(gen);
      a = x < 26 ? x + 'a' : 'A' + (x - 26);
    }
    if (!FLAG) {
      return {fmt::format("\'{}\'", t), nullptr};
    }
    auto v = std::make_unique<StringValue>();
    v->data = std::move(t);
    return {fmt::format("\'{}\'", v->data), std::move(v)};
  }
};

template <typename... T>
struct __gen_random_tuple {
  template <int FLAG>
  static std::pair<std::string, std::vector<std::unique_ptr<Value>>> Generate(
      std::mt19937_64& gen,
      std::span<std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>>
          vec) {
    return fuck<FLAG, T...>(gen, vec);
  }

 private:
  template <int FLAG, typename UU, typename VV, typename... TT>
  static std::pair<std::string, std::vector<std::unique_ptr<Value>>> fuck(
      std::mt19937_64& gen,
      std::span<std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>>
          vec) {
    auto R = __gen_random_tuple<VV, TT...>::template Generate<FLAG>(
        gen, vec.subspan(1, vec.size() - 1));
    auto L = __gen_random_tuple_pair<UU>::template Generate<FLAG>(gen, vec[0]);
    L.first += ", " + R.first;
    if (FLAG)
      R.second.push_back(std::move(L.second));
    return std::make_pair(L.first, std::move(R.second));
  }

  template <int FLAG, typename UU>
  static std::pair<std::string, std::vector<std::unique_ptr<Value>>> fuck(
      std::mt19937_64& gen,
      std::span<std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>>
          vec) {
    auto L = __gen_random_tuple_pair<UU>::template Generate<FLAG>(gen, vec[0]);
    std::vector<std::unique_ptr<Value>> lvec;
    if (FLAG)
      lvec.push_back(std::move(L.second));
    return std::make_pair(L.first, std::move(lvec));
  }
};

template <typename, typename>
struct __gen_range_vec_pair {};

template <>
struct __gen_range_vec_pair<int, int> {
  static std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>> Generate(
      int x, int y) {
    return std::make_pair(IntValue::Create(x), IntValue::Create(y));
  }
};

template <>
struct __gen_range_vec_pair<int64_t, int64_t> {
  static std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>> Generate(
      int64_t x, int64_t y) {
    return std::make_pair(IntValue::Create(x), IntValue::Create(y));
  }
};

template <>
struct __gen_range_vec_pair<double, double> {
  static std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>> Generate(
      double x, double y) {
    return std::make_pair(FloatValue::Create(x), FloatValue::Create(y));
  }
};

template <typename... T>
struct __gen_range_vec {
  static std::vector<std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>>
  Generate(T... args) {
    return fuck(std::forward<T>(args)...);
  }

 private:
  template <typename UU, typename VV, typename WW, typename... TT>
  static std::vector<std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>>
  fuck(UU x, VV y, WW z, TT... args) {
    auto R = __gen_range_vec<WW, TT...>::Generate(z, args...);
    auto L = __gen_range_vec_pair<UU, VV>::Generate(x, y);
    R.push_back(std::move(L));
    return R;
  }

  template <typename UU, typename VV>
  static std::vector<std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>>
  fuck(UU x, VV y) {
    auto L = __gen_range_vec_pair<UU, VV>::Generate(x, y);
    std::vector<std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>> vec;
    vec.push_back(std::move(L));
    return vec;
  }
};

}  // namespace __detail

class ValueVector {
 public:
  ValueVector(
      size_t num_per_tuple, std::vector<std::unique_ptr<Value>>&& values)
    : num_per_tuple_(num_per_tuple), values_(std::move(values)) {}
  
  /* The id-th tuple, the i-th field. */
  std::unique_ptr<Value>& Get(size_t id, size_t i) {
    return values_[num_per_tuple_ * id + i];
  }
  
  /* Get the id-th tuple. */
  std::span<std::unique_ptr<Value>> GetTuple(size_t id) {
    return {values_.begin() + id * num_per_tuple_,
        values_.begin() + (id + 1) * num_per_tuple_};
  }

  /* Get all tuples. */
  std::vector<std::unique_ptr<Value>>& GetVector() { return values_; }
  const std::vector<std::unique_ptr<Value>>& GetVector() const {
    return values_;
  }

  /* Get the number of fields of tuples. */
  size_t GetTupleSize() { return num_per_tuple_; }

  /* Get the number of tuples. */
  size_t GetSize() { return values_.size() / num_per_tuple_; }

 private:
  size_t num_per_tuple_;
  std::vector<std::unique_ptr<Value>> values_;
};

/** Generate tuples of template types
 * Constructor requires the ranges of fields.
 * For numeric fields, the range is a closed interval of the number.
 * For string fields, the range is a closed interval of the length of string.
 * For example, for table (a int64, b float64, c varchar(1), d char(255), e
 * int32); We use RandomTuple<int64_t, double, std::string, std::string,
 * int32_t> tuple_gen (0, 10000, 0.0, 1.0, 0, 1, 0, 255, 1, 1);
 * */
template <typename... T>
class RandomTuple {
 public:
  template <typename... U>
  RandomTuple(size_t seed, U&&... args) : gen(seed) {
    vec_ = __detail::__gen_range_vec<U...>::Generate(std::forward<U>(args)...);
    std::reverse(vec_.begin(), vec_.end());
  }

  /**
   * Generate something like:
   * ret.first: "2, \"xxx\", 0.123456"
   * ret.second: {std::unique_ptr<IntValue>, std::unique_ptr<StringValue>,
   * std::unique_ptr<FloatValue>}
   */
  std::pair<std::string, std::vector<std::unique_ptr<Value>>> Generate() {
    auto ret = __detail::__gen_random_tuple<T...>::Generate<1>(gen, vec_);
    std::reverse(ret.second.begin(), ret.second.end());
    return ret;
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
    std::vector<std::unique_ptr<Value>> vecs;
    for (size_t i = 0; i < x; i++) {
      auto ret =
          __detail::__gen_random_tuple<T...>::template Generate<1>(gen, vec_);
      clause += "(";
      clause += ret.first;
      clause += i == x - 1 ? ")" : "),";
      vecs.insert(vecs.end(), std::make_move_iterator(ret.second.rbegin()),
          std::make_move_iterator(ret.second.rend()));
    }
    return {clause, ValueVector(vec_.size(), std::move(vecs))};
  }

  std::string GenerateValuesClauseStmt(size_t x) {
    std::string clause = "values";
    for (size_t i = 0; i < x; i++) {
      auto ret =
          __detail::__gen_random_tuple<T...>::template Generate<0>(gen, vec_);
      clause += "(";
      clause += ret.first;
      clause += i == x - 1 ? ")" : "),";
    }
    return clause;
  }

 private:
  std::mt19937_64 gen;
  std::vector<std::pair<std::unique_ptr<Value>, std::unique_ptr<Value>>> vec_;
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

using IV = IntValue;
using SV = StringValue;
using FV = FloatValue;
using PVec = std::vector<std::unique_ptr<Value>>;

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
    if (!TestEQ(tuple, i, ans[i].get()))
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
    if (!TestEQ(tuple, i, (*ans_ptr)[i].get()))
      return false;
  }
  return true;
}

bool CheckVecAns(const PVec& m, const ResultSet::ResultData& tuple, size_t sz) {
  for (size_t i = 0; i < sz; i++) {
    if (!TestEQ(tuple, i, m[i].get()))
      return false;
  }
  return true;
}

#define CHECK_ALL_ANS(answer, result, key_stmt, num_field)       \
  EXPECT_TRUE(result.Valid());                                   \
  for (uint32_t __i = 0; __i < answer.size(); __i++) {           \
    auto tuple = result.Next();                                  \
    EXPECT_TRUE(bool(tuple));                                    \
    if (!tuple)                                                  \
      break;                                                     \
    EXPECT_TRUE(CheckAns((key_stmt), answer, tuple, num_field)); \
  }                                                              \
  EXPECT_FALSE(result.Next());

#define CHECK_ALL_SORTED_ANS(answer, result, num_field)      \
  EXPECT_TRUE(result.Valid());                               \
  for (uint32_t __i = 0; __i < answer.size(); __i++) {       \
    auto tuple = result.Next();                              \
    EXPECT_TRUE(bool(tuple));                                \
    if (!tuple)                                              \
      break;                                                 \
    EXPECT_TRUE(CheckVecAns(answer[__i], tuple, num_field)); \
  }                                                          \
  EXPECT_FALSE(result.Next());

}  // namespace wing::wing_testing

#endif