#ifndef SAKURA_DBINFO_H__
#define SAKURA_DBINFO_H__

#include <string>
#include <vector>

#include "type/field.hpp"

namespace wing {

class CountMinSketch {
 public:
  const static uint32_t kDefaultHashCounts = 8;
  const static uint32_t kDefaultHashBuckets = 2027;
  CountMinSketch(size_t buckets, size_t funcs) : buckets_(buckets), funcs_(funcs), data_(buckets * funcs) {}
  CountMinSketch() : CountMinSketch(kDefaultHashBuckets, kDefaultHashCounts) {}
  double GetFreqCount(std::string_view data) const;
  void AddCount(std::string_view data, double value = 1.0);

 private:
  size_t buckets_, funcs_;
  std::vector<double> data_;
};

class HyperLL {
 public:
  const static size_t kDefaultRegCount = 1024;
  HyperLL(size_t reg_count) : data_(reg_count) {}
  HyperLL() : HyperLL(kDefaultRegCount) {}
  void Add(std::string_view data);
  double GetDistinctCounts() const;

 private:
  std::vector<uint8_t> data_;
  size_t N_{0};
};

class TableStatistics {
 public:
  TableStatistics(size_t tuple_num, std::vector<Field>&& max, std::vector<Field>&& min, std::vector<double>&& distinct_rate,
                  std::vector<CountMinSketch>&& freq)
      : tuple_num_(tuple_num), max_(std::move(max)), min_(std::move(min)), distinct_rate_(std::move(distinct_rate)), freq_(std::move(freq)) {}
  const Field& GetMax(int col) const { return max_[col]; }
  const Field& GetMin(int col) const { return min_[col]; }
  double GetDistinctRate(int col) const { return distinct_rate_[col]; }
  const std::vector<double>& GetDistinctRate() const { return distinct_rate_; }
  const CountMinSketch& GetCountMinSketch(int col) const { return freq_[col]; }
  size_t GetTupleNum() const { return tuple_num_; }

 private:
  size_t tuple_num_;
  std::vector<Field> max_;
  std::vector<Field> min_;
  std::vector<double> distinct_rate_;
  std::vector<CountMinSketch> freq_;
};

}  // namespace wing

#endif