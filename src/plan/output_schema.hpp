#ifndef SAKURA_OUTPUT_SCHEMA_H__
#define SAKURA_OUTPUT_SCHEMA_H__

#include <fmt/core.h>

#include <optional>
#include <string>
#include <vector>

#include "common/logging.hpp"
#include "type/field_type.hpp"

namespace wing {

class OutputColumnData {
 public:
  /* Id in the whole plan. */
  uint32_t id_;
  /* Table name. The original table name, or an alias.*/
  std::string table_name_;
  /* Column name. The original column name of the table, or an alias. */
  std::string column_name_;
  /* The type of the column. */
  FieldType type_;
  /* The size of the column. */
  uint32_t size_;
  /* Is the column unique. */
  bool is_unique_{false};
  /* Is the column sorted. */
  bool is_sort_{false};
  /* If the column is sorted, is it asc? */
  bool is_sort_asc{false};
  std::string ToString() const {
    return fmt::format(
        "{{ id:{}, table name: {}, column name: {}, type: {}, size: {} }}", id_,
        table_name_, column_name_, (uint32_t)type_, size_);
  }
};

/**
 * It represents the output of a PlanNode.
 */
class OutputSchema {
 public:
  OutputSchema() = default;
  OutputSchema(const std::vector<OutputColumnData>& cols, bool is_raw = false)
    : cols_(cols), is_raw_(is_raw) {}
  OutputSchema(std::vector<OutputColumnData>&& cols, bool is_raw = false)
    : cols_(std::move(cols)), is_raw_(is_raw) {}

  bool IsRaw() const { return is_raw_; }
  void SetRaw(bool is_raw) { is_raw_ = is_raw; }
  OutputColumnData& operator[](size_t index) { return cols_[index]; }
  const OutputColumnData& operator[](size_t index) const {
    return cols_[index];
  }
  std::vector<OutputColumnData>& GetCols() { return cols_; }
  const std::vector<OutputColumnData>& GetCols() const { return cols_; }
  /* Find the index of output column schema by id.*/
  std::optional<size_t> FindById(size_t id) const {
    for (size_t i = 0; i < cols_.size(); i++)
      if (cols_[i].id_ == id)
        return i;
    return {};
  }
  /* Append a column. */
  void Append(const OutputColumnData& column) { cols_.push_back(column); }
  void Append(OutputColumnData&& column) { cols_.push_back(std::move(column)); }
  void Append(const OutputSchema& R) {
    cols_.insert(cols_.end(), R.cols_.begin(), R.cols_.end());
  }
  void Append(OutputSchema&& R) {
    cols_.insert(cols_.end(), std::make_move_iterator(R.cols_.begin()),
        std::make_move_iterator(R.cols_.end()));
  }
  size_t Size() const { return cols_.size(); }

  /* Concatenate two OutputColumnSchema. */
  static OutputSchema Concat(auto&& L, auto&& R) {
    OutputSchema ret(std::forward<decltype(L)>(L));
    ret.Append(std::forward<decltype(R)>(R));
    ret.SetRaw(false);
    return ret;
  }

 private:
  std::vector<OutputColumnData> cols_;
  bool is_raw_{false};
};

}  // namespace wing

#endif