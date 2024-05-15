#pragma once

#include <span>

#include "common/bitvector.hpp"
#include "type/field_type.hpp"
#include "type/static_field.hpp"
#include "type/vector.hpp"

namespace wing {

class TupleBatch {
 public:
  TupleBatch() = default;

  TupleBatch(const TupleBatch& t);

  TupleBatch(TupleBatch&& t);

  TupleBatch& operator=(const TupleBatch& t);

  TupleBatch& operator=(TupleBatch&& t);

  /* Create a TupleBatch using logical types of columns and the capacity of
   * vectors. */
  void Init(const std::vector<LogicalType>& types, size_t capacity);

  /**
   * Create a TupleBatch using vectors and a selection vector.
   * Vectors should have the same number of elements.
   * */
  void Init(std::span<Vector> vectors, size_t count, BitVector sel);

  /* Create TupleBatch from input. It only loads valid values (1 in sel_). */
  void Init(const TupleBatch& input);

  /* Set the value of the column col_idx of tuple tuple_idx to val. */
  void Set(size_t tuple_idx, size_t col_idx, StaticFieldRef val);

  /* Set the tuple_idx-th tuple */
  void Set(size_t tuple_idx, std::span<const StaticFieldRef> tuple);

  /* Append a tuple */
  void Append(std::span<const StaticFieldRef> tuple);

  /* Append a tuple, but read tuple data from column vectors. */
  void Append(std::span<Vector> vectors, size_t tuple_idx);

  /* Check if the tuple tuple_idx is valid. */
  inline bool IsValid(size_t tuple_idx) const {
    return tuple_idx < num_tuple_ && sel_[tuple_idx];
  }

  inline void SetValid(size_t tuple_idx, bool x) {
    num_valid_tuple_ -= sel_[tuple_idx] ? 1 : 0;
    num_valid_tuple_ += x ? 1 : 0;
    sel_[tuple_idx] = x;
  }

  /* Get the value of the column col_idx of tuple tuple_idx. */
  inline StaticFieldRef Get(size_t tuple_idx, size_t col_idx) const {
    return cols_[col_idx].Get(tuple_idx);
  }

  /* The amount of tuples. (maybe invalid) */
  inline size_t size() const { return num_tuple_; }

  /* The amount of valid tuples. */
  inline size_t ValidSize() const { return num_valid_tuple_; }

  /* the capacity of the tuple batch. (The maximum amount of tuples)*/
  inline size_t Capacity() const { return capacity_; }

  inline bool IsFull() const { return size() == Capacity(); }

  std::vector<Vector>& GetCols() { return cols_; }

  const std::vector<Vector>& GetCols() const { return cols_; }

  const BitVector& GetSelVector() const { return sel_; }

  void SetSelVector(const BitVector& sel) { sel_ = sel; }

  void SetSize(size_t num_tuple) { num_tuple_ = num_tuple; }

  /* shuffle the tuples. */
  void Shuffle(std::span<size_t> shuffle);

  /* Create a slice */
  TupleBatch Slice(size_t begin, size_t count);

  std::vector<LogicalType> GetColElemTypes() const;

  void Clear();

  /* C++-style iterator */
  class SingleTuple {
   public:
    SingleTuple(std::span<const Vector> data, size_t tuple_idx)
      : data_(data), tuple_idx_(tuple_idx) {}

    StaticFieldRef operator[](const size_t col_idx) {
      return data_[col_idx].Get(tuple_idx_);
    }

    size_t size() const { return data_.size(); }

    LogicalType GetElemType(size_t i) const { return data_[i].GetElemType(); }

    SingleTuple SubTuple(size_t begin, size_t count) const {
      return SingleTuple(data_.subspan(begin, count), tuple_idx_);
    }

   private:
    std::span<const Vector> data_;
    size_t tuple_idx_;
  };
  class Iterator {
   public:
    Iterator(TupleBatch& data, size_t tuple_idx)
      : data_(data), tuple_idx_(tuple_idx) {
      SkipInvalid();
    }

    SingleTuple operator*() { return SingleTuple(data_.cols_, tuple_idx_); }

    Iterator& operator++() {
      tuple_idx_ += 1;
      SkipInvalid();
      return *this;
    }

    bool operator==(const Iterator& it) const {
      return tuple_idx_ == it.tuple_idx_;
    }

   private:
    void SkipInvalid() {
      while (tuple_idx_ < data_.size() && !data_.IsValid(tuple_idx_)) {
        tuple_idx_ += 1;
      }
    }
    TupleBatch& data_;
    size_t tuple_idx_;
  };

  Iterator begin() { return Iterator(*this, 0); }

  Iterator end() { return Iterator(*this, num_tuple_); }

  /* Set the tuple_idx-th tuple, but use SingleTuple */
  void Set(size_t tuple_idx, SingleTuple tuple);

  /* Append a tuple, but use SingleTuple */
  void Append(SingleTuple tuple);

  void Resize(size_t new_capacity);

  SingleTuple GetSingleTuple(size_t tuple_index) const {
    return SingleTuple(cols_, tuple_index);
  }

 private:
  /* vectors storing data of each column*/
  std::vector<Vector> cols_;
  /* amount of tuples */
  size_t num_tuple_{0};
  /* amount of valid tuples */
  size_t num_valid_tuple_{0};
  /* The capacity (the maximum amount of tuples). */
  size_t capacity_{0};
  /* selection vector, 0 represents valid, 1 represents invalid. */
  BitVector sel_;
};

}  // namespace wing
