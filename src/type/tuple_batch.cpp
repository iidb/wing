#include "type/tuple_batch.hpp"

namespace wing {

TupleBatch::TupleBatch(const TupleBatch& t) {
  cols_ = t.cols_;
  num_tuple_ = t.num_tuple_;
  num_valid_tuple_ = t.num_valid_tuple_;
  capacity_ = t.capacity_;
  sel_ = t.sel_;
}

TupleBatch::TupleBatch(TupleBatch&& t) {
  cols_ = std::move(t.cols_);
  num_tuple_ = t.num_tuple_;
  num_valid_tuple_ = t.num_valid_tuple_;
  capacity_ = t.capacity_;
  sel_ = std::move(t.sel_);
}

TupleBatch& TupleBatch::operator=(const TupleBatch& t) {
  cols_ = t.cols_;
  num_tuple_ = t.num_tuple_;
  num_valid_tuple_ = t.num_valid_tuple_;
  capacity_ = t.capacity_;
  sel_ = t.sel_;
  return *this;
}

TupleBatch& TupleBatch::operator=(TupleBatch&& t) {
  cols_ = std::move(t.cols_);
  num_tuple_ = t.num_tuple_;
  num_valid_tuple_ = t.num_valid_tuple_;
  capacity_ = t.capacity_;
  sel_ = std::move(t.sel_);
  return *this;
}

void TupleBatch::Init(const std::vector<LogicalType>& types, size_t capacity) {
  cols_.resize(types.size());
  for (uint32_t i = 0; i < cols_.size(); i++) {
    cols_[i] = Vector(VectorType::Flat, types[i], capacity);
    if (types[i] == LogicalType::STRING) {
      cols_[i].SetAux(StringVectorBuffer::Create());
    }
  }
  sel_ = BitVector(capacity);
  capacity_ = capacity;
  num_tuple_ = 0;
}

void TupleBatch::Init(std::span<Vector> vectors, size_t count, BitVector sel) {
  cols_.resize(vectors.size());
  for (uint32_t i = 0; i < vectors.size(); i++) {
    cols_[i] = vectors[i];
  }
  sel_ = sel;
  capacity_ = vectors.size() == 0 ? 0 : vectors[0].size();
  num_tuple_ = count;
  num_valid_tuple_ = sel.Count();
}

void TupleBatch::Init(const TupleBatch& input) {
  // Create new vectors
  cols_.resize(input.cols_.size());
  for (uint32_t i = 0; i < cols_.size(); i++) {
    cols_[i] = Vector(input.cols_[i].GetVectorType(),
        input.cols_[i].GetElemType(), input.num_tuple_);
    if (input.cols_[i].GetElemType() == LogicalType::STRING) {
      cols_[i].SetAux(input.cols_[i].GetAux());
    }
  }
  num_tuple_ = input.num_tuple_;
  capacity_ = input.capacity_;
  sel_ = BitVector(num_tuple_);
  // Copy valid tuples from input
  for (uint32_t i = 0, j = 0; i < input.sel_.size(); i++) {
    if (input.sel_[i]) {
      for (uint32_t k = 0; k < cols_.size(); k++) {
        cols_[k].Data()[j] = input.cols_[k].Data()[i];
      }
      sel_[j] = 1;
      j += 1;
    }
  }
}

void TupleBatch::Set(size_t tuple_idx, size_t col_idx, StaticFieldRef val) {
  if (cols_[col_idx].GetElemType() == LogicalType::STRING) {
    auto copied_str =
        static_cast<StringVectorBuffer*>(cols_[col_idx].GetAux().get())
            ->AddString(val);
    cols_[col_idx].Set(tuple_idx, copied_str);
  } else {
    cols_[col_idx].Set(tuple_idx, val);
  }
}

void TupleBatch::Set(size_t tuple_idx, std::span<const StaticFieldRef> tuple) {
  if (tuple.size() != cols_.size()) {
    DB_ERR(
        "Length of vector is different in TupleBatch::Set! Received {}, "
        "expected {}.",
        tuple.size(), cols_.size());
  }
  if (tuple_idx >= num_tuple_) {
    DB_ERR("The index of tuple {} is >= the number of tuples {}.", tuple_idx,
        num_tuple_);
  }
  for (uint32_t i = 0; i < tuple.size(); i++) {
    Set(tuple_idx, i, tuple[i]);
  }
  if (!sel_[tuple_idx]) {
    num_valid_tuple_ += 1;
  }
  sel_[tuple_idx] = 1;
}

void TupleBatch::Append(std::span<const StaticFieldRef> tuple) {
  if (num_tuple_ + 1 > capacity_) {
    DB_ERR("The number of tuples {} exceed capacity {}.", num_tuple_ + 1,
        capacity_);
  }
  num_tuple_ += 1;
  Set(num_tuple_ - 1, tuple);
}

void TupleBatch::Append(std::span<Vector> vectors, size_t tuple_idx) {
  if (num_tuple_ + 1 > capacity_) {
    DB_ERR("The number of tuples {} exceed capacity {}.", num_tuple_ + 1,
        capacity_);
  }
  if (vectors.size() != cols_.size()) {
    DB_ERR(
        "Length of vector is different in TupleBatch::Set! Received {}, "
        "expected {}.",
        vectors.size(), cols_.size());
  }
  num_tuple_ += 1;
  for (uint32_t i = 0; i < vectors.size(); i++) {
    Set(num_tuple_ - 1, i, vectors[i].Get(tuple_idx));
  }
  if (sel_[num_tuple_ - 1] == 0) {
    num_valid_tuple_ += 1;
  }
  sel_[num_tuple_ - 1] = 1;
}

void TupleBatch::Set(size_t tuple_idx, SingleTuple tuple) {
  if (tuple.size() != cols_.size()) {
    DB_ERR(
        "Lengths of vector are different in TupleBatch::Set! Received {}, "
        "expected {}.",
        tuple.size(), cols_.size());
  }
  if (tuple_idx >= num_tuple_) {
    DB_ERR("The index of tuple {} is >= the number of tuples {}.", tuple_idx,
        num_tuple_);
  }
  for (uint32_t i = 0; i < tuple.size(); i++) {
    Set(tuple_idx, i, tuple[i]);
  }
  if (sel_[tuple_idx] == 0) {
    num_valid_tuple_ += 1;
  }
  sel_[tuple_idx] = 1;
}

void TupleBatch::Append(SingleTuple tuple) {
  if (num_tuple_ + 1 > capacity_) {
    Resize(num_tuple_ + 1);
  }
  num_tuple_ += 1;
  Set(num_tuple_ - 1, tuple);
}

void TupleBatch::Resize(size_t new_capacity) {
  size_t sz = 16;
  while (sz < new_capacity)
    sz <<= 1;
  for (auto& col : cols_) {
    col.Resize(sz);
  }
  sel_.Resize(sz);
  capacity_ = sz;
}

void TupleBatch::Clear() {
  num_tuple_ = 0;
  num_valid_tuple_ = 0;
  sel_.SetZeros();
}

void TupleBatch::Shuffle(std::span<size_t> shuffle) {
  if (num_tuple_ != shuffle.size()) {
    DB_ERR("Length of shuffle is {}, but expected {}!", shuffle.size(),
        num_tuple_);
  }
  for (auto& col : cols_) {
    Vector new_col(col.GetVectorType(), col.GetElemType(), col.size());
    new_col.SetAux(col.GetAux());
    for (uint32_t i = 0; i < num_tuple_; i++) {
      new_col.Set(i, col.Get(shuffle[i]));
    }
    col = new_col;
  }

  BitVector new_sel(sel_.size());
  for (uint32_t i = 0; i < num_tuple_; i++) {
    new_sel[i] = bool(sel_[shuffle[i]]);
  }
  sel_ = new_sel;
}

std::vector<LogicalType> TupleBatch::GetColElemTypes() const {
  std::vector<LogicalType> ret;
  for (auto& col : cols_) {
    ret.push_back(col.GetElemType());
  }
  return ret;
}

}  // namespace wing
