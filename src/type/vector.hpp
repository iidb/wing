#ifndef SAKURA_VECTOR_H__
#define SAKURA_VECTOR_H__

#include "plan/output_schema.hpp"
#include "type/field.hpp"
#include "type/static_field.hpp"
#include "type/tuple.hpp"

namespace wing {

class StaticFieldVector {
 public:
  StaticFieldVector() = default;
  StaticFieldVector(StaticFieldVector&& vec) noexcept : vec_(std::move(vec.vec_)), str_(std::move(vec.str_)) {}
  StaticFieldVector(const std::vector<Field>& fields) {
    vec_.reserve(fields.size());
    size_t str_sz = 0;
    for (auto& a : fields) {
      if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
        str_sz += a.size_ + 4;
      }
      vec_.push_back(StaticFieldRef::CreateInt(a.data_.int_data));
    }
    str_ = std::unique_ptr<uint8_t[]>(new uint8_t[str_sz]);
    size_t str_offset = 0;
    for (uint32_t i = 0; auto& a : fields) {
      if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
        *reinterpret_cast<uint32_t*>(str_.get() + str_offset) = a.size_ + sizeof(uint32_t);
        std::memcpy(str_.get() + str_offset + sizeof(uint32_t), a.data_.str_data, a.size_);
        vec_[i].data_.str_data = reinterpret_cast<const StaticStringField*>(str_.get() + str_offset);
        str_offset += a.size_ + sizeof(uint32_t);
      }
      i++;
    }
  }
  StaticFieldVector& operator=(StaticFieldVector&& vec) noexcept {
    vec_ = std::move(vec.vec_);
    str_ = std::move(vec.str_);
    return *this;
  }

  const std::vector<StaticFieldRef>& GetFieldVector() const { return vec_; }

 private:
  std::vector<StaticFieldRef> vec_;
  std::unique_ptr<uint8_t[]> str_;
};

class TupleVector {
 public:
  TupleVector() = default;
  TupleVector(const OutputSchema& input_schema) {
    is_raw_data_flag_ = input_schema.IsRaw();
    field_num_ = input_schema.Size();
    static_field_size_ = 0;
    has_str_field_ = false;
    for (uint32_t index = 0; auto& a : input_schema.GetCols()) {
      if (a.type_ != FieldType::CHAR && a.type_ != FieldType::VARCHAR) {
        static_field_size_ += a.size_;
      } else {
        has_str_field_ = true;
        str_indexes_.push_back(index);
      }
      columns_schema_.emplace_back(std::string(""), a.type_, a.size_);
      index += 1;
    }
  }

  TupleVector(TupleVector&& t) {
    is_raw_data_flag_ = t.is_raw_data_flag_;
    columns_schema_ = t.columns_schema_;
    has_str_field_ = t.has_str_field_;
    field_num_ = t.field_num_;
    static_field_size_ = t.static_field_size_;
    columns_schema_ = std::move(t.columns_schema_);
    str_indexes_ = std::move(t.str_indexes_);
    allocator_ = std::move(t.allocator_);
  }

  TupleVector& operator=(TupleVector&& t) {
    is_raw_data_flag_ = t.is_raw_data_flag_;
    columns_schema_ = t.columns_schema_;
    has_str_field_ = t.has_str_field_;
    field_num_ = t.field_num_;
    static_field_size_ = t.static_field_size_;
    columns_schema_ = std::move(t.columns_schema_);
    str_indexes_ = std::move(t.str_indexes_);
    allocator_ = std::move(t.allocator_);
    return *this;
  }

  TupleVector(const TupleVector&) = delete;

  uint8_t* Append(const uint8_t* input) {
    auto size = field_num_ * sizeof(StaticFieldRef);
    if (is_raw_data_flag_) {
      size += has_str_field_ ? Tuple::GetSizeOfAllStrings(input, static_field_size_) : 0;
    } else {
      for (auto index : str_indexes_) {
        size += reinterpret_cast<const StaticFieldRef*>(input)[index].Size(FieldType::VARCHAR);
      }
    }
    auto ret = allocator_.Allocate(size);
    if (is_raw_data_flag_) {
      Tuple::DeSerialize(ret, input, columns_schema_);
    } else {
      std::memcpy(ret, input, field_num_ * sizeof(StaticFieldRef));
    }
    auto data_ptr = ret + field_num_ * sizeof(StaticFieldRef);
    auto vec = reinterpret_cast<const StaticFieldRef*>(ret);
    for (auto index : str_indexes_) {
      StaticStringField::Copy(data_ptr, vec[index].ReadStringFieldPointer());
      reinterpret_cast<StaticFieldRef*>(ret)[index].data_.str_data = reinterpret_cast<const StaticStringField*>(data_ptr);
      data_ptr += vec[index].Size(FieldType::VARCHAR);
    }
    return ret;
  }

  void Clear() {
    allocator_.Clear();
  }

 private:
  bool is_raw_data_flag_{false};
  bool has_str_field_{false};
  uint32_t field_num_{0};
  uint32_t static_field_size_{0};
  std::vector<ColumnSchema> columns_schema_;
  std::vector<uint32_t> str_indexes_;
  BlockAllocator<8192> allocator_;
};

class TupleStore {
 public:
  TupleStore() = default;
  TupleStore(const OutputSchema& input_schema) : tuple_vec_(input_schema) {}
  void Append(const uint8_t* input) { pointer_vec_.push_back(tuple_vec_.Append(input)); }
  const std::vector<uint8_t*>& GetPointerVec() const { return pointer_vec_; }

 private:
  TupleVector tuple_vec_;
  std::vector<uint8_t*> pointer_vec_;
};

}  // namespace wing

#endif