#pragma once

#include "plan/output_schema.hpp"
#include "type/field.hpp"
#include "type/static_field.hpp"
#include "type/tuple.hpp"

namespace wing {

class StaticFieldArray {
 public:
  StaticFieldArray() = default;
  StaticFieldArray(StaticFieldArray&& vec) noexcept
    : vec_(std::move(vec.vec_)), str_(std::move(vec.str_)) {}
  StaticFieldArray(const std::vector<Field>& fields) {
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
        *reinterpret_cast<uint32_t*>(str_.get() + str_offset) =
            a.size_ + sizeof(uint32_t);
        std::memcpy(str_.get() + str_offset + sizeof(uint32_t),
            a.data_.str_data, a.size_);
        vec_[i].data_.str_data =
            reinterpret_cast<const StaticStringField*>(str_.get() + str_offset);
        str_offset += a.size_ + sizeof(uint32_t);
      }
      i++;
    }
  }
  StaticFieldArray& operator=(StaticFieldArray&& vec) noexcept {
    vec_ = std::move(vec.vec_);
    str_ = std::move(vec.str_);
    return *this;
  }

  const std::vector<StaticFieldRef>& GetFieldVector() const { return vec_; }

  std::vector<StaticFieldRef>& GetFieldVector() { return vec_; }

 private:
  std::vector<StaticFieldRef> vec_;
  std::unique_ptr<uint8_t[]> str_;
};

/**
 * Store tuples of a output schema.
 */
class TupleArray {
 public:
  /* This is invalid. */
  TupleArray() = default;

  /* Construct with an input_schema.*/
  explicit TupleArray(const OutputSchema& input_schema)
    : columns_schema_(input_schema) {
    field_num_ = input_schema.size();
    static_field_size_ = 0;
    has_str_field_ = false;
    /* Calulate the total size of fields of invariant size. */
    for (uint32_t index = 0; auto& a : input_schema.GetCols()) {
      if (a.type_ != LogicalType::STRING) {
        static_field_size_ += a.size_;
      } else {
        has_str_field_ = true;
        str_indexes_.push_back(index);
      }
      index += 1;
    }
  }

  TupleArray(TupleArray&& t) noexcept {
    columns_schema_ = t.columns_schema_;
    has_str_field_ = t.has_str_field_;
    field_num_ = t.field_num_;
    static_field_size_ = t.static_field_size_;
    columns_schema_ = std::move(t.columns_schema_);
    str_indexes_ = std::move(t.str_indexes_);
    allocator_ = std::move(t.allocator_);
  }

  TupleArray& operator=(TupleArray&& t) noexcept {
    columns_schema_ = t.columns_schema_;
    has_str_field_ = t.has_str_field_;
    field_num_ = t.field_num_;
    static_field_size_ = t.static_field_size_;
    columns_schema_ = std::move(t.columns_schema_);
    str_indexes_ = std::move(t.str_indexes_);
    allocator_ = std::move(t.allocator_);
    return *this;
  }

  /* It cannot be copied.*/
  TupleArray(const TupleArray&) = delete;

  /* Append a tuple. Return a pointer pointing to it. You should store it. */
  uint8_t* Append(const void* input) {
    auto size = field_num_ * sizeof(StaticFieldRef);
    for (auto index : str_indexes_) {
      size += reinterpret_cast<const StaticFieldRef*>(input)[index].size(
          FieldType::VARCHAR, 0);
    }
    /* Allocate memory for StaticFieldRefs and string data. */
    auto ret = allocator_.Allocate(size);
    std::memcpy(ret, input, field_num_ * sizeof(StaticFieldRef));
    /* Copy string to the allocated memory region. */
    auto data_ptr = ret + field_num_ * sizeof(StaticFieldRef);
    auto vec = reinterpret_cast<const StaticFieldRef*>(ret);
    for (auto index : str_indexes_) {
      StaticStringField::Copy(data_ptr, vec[index].ReadStringFieldPointer());
      /* Set the pointers of string fields to copied strings. */
      reinterpret_cast<StaticFieldRef*>(ret)[index].data_.str_data =
          reinterpret_cast<const StaticStringField*>(data_ptr);
      data_ptr += vec[index].size(FieldType::VARCHAR, 0);
    }
    return ret;
  }

  void Clear() { allocator_.Clear(); }

 private:
  /* Check if it has string (VARCHAR) field. */
  bool has_str_field_{false};
  /* The number of fields. */
  uint32_t field_num_{0};
  /* The total size of fields of invariant size. */
  uint32_t static_field_size_{0};
  /* The column schema. */
  OutputSchema columns_schema_;
  /* The index of string (VARCHAR) fields in columns_schema_. */
  std::vector<uint32_t> str_indexes_;
  /* The allocator for tuple data allocating. */
  ArenaAllocator allocator_;
};

/**
 * A good encapsulation for TupleArray.
 * Used for storing input of a certain schema.
 */
class TupleStore {
 public:
  TupleStore() = default;
  TupleStore(const OutputSchema& input_schema) : tuple_vec_(input_schema) {}

  /* Append tuple. */
  void Append(const void* input) {
    pointer_vec_.push_back(tuple_vec_.Append(input));
  }

  /* Get all tuples. */
  const std::vector<uint8_t*>& GetPointerVec() const { return pointer_vec_; }

 private:
  /* The TupleArray. */
  TupleArray tuple_vec_;
  /* The vector storing pointers pointing to tuples. */
  std::vector<uint8_t*> pointer_vec_;
};

}  // namespace wing
