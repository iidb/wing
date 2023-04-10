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
  StaticFieldVector(StaticFieldVector&& vec) noexcept
    : vec_(std::move(vec.vec_)), str_(std::move(vec.str_)) {}
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

/**
 * Store tuples of a output schema.
 */
class TupleVector {
 public:
  /* This is invalid. */
  TupleVector() = default;

  /* Construct with an input_schema.*/
  explicit TupleVector(const OutputSchema& input_schema) {
    is_raw_data_flag_ = input_schema.IsRaw();
    field_num_ = input_schema.Size();
    static_field_size_ = 0;
    has_str_field_ = false;
    /* Calulate the total size of fields of invariant size. */
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

  TupleVector(TupleVector&& t) noexcept {
    is_raw_data_flag_ = t.is_raw_data_flag_;
    columns_schema_ = t.columns_schema_;
    has_str_field_ = t.has_str_field_;
    field_num_ = t.field_num_;
    static_field_size_ = t.static_field_size_;
    columns_schema_ = std::move(t.columns_schema_);
    str_indexes_ = std::move(t.str_indexes_);
    allocator_ = std::move(t.allocator_);
  }

  TupleVector& operator=(TupleVector&& t) noexcept {
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

  /* It cannot be copied.*/
  TupleVector(const TupleVector&) = delete;

  /* Append a tuple. Return a pointer pointing to it. You should store it. */
  uint8_t* Append(const uint8_t* input) {
    auto size = field_num_ * sizeof(StaticFieldRef);
    if (is_raw_data_flag_) {
      /* If it is raw data, then we get the size of strings by reading the
       * offset table. */
      size += has_str_field_
                  ? Tuple::GetSizeOfAllStrings(input, static_field_size_)
                  : 0;
    } else {
      /* If it is not raw data, then we get the size by reading size of each
       * string fields. */
      for (auto index : str_indexes_) {
        size += reinterpret_cast<const StaticFieldRef*>(input)[index].Size(
            FieldType::VARCHAR, 0);
      }
    }
    /* Allocate memory for StaticFieldRefs and string data. */
    auto ret = allocator_.Allocate(size);
    if (is_raw_data_flag_) {
      /* If it is raw data, we must deserialize it first. */
      Tuple::DeSerialize(ret, input, columns_schema_);
    } else {
      /* If it is not raw data, copy the fields directly. String fields are set
       * after. */
      std::memcpy(ret, input, field_num_ * sizeof(StaticFieldRef));
    }
    /* Copy string to the allocated memory region. */
    auto data_ptr = ret + field_num_ * sizeof(StaticFieldRef);
    auto vec = reinterpret_cast<const StaticFieldRef*>(ret);
    for (auto index : str_indexes_) {
      StaticStringField::Copy(data_ptr, vec[index].ReadStringFieldPointer());
      /* Set the pointers of string fields to copied strings. */
      reinterpret_cast<StaticFieldRef*>(ret)[index].data_.str_data =
          reinterpret_cast<const StaticStringField*>(data_ptr);
      data_ptr += vec[index].Size(FieldType::VARCHAR, 0);
    }
    return ret;
  }

  void Clear() { allocator_.Clear(); }

 private:
  /* Check if it is raw data. i.e. the serialized tuple stored in B+tree. */
  bool is_raw_data_flag_{false};
  /* Check if it has string (VARCHAR) field. */
  bool has_str_field_{false};
  /* The number of fields. */
  uint32_t field_num_{0};
  /* The total size of fields of invariant size. */
  uint32_t static_field_size_{0};
  /* The column schema. */
  std::vector<ColumnSchema> columns_schema_;
  /* The index of string (VARCHAR) fields in columns_schema_. */
  std::vector<uint32_t> str_indexes_;
  /* The allocator for tuple data allocating. */
  BlockAllocator<8192> allocator_;
};

/**
 * A good encapsulation for TupleVector.
 * Used for storing input of a certain schema.
 */
class TupleStore {
 public:
  TupleStore() = default;
  TupleStore(const OutputSchema& input_schema) : tuple_vec_(input_schema) {}

  /* Append tuple. */
  void Append(const uint8_t* input) {
    pointer_vec_.push_back(tuple_vec_.Append(input));
  }

  /* Get all tuples. */
  const std::vector<uint8_t*>& GetPointerVec() const { return pointer_vec_; }

 private:
  /* The TupleVector. */
  TupleVector tuple_vec_;
  /* The vector storing pointers pointing to tuples. */
  std::vector<uint8_t*> pointer_vec_;
};

}  // namespace wing

#endif