#ifndef SAKURA_TYPE_H__
#define SAKURA_TYPE_H__

#include <cstring>
#include <memory>
#include <vector>

#include "common/exception.hpp"
#include "common/logging.hpp"
#include "type/field_type.hpp"

namespace wing {

/**
 * FieldRef is a reference to an existing memory region. This memory region may
 * become invalid without informing FieldRef. So you must use FieldRef within
 * the lifetime of the memory region.
 *
 * For integers and real numbers, their sizes equal to the size of uint8_t* on
 * 64-bit machine. So if type is FieldType::INT32 or FieldType::INT64 or
 * FieldType::FLOAT64, we store the value directly.
 *
 * For integers of different length, we store them using int64_t and cast them
 * to int32_t if it is needed.
 *
 * FieldRef is used in iterating pages of tables and passing parameters while
 * executing. Clearly, pages cannot be flushed while being iterated. Parameters
 * cannot be released while evaluating expressions.
 */

class FieldRef {
 public:
  FieldType type_;
  uint32_t size_;
  union {
    int64_t int_data;
    const uint8_t* str_data;
    double double_data;
  } data_;

  FieldRef() : type_(FieldType::EMPTY) {}

  static FieldRef CreateInt(FieldType type, uint32_t size, int64_t int_data) {
    DB_ASSERT(type == FieldType::INT32 || type == FieldType::INT64);
    DB_ASSERT((type == FieldType::INT32 && size == 4) ||
              (type == FieldType::INT64 && size == 8));
    FieldRef ret;
    ret.type_ = type;
    ret.size_ = size;
    ret.data_.int_data = int_data;
    return ret;
  }

  static FieldRef CreateStringRef(
      FieldType type, uint32_t size, const uint8_t* str_data) {
    DB_ASSERT(type == FieldType::CHAR || type == FieldType::VARCHAR);
    DB_ASSERT(str_data != nullptr);
    FieldRef ret;
    ret.type_ = type;
    ret.size_ = size;
    ret.data_.str_data = str_data;
    return ret;
  }

  static FieldRef CreateFloat(
      FieldType type, uint32_t size, double double_data) {
    DB_ASSERT(type == FieldType::FLOAT64);
    DB_ASSERT(size == 8);
    FieldRef ret;
    ret.type_ = type;
    ret.size_ = size;
    ret.data_.double_data = double_data;
    return ret;
  }

  static FieldRef CreateStringRef(FieldType type, std::string_view str) {
    return CreateStringRef(
        type, str.size(), reinterpret_cast<const uint8_t*>(str.data()));
  }

  uint32_t Size() { return size_; }

  static FieldRef Read(FieldType type, size_t size, const uint8_t* a) {
    FieldRef ret;
    ret.type_ = type;
    ret.size_ = size;
    switch (type) {
      case FieldType::INT32:
        ret.data_.int_data = *reinterpret_cast<const int32_t*>(a);
        break;
      case FieldType::INT64:
        ret.data_.int_data = *reinterpret_cast<const int64_t*>(a);
        break;
      case FieldType::FLOAT64:
        ret.data_.double_data = *reinterpret_cast<const double*>(a);
        break;
      case FieldType::CHAR:
      case FieldType::VARCHAR:
        ret.data_.str_data = a;
        break;
      case FieldType::EMPTY:
        DB_ERR("This is a null field!");
        break;
      default:
        DB_ERR("Unrecognized FieldType!");
    }
    return ret;
  }

  void Write(uint8_t* a) const {
    switch (type_) {
      case FieldType::INT32:
        *reinterpret_cast<int32_t*>(a) = data_.int_data;
        break;
      case FieldType::INT64:
        *reinterpret_cast<int64_t*>(a) = data_.int_data;
        break;
      case FieldType::FLOAT64:
        *reinterpret_cast<double*>(a) = data_.double_data;
        break;
      case FieldType::CHAR:
      case FieldType::VARCHAR:
        std::memcpy(a, data_.str_data, size_);
        break;
      case FieldType::EMPTY:
        DB_ERR("This is a null field!");
        break;
      default:
        DB_ERR("Unrecognized FieldType!");
    }
  }

  int64_t ReadInt() const { return data_.int_data; }

  double ReadFloat() const { return data_.double_data; }

  std::string ReadString() const {
    return std::string(reinterpret_cast<const char*>(data_.str_data), size_);
  }

  std::string_view ReadStringView() const {
    return std::string_view(
        reinterpret_cast<const char*>(data_.str_data), size_);
  }

  std::string ToString() const {
    if (type_ == FieldType::INT32) {
      return fmt::format("{}", (int32_t)data_.int_data);
    } else if (type_ == FieldType::INT64) {
      return fmt::format("{}", (int64_t)data_.int_data);
    } else if (type_ == FieldType::FLOAT64) {
      return fmt::format("{}", data_.double_data);
    } else if (type_ == FieldType::EMPTY) {
      return "null";
    } else if (type_ == FieldType::CHAR || type_ == FieldType::VARCHAR) {
      return std::string(reinterpret_cast<const char*>(data_.str_data), size_);
    } else
      DB_ERR("Internal Error: Unrecognized FieldType.");
  }
};

/**
 * Field is a independent field, i.e., it is not a reference to an existing
 * memory area.
 *
 */

class Field {
 public:
  FieldType type_;
  uint32_t size_;
  union {
    int64_t int_data;
    uint8_t* str_data;
    double double_data;
  } data_;

  Field() : type_(FieldType::EMPTY), size_(0) { data_.int_data = 0; }

  static Field CreateInt(FieldType type, uint32_t size, int64_t int_data) {
    DB_ASSERT(type == FieldType::INT32 || type == FieldType::INT64);
    DB_ASSERT((type == FieldType::INT32 && size == 4) ||
              (type == FieldType::INT64 && size == 8));
    Field ret;
    ret.type_ = type;
    ret.size_ = size;
    ret.data_.int_data = int_data;
    return ret;
  }

  static Field CreateString(FieldType type, uint32_t size) {
    DB_ASSERT(type == FieldType::CHAR || type == FieldType::VARCHAR);
    Field ret;
    ret.type_ = type;
    ret.size_ = size;
    ret.data_.str_data = new uint8_t[size];
    return ret;
  }

  static Field CreateString(FieldType type, uint32_t size, const uint8_t* str) {
    DB_ASSERT(type == FieldType::CHAR || type == FieldType::VARCHAR);
    Field ret;
    ret.type_ = type;
    ret.size_ = size;
    ret.data_.str_data = new uint8_t[size];
    std::memcpy(ret.data_.str_data, str, size);
    return ret;
  }

  static Field CreateString(FieldType type, std::string_view str) {
    return CreateString(
        type, str.size(), reinterpret_cast<const uint8_t*>(str.data()));
  }

  static Field CreateFloat(FieldType type, uint32_t size, double double_data) {
    DB_ASSERT(type == FieldType::FLOAT64);
    DB_ASSERT(size == 8);
    Field ret;
    ret.type_ = type;
    ret.size_ = size;
    ret.data_.double_data = double_data;
    return ret;
  }

  Field& operator=(const FieldRef& d) {
    if ((type_ == FieldType::CHAR || type_ == FieldType::VARCHAR) &&
        data_.str_data != nullptr) {
      delete[] data_.str_data;
    }
    if (d.type_ == FieldType::CHAR || d.type_ == FieldType::VARCHAR) {
      data_.str_data = new uint8_t[d.size_];
      std::memcpy(data_.str_data, d.data_.str_data, d.size_);
    } else {
      data_.double_data = d.data_.double_data;
    }
    size_ = d.size_;
    type_ = d.type_;
    return *this;
  }

  Field& operator=(const Field& d) { return (*this) = d.Ref(); }

  Field& operator=(Field&& d) noexcept {
    data_.double_data = d.data_.double_data;
    size_ = d.size_;
    type_ = d.type_;
    d.data_.str_data = 0;
    d.size_ = 0;
    return *this;
  }

  Field(const Field& d) {
    if (d.type_ == FieldType::CHAR || d.type_ == FieldType::VARCHAR) {
      data_.str_data = new uint8_t[d.size_];
      std::memcpy(data_.str_data, d.data_.str_data, d.size_);
    } else {
      data_.double_data = d.data_.double_data;
    }
    size_ = d.size_;
    type_ = d.type_;
  }

  Field(const FieldRef& d) {
    if (d.type_ == FieldType::CHAR || d.type_ == FieldType::VARCHAR) {
      data_.str_data = new uint8_t[d.size_];
      std::memcpy(data_.str_data, d.data_.str_data, d.size_);
    } else {
      data_.double_data = d.data_.double_data;
    }
    size_ = d.size_;
    type_ = d.type_;
  }

  Field(Field&& d) noexcept {
    data_.double_data = d.data_.double_data;
    size_ = d.size_;
    type_ = d.type_;
    d.data_.str_data = 0;
    d.size_ = 0;
  }

  ~Field() {
    if ((type_ == FieldType::CHAR || type_ == FieldType::VARCHAR) &&
        data_.str_data != nullptr)
      delete[] data_.str_data;
  }

  uint32_t Size() { return size_; }

  FieldRef Ref() const {
    if (type_ == FieldType::INT32 || type_ == FieldType::INT64)
      return FieldRef::CreateInt(type_, size_, data_.int_data);
    else if (type_ == FieldType::FLOAT64)
      return FieldRef::CreateFloat(type_, size_, data_.double_data);
    else if (type_ == FieldType::CHAR || type_ == FieldType::VARCHAR)
      return FieldRef::CreateStringRef(type_, size_, data_.str_data);
    else if (type_ == FieldType::EMPTY)
      return FieldRef();
    else
      DB_ERR("Unrecognized FieldType!");
  }

  static Field Read(FieldType type, size_t size, const uint8_t* a) {
    Field ret;
    ret.type_ = type;
    ret.size_ = size;
    switch (type) {
      case FieldType::INT32:
        ret.data_.int_data = *reinterpret_cast<const int32_t*>(a);
        break;
      case FieldType::INT64:
        ret.data_.int_data = *reinterpret_cast<const int64_t*>(a);
        break;
      case FieldType::FLOAT64:
        ret.data_.double_data = *reinterpret_cast<const double*>(a);
        break;
      case FieldType::CHAR:
      case FieldType::VARCHAR:
        ret.data_.str_data = new uint8_t[size];
        std::memcpy(ret.data_.str_data, a, size);
        break;
      case FieldType::EMPTY:
        DB_ERR("This is a null field!");
        break;
      default:
        DB_ERR("Unrecognized FieldType!");
    }
    return ret;
  }

  void Write(uint8_t* a) const {
    switch (type_) {
      case FieldType::INT32:
        *reinterpret_cast<int32_t*>(a) = data_.int_data;
        break;
      case FieldType::INT64:
        *reinterpret_cast<int64_t*>(a) = data_.int_data;
        break;
      case FieldType::FLOAT64:
        *reinterpret_cast<double*>(a) = data_.double_data;
        break;
      case FieldType::CHAR:
      case FieldType::VARCHAR:
        std::memcpy(a, data_.str_data, size_);
        break;
      case FieldType::EMPTY:
        DB_ERR("This is a null field!");
        break;
      default:
        DB_ERR("Unrecognized FieldType!");
    }
  }

  int64_t ReadInt() const { return data_.int_data; }

  double ReadFloat() const { return data_.double_data; }

  std::string ReadString() const {
    return std::string(reinterpret_cast<char*>(data_.str_data), size_);
  }

  /* Get std::string_view of string Field. If Field is integer/float then don't use it. */
  std::string_view ReadStringView() const {
    return std::string_view(
        reinterpret_cast<const char*>(data_.str_data), size_);
  }

  /* Get std::string_view of Field */
  std::string_view GetView() const {
    if (type_ == FieldType::CHAR || type_ == FieldType::VARCHAR) {
      // TODO: CHAR type uses another way.
      return ReadStringView();
    } else {
      // If the CPU is big-endian and type is int32, we use the last 4 bytes.
      if (std::endian::native == std::endian::big && type_ == FieldType::INT32) {
        return {reinterpret_cast<const char*>(&data_.int_data) + 4, 4};
      }
      return {reinterpret_cast<const char*>(&data_.int_data), 8};
    }
  }

  std::string ToString() const {
    if (type_ == FieldType::INT32) {
      return fmt::format("{}", (int32_t)data_.int_data);
    } else if (type_ == FieldType::INT64) {
      return fmt::format("{}", (int64_t)data_.int_data);
    } else if (type_ == FieldType::FLOAT64) {
      return fmt::format("{}", data_.double_data);
    } else if (type_ == FieldType::EMPTY) {
      return "null";
    } else if (type_ == FieldType::CHAR || type_ == FieldType::VARCHAR) {
      return std::string(reinterpret_cast<char*>(data_.str_data), size_);
    } else
      DB_ERR("Internal Error: Unrecognized FieldType.");
  }

  std::partial_ordering operator<=>(const Field& f) const {
    if (type_ == FieldType::INT32 || type_ == FieldType::INT64) {
      return data_.int_data <=> f.data_.int_data;
    } else if (type_ == FieldType::FLOAT64) {
      return data_.double_data <=> f.data_.double_data;
    } else if (type_ == FieldType::CHAR || type_ == FieldType::VARCHAR) {
      return ReadStringView() <=> f.ReadStringView();
    } else
      DB_ERR("Internal Error: Invalid FieldType.");
  }

  bool Empty() const { return type_ == FieldType::EMPTY; }
};

}  // namespace wing

#endif