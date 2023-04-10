#ifndef SAKURA_STATIC_FIELD_H__
#define SAKURA_STATIC_FIELD_H__

#include <memory>
#include <string>

#include "common/exception.hpp"
#include "common/logging.hpp"

namespace wing {

/**
 * StaticStringField is used for the case that the string is known and will not
 * be modified. For example, tuple stored in B+tree and the outputs of
 * Executors. The length of the string is stored at the beginning. It is not
 * managed by StaticStringField itself. It is determined at the beginning.
 *
 */
class StaticStringField {
 public:
  uint32_t size_{0};
  uint8_t str_[1];

  StaticStringField() = default;

  /* Generate a new StaticStringField from a[0...len - 1]. */
  static StaticStringField* Generate(const uint8_t* a, uint32_t len) {
    auto mem = reinterpret_cast<StaticStringField*>(
        new uint8_t[len + sizeof(uint32_t)]);
    std::memcpy(mem->str_, a, len);
    mem->size_ = len + sizeof(uint32_t);
    return mem;
  }

  /** Free the StaticStringField that is create by Generate. We cannot delete
   * field directly, since field is StaticStringField*, so it uses delete
   * operator, but the corresponding new is uint8_t[], which needs delete[]
   * operator.
   */
  static void FreeFromGenerate(StaticStringField* field) {
    delete[] reinterpret_cast<uint8_t*>(field);
  }

  /* Generate a new StaticStringField from a std::string_view. */
  static StaticStringField* Generate(std::string_view str) {
    return Generate(reinterpret_cast<const uint8_t*>(str.data()), str.size());
  }

  /* Generate a new StaticStringField from a StaticStringField. */
  static StaticStringField* Generate(const StaticStringField* field) {
    return Generate(field->str_, field->Length());
  }

  /* Write to dest (StaticStringField*) using the layout of StaticStringField.
   */
  static void Write(void* dest, const void* a, uint32_t len) {
    auto cdest = reinterpret_cast<uint8_t*>(dest);
    *reinterpret_cast<uint32_t*>(cdest) = len + sizeof(uint32_t);
    cdest += sizeof(uint32_t);
    std::memcpy(cdest, a, len);
  }

  /* Copy the string from src to dest. Only copy str_[0...size_ - 1].*/
  static void Copy(void* dest, const void* src) {
    auto src_field = reinterpret_cast<const StaticStringField*>(src);
    Write(dest, src_field->str_, src_field->Length());
  }

  /* Create a std::string_view referring this string. */
  std::string_view ReadStringView() const {
    return std::string_view(reinterpret_cast<const char*>(str_), Length());
  }

  /* Create a std::string from str_[0...size_ - 1]. */
  std::string ReadString() const {
    return std::string(reinterpret_cast<const char*>(str_), Length());
  }

  /* Write the string to data, return pointer pointing to the end. */
  uint8_t* Write(uint8_t* data) const {
    std::memcpy(data, &size_, size_);
    return data + size_;
  }

  /* The length of the string. */
  uint32_t Length() const { return size_ - sizeof(uint32_t); }
};

/**
 * StaticFieldRef is a ref to data of all field types.
 * It is used in the case that types are known.
 * For example, outputs of Executors.
 * It uses 8 byte, which is the lower bound.
 *
 * To create a StaticFieldRef, you better use StaticFieldRef::CreateXXX,
 * since StaticFieldRef(int64_t) and StaticFieldRef(double) cannot be deduced
 * correctly sometimes. To read, you can use
 */
class StaticFieldRef {
 public:
  union {
    int64_t int_data;
    const StaticStringField* str_data;
    double double_data;
  } data_;

  StaticFieldRef() = default;

  StaticFieldRef(int64_t a) { data_.int_data = a; }

  StaticFieldRef(double a) { data_.double_data = a; }

  StaticFieldRef(const StaticStringField* a) { data_.str_data = a; }

  /* Create a 64-bit integer. */
  static StaticFieldRef CreateInt(int64_t a) {
    StaticFieldRef ret;
    ret.data_.int_data = a;
    return ret;
  }

  /* Create a 64-bit float. */
  static StaticFieldRef CreateFloat(double a) {
    StaticFieldRef ret;
    ret.data_.double_data = a;
    return ret;
  }

  /* Create a string ref. */
  static StaticFieldRef CreateStringRef(const StaticStringField* a) {
    StaticFieldRef ret;
    ret.data_.str_data = a;
    return ret;
  }

  /* Create a string ref from std::string_view. */
  static StaticFieldRef CreateFromStringView(
      const std::string_view a, FieldType type) {
    StaticFieldRef ret;
    if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      ret.data_.str_data = reinterpret_cast<const StaticStringField*>(a.data());
      return ret;
    } else if (type == FieldType::INT32) {
      ret.data_.int_data = *reinterpret_cast<const int32_t*>(a.data());
      return ret;
    } else if (type == FieldType::INT64) {
      ret.data_.int_data = *reinterpret_cast<const int64_t*>(a.data());
      return ret;
    } else if (type == FieldType::FLOAT64) {
      ret.data_.int_data = *reinterpret_cast<const double*>(a.data());
      return ret;
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

  /* Read integer. */
  int64_t ReadInt() const { return data_.int_data; }

  /* Read float64. */
  double ReadFloat() const { return data_.double_data; }

  /* Read StaticStringField pointer. */
  const StaticStringField* ReadStringFieldPointer() const {
    return data_.str_data;
  }

  /* Read std::string_view. */
  std::string_view ReadStringView() const {
    return data_.str_data->ReadStringView();
  }

  /* Read string, and create std::string. */
  std::string ReadString() const { return data_.str_data->ReadString(); }

  /* Use fmt::format to format the data. */
  std::string ToString(FieldType type, [[maybe_unused]] size_t size) const {
    if (type == FieldType::INT32) {
      return fmt::format("{}", (int32_t)data_.int_data);
    } else if (type == FieldType::INT64) {
      return fmt::format("{}", (int64_t)data_.int_data);
    } else if (type == FieldType::FLOAT64) {
      return fmt::format("{}", data_.double_data);
    } else if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      // TODO: CHAR type uses another way.
      return data_.str_data->ReadString();
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

  /* Write the data to a memory region. */
  uint8_t* Write(FieldType type, size_t size, uint8_t* data) const {
    if (type == FieldType::INT32) {
      *reinterpret_cast<int32_t*>(data) = (int32_t)data_.int_data;
      return data + sizeof(int32_t);
    } else if (type == FieldType::INT64) {
      *reinterpret_cast<int64_t*>(data) = (int64_t)data_.int_data;
      return data + sizeof(int64_t);
    } else if (type == FieldType::FLOAT64) {
      *reinterpret_cast<double*>(data) = (double)data_.double_data;
      return data + sizeof(double);
    } else if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      // TODO: CHAR type uses another way.
      return data_.str_data->Write(data);
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

  /* Read data from a memory region. */
  const uint8_t* Read(FieldType type, size_t size, const uint8_t* data) {
    if (type == FieldType::INT32) {
      data_.int_data = *reinterpret_cast<const int32_t*>(data);
      return data + sizeof(int32_t);
    } else if (type == FieldType::INT64) {
      data_.int_data = *reinterpret_cast<const int64_t*>(data);
      return data + sizeof(int64_t);
    } else if (type == FieldType::FLOAT64) {
      data_.double_data = *reinterpret_cast<const double*>(data);
      return data + sizeof(double);
    } else if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      // TODO: CHAR type uses another way.
      data_.str_data = reinterpret_cast<const StaticStringField*>(data);
      return data + data_.str_data->size_;
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

  /* Return the size of a type. */
  size_t Size(FieldType type, size_t size) const {
    if (type == FieldType::INT32) {
      return sizeof(int32_t);
    } else if (type == FieldType::INT64) {
      return sizeof(int64_t);
    } else if (type == FieldType::FLOAT64) {
      return sizeof(double);
    } else if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      // TODO: CHAR type uses another way.
      return data_.str_data->size_;
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

  /** Return the view to the data. For string data it is simple,
   * But for numeric data, it uses the address of (*this).
   * You should use GetView() in the same scope of the StaticFieldRef variable.
   */
  static std::string_view GetView(
      const StaticFieldRef* a, FieldType type, size_t size) {
    if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      // TODO: CHAR type uses another way.
      return a->ReadStringView();
    } else {
      // If the CPU is big-endian and type is int32, we use the last 4 bytes.
      if (std::endian::native == std::endian::big && type == FieldType::INT32) {
        return {reinterpret_cast<const char*>(a) + 4, size};
      }
      return {reinterpret_cast<const char*>(a), size};
    }
  }
};

}  // namespace wing

#endif