#ifndef SAKURA_STATIC_FIELD_H__
#define SAKURA_STATIC_FIELD_H__

#include <memory>
#include <string>

#include "common/exception.hpp"
#include "common/logging.hpp"

namespace wing {

class StaticStringField {
 public:
  uint32_t size_{0};
  uint8_t str_[1];

  StaticStringField() = default;

  static StaticStringField* Generate(const uint8_t* a, uint32_t len) {
    auto mem = reinterpret_cast<StaticStringField*>(new uint8_t[len + sizeof(uint32_t)]);
    std::memcpy(mem->str_, a, len);
    mem->size_ = len + sizeof(uint32_t);
    return mem;
  }

  /* Free the StaticStringField that is create by Generate */
  static void FreeFromGenerate(StaticStringField* field) {
    delete[] reinterpret_cast<uint8_t*>(field);
  }

  static StaticStringField* Generate(std::string_view str) { return Generate(reinterpret_cast<const uint8_t*>(str.data()), str.size()); }

  static StaticStringField* Generate(const StaticStringField* field) { return Generate(field->str_, field->Length()); }

  static void Write(void* dest, const void* a, uint32_t len) {
    auto cdest = reinterpret_cast<uint8_t*>(dest);
    *reinterpret_cast<uint32_t*>(cdest) = len + sizeof(uint32_t);
    cdest += sizeof(uint32_t);
    std::memcpy(cdest, a, len);
  }

  static void Copy(void* dest, const void* src) {
    auto src_field = reinterpret_cast<const StaticStringField*>(src);
    Write(dest, src_field->str_, src_field->Length());
  }

  std::string_view ReadStringView() const { return std::string_view(reinterpret_cast<const char*>(str_), Length()); }

  std::string ReadString() const { return std::string(reinterpret_cast<const char*>(str_), Length()); }

  uint8_t* Write(uint8_t* data) const {
    std::memcpy(data, &size_, size_);
    return data + size_;
  }

  uint32_t Length() const { return size_ - sizeof(uint32_t); }
};

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

  static StaticFieldRef CreateInt(int64_t a) {
    StaticFieldRef ret;
    ret.data_.int_data = a;
    return ret;
  }

  static StaticFieldRef CreateFloat(double a) {
    StaticFieldRef ret;
    ret.data_.double_data = a;
    return ret;
  }

  static StaticFieldRef CreateStringRef(const StaticStringField* a) {
    StaticFieldRef ret;
    ret.data_.str_data = a;
    return ret;
  }

  static StaticFieldRef CreateFromStringView(const std::string_view a, FieldType type) {
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

  int64_t ReadInt() const { return data_.int_data; }

  double ReadFloat() const { return data_.double_data; }

  const StaticStringField* ReadStringFieldPointer() const { return data_.str_data; }

  std::string_view ReadStringView() const { return data_.str_data->ReadStringView(); }

  std::string ReadString() const { return data_.str_data->ReadString(); }

  std::string ToString(FieldType type, [[maybe_unused]] size_t size) const {
    if (type == FieldType::INT32) {
      return fmt::format("{}", (int32_t)data_.int_data);
    } else if (type == FieldType::INT64) {
      return fmt::format("{}", (int64_t)data_.int_data);
    } else if (type == FieldType::FLOAT64) {
      return fmt::format("{}", data_.double_data);
    } else if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      return data_.str_data->ReadString();
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

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
      return data_.str_data->Write(data);
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

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
      data_.str_data = reinterpret_cast<const StaticStringField*>(data);
      return data + data_.str_data->size_;
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

  size_t Size(FieldType type) const {
    if (type == FieldType::INT32) {
      return sizeof(int32_t);
    } else if (type == FieldType::INT64) {
      return sizeof(int64_t);
    } else if (type == FieldType::FLOAT64) {
      return sizeof(double);
    } else if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      return data_.str_data->size_;
    } else {
      DB_ERR("Internal Error: Unrecognized FieldType.");
    }
  }

  static std::string_view GetView(const StaticFieldRef* a, FieldType type, size_t size) {
    if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      return a->ReadStringView();
    } else {
      // Assume Little Endian.
      return {reinterpret_cast<const char*>(a), size};
    }
  }
};

}  // namespace wing

#endif