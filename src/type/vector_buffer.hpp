#pragma once

#include "common/allocator.hpp"
#include "type/field_type.hpp"
#include "type/static_field.hpp"

namespace wing {

enum class VectorBufferType {
  Standard = 0,  // standard buffer, holds a single array of data
  String,  // string buffer, holds an array of string pointers, and an extra
           // string buffer.
};

/**
 * The actual data structure stores the data
 * */
class VectorBuffer {
 public:
  VectorBuffer(VectorBufferType type) : type_(type) {}

  VectorBuffer(VectorBufferType type, std::unique_ptr<uint8_t[]> data)
    : type_(type), data_(std::move(data)) {}

  VectorBuffer(VectorBufferType type, size_t size)
    : type_(type), data_(new uint8_t[size]) {}

  static std::shared_ptr<VectorBuffer> CreateStandardBuffer(
      LogicalType type, size_t size) {
    return std::make_shared<VectorBuffer>(
        VectorBufferType::Standard, GetTypeSize(type) * size);
  }

  static std::shared_ptr<VectorBuffer> CreateConstantBuffer(LogicalType type) {
    return std::make_shared<VectorBuffer>(
        VectorBufferType::Standard, GetTypeSize(type));
  }

  uint8_t* Data() { return data_.get(); }

 private:
  VectorBufferType type_;
  std::unique_ptr<uint8_t[]> data_;
};

/**
 * The data structure stores string data.
 */
class StringVectorBuffer : public VectorBuffer {
 public:
  StringVectorBuffer() : VectorBuffer(VectorBufferType::String) {}

  static std::shared_ptr<StringVectorBuffer> Create() {
    return std::make_shared<StringVectorBuffer>();
  }

  StaticFieldRef AddString(StaticFieldRef str) {
    auto ptr = alloc_.Allocate(str.size(FieldType::VARCHAR, 0));
    StaticStringField::Copy(ptr, str.ReadStringFieldPointer());
    return reinterpret_cast<const StaticStringField*>(ptr);
  }

  StaticFieldRef AddString(std::string_view str) {
    auto ptr = alloc_.Allocate(str.length() + sizeof(uint32_t));
    StaticStringField::Write(ptr, str.data(), str.size());
    return reinterpret_cast<const StaticStringField*>(ptr);
  }

 private:
  ArenaAllocator alloc_;
};

};  // namespace wing
