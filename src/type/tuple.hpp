#ifndef SAKURA_TUPLE_H__
#define SAKURA_TUPLE_H__

#include <memory>

#include "catalog/schema.hpp"
#include "common/allocator.hpp"
#include "type/field_type.hpp"
#include "type/static_field.hpp"

namespace wing {
/** Tuple layout:
 *  ----------------------------------------------------------------------------------------------------------------
 *  invariant size fields (static fields) | offsets (4b) of variant size fields (varchar) | varchars
 *  -----------------------------------------------------------------------------------------------------------------
 *
 * Varchar (StaticStringField) layout:
 *  -----------------------------------------------------------------------------
 *  string size (4 byte) = string length + 4 | string
 *  -----------------------------------------------------------------------------
 *
 * For example: tuple (2(int32), 3.0(float64), "fay"(varchar))
 *  ----------------------------------------------------------
 *  2 (4b) | 3.0 (4b) | 16 (4b) | 7 (4b) | fay (3b)
 *  ---------------------------------------------------------
 *
 * */
class Tuple {
 public:
  static uint32_t GetOffsetsOfStrings(uint32_t sum_of_static_fields, uint32_t str_id) { return sum_of_static_fields + str_id * sizeof(uint32_t); }
  static uint32_t GetOffsetOfStaticField(uint32_t sum_of_static_fields_before) { return sum_of_static_fields_before; }
  static uint32_t GetTupleSize(const void* data_ptr, uint32_t sum_of_static_fields) {
    auto u8_ptr = reinterpret_cast<const uint8_t*>(data_ptr);
    auto str0offset = *reinterpret_cast<const uint32_t*>(u8_ptr + GetOffsetsOfStrings(sum_of_static_fields, 0));
    auto last_offset = *reinterpret_cast<const uint32_t*>(u8_ptr + str0offset - sizeof(uint32_t));
    return *reinterpret_cast<const uint32_t*>(u8_ptr + last_offset) + last_offset;
  }
  static uint32_t GetSizeOfAllStrings(const void* data_ptr, uint32_t sum_of_static_fields) {
    auto u8_ptr = reinterpret_cast<const uint8_t*>(data_ptr);
    auto str0offset = *reinterpret_cast<const uint32_t*>(u8_ptr + GetOffsetsOfStrings(sum_of_static_fields, 0));
    auto last_offset = *reinterpret_cast<const uint32_t*>(u8_ptr + str0offset - sizeof(uint32_t));
    return *reinterpret_cast<const uint32_t*>(u8_ptr + last_offset) + last_offset - str0offset;
  }
  // Get the size serialized from an array of StaticFieldRef.
  static uint32_t GetSerializeSize(const void* vec_data, auto&& columns_schema) {
    uint32_t size = 0;
    auto vec = reinterpret_cast<const StaticFieldRef*>(vec_data);
    for (uint32_t index = 0; auto& a : columns_schema) {
      size += vec[index].Size(a.type_);
      if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
        size += sizeof(uint32_t);
      }
      index += 1;
    }
    return size;
  }
  static void Serialize(void* data_ptr, const void* vec_data, auto&& storage_cols, auto&& shuffle) {
    auto in = reinterpret_cast<uint8_t*>(data_ptr);
    auto vec = reinterpret_cast<const StaticFieldRef*>(vec_data);
    uint32_t offset = 0;
    for (uint32_t _index = 0; auto& a : storage_cols) {
      auto index = shuffle[_index];
      if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
        // Size of offset table
        offset += sizeof(uint32_t) * (storage_cols.size() - _index);
        // Get the offsets
        for (auto _temp_index = _index; _temp_index < storage_cols.size(); _temp_index += 1) {
          auto temp_index = shuffle[_temp_index];
          *reinterpret_cast<uint32_t*>(in) = offset;
          offset += vec[temp_index].Size(FieldType::VARCHAR);
          in += sizeof(uint32_t);
        }
        // Write the strings
        for (auto _temp_index = _index; _temp_index < storage_cols.size(); _temp_index += 1) {
          auto temp_index = shuffle[_temp_index];
          auto& a = storage_cols[_temp_index];
          // DB_INFO("{}", temp_index);
          // DB_INFO("{}", vec[temp_index].ReadStringFieldPointer()->size_);
          // Check the length
          if (a.size_ < vec[temp_index].ReadStringFieldPointer()->Length()) {
            throw DBException("String length exceeds limit {}.", a.size_);
          }
          in = vec[temp_index].Write(a.type_, a.size_, in);
        }
        break;
      }
      offset += vec[index].Size(a.type_);
      in = vec[index].Write(a.type_, a.size_, in);
      _index += 1;
    }
  }
  static void DeSerialize(void* output_ptr, const void* data_ptr, auto&& columns_schema) {
    auto out = reinterpret_cast<uint8_t*>(output_ptr);
    auto in = reinterpret_cast<const uint8_t*>(data_ptr);
    uint32_t offset = 0;
    uint32_t str_id = 0;
    for (uint32_t i = 0; const auto& a : columns_schema) {
      if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
        reinterpret_cast<StaticFieldRef*>(out)[i].Read(a.type_, a.size_,
                                                       in + *reinterpret_cast<const uint32_t*>(in + Tuple::GetOffsetsOfStrings(offset, str_id)));
        str_id += 1;
      } else {
        reinterpret_cast<StaticFieldRef*>(out)[i].Read(a.type_, a.size_, in + Tuple::GetOffsetOfStaticField(offset));
        offset += a.size_;
      }
      i += 1;
    }
  }
  static std::string_view GetFieldView(const void* data_ptr, uint32_t offset, FieldType type, uint32_t size) {
    auto in = reinterpret_cast<const uint8_t*>(data_ptr);
    if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      auto offset_pos = *reinterpret_cast<const uint32_t*>(in + offset);
      return StaticFieldRef::CreateStringRef(reinterpret_cast<const StaticStringField*>(in + offset_pos)).ReadStringView();
    } else {
      return {reinterpret_cast<const char*>(in + offset), size};
    }
  }
  static uint32_t GetOffset(uint32_t index, auto&& storage_column_schema) {
    uint32_t sum = 0;
    int32_t str_id = 0;
    for (uint32_t i = 0; i < index; i++) {
      if (storage_column_schema[i].type_ == FieldType::CHAR || storage_column_schema[i].type_ == FieldType::VARCHAR) {
        str_id += 1;
      } else {
        sum += storage_column_schema[i].size_;
      }
    }
    if (storage_column_schema[index].type_ == FieldType::CHAR || storage_column_schema[index].type_ == FieldType::VARCHAR) {
      return GetOffsetsOfStrings(sum, str_id);
    } else {
      return sum;
    }
  }
};

}  // namespace wing

#endif