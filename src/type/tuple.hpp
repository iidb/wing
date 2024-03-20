#pragma once

#include <memory>

#include "catalog/schema.hpp"
#include "common/allocator.hpp"
#include "type/field_type.hpp"
#include "type/static_field.hpp"

namespace wing {
/**
 * Tuple layout: We ensure that all varchars are stored behind other fields.
 *  ----------------------------------------------------------------------------------------------------------------
 *  invariant size fields (static fields) | string offset table (each offset 4b)
 * | varchars
 *  -----------------------------------------------------------------------------------------------------------------
 * This is how the column are stored physically, the logical schema is different
 * from it. For example, for table A (a int64, c varchar(20), b int64), the
 * logical schema is (int64, varchar(20), int64) (that means if you want to
 * insert into it, you must ensure that the 1st field and 3rd field are int64,
 * and 2nd is varchar). But the physical schema is (int64, int64, varchar(20)).
 *
 * You can learn more about that by reading Tuple::Serialize and
 * Tuple::Deserialize, InsertExecutor, DeleteExecutor.
 *
 * Varchar (StaticStringField) layout:
 *  -----------------------------------------------------------------------------
 *  string size (4 byte) = string length + 4 | string
 *  -----------------------------------------------------------------------------
 *
 * For example: tuple (2(int32), 3.0(float64), "fay"(varchar))
 *  ----------------------------------------------------------
 *  2 (4b) | 3.0 (4b) | 12 (4b) | 7 (4b) | fay (3b)
 *  ---------------------------------------------------------
 *
 * */
class Tuple {
 public:
  static uint32_t GetOffsetsOfStrings(
      uint32_t sum_of_static_fields, uint32_t str_id) {
    return sum_of_static_fields + str_id * sizeof(uint32_t);
  }
  static uint32_t GetOffsetOfStaticField(uint32_t sum_of_static_fields_before) {
    return sum_of_static_fields_before;
  }
  static uint32_t GetTupleSize(
      const void* data_ptr, uint32_t sum_of_static_fields) {
    auto u8_ptr = reinterpret_cast<const uint8_t*>(data_ptr);
    auto str0offset = *reinterpret_cast<const uint32_t*>(
        u8_ptr + GetOffsetsOfStrings(sum_of_static_fields, 0));
    auto last_offset = *reinterpret_cast<const uint32_t*>(
        u8_ptr + str0offset - sizeof(uint32_t));
    return *reinterpret_cast<const uint32_t*>(u8_ptr + last_offset) +
           last_offset;
  }
  static uint32_t GetSizeOfAllStrings(
      const void* data_ptr, uint32_t sum_of_static_fields) {
    auto u8_ptr = reinterpret_cast<const uint8_t*>(data_ptr);
    auto str0offset = *reinterpret_cast<const uint32_t*>(
        u8_ptr + GetOffsetsOfStrings(sum_of_static_fields, 0));
    auto last_offset = *reinterpret_cast<const uint32_t*>(
        u8_ptr + str0offset - sizeof(uint32_t));
    return *reinterpret_cast<const uint32_t*>(u8_ptr + last_offset) +
           last_offset - str0offset;
  }
  // Get the serialized size of an array of StaticFieldRef.
  static uint32_t GetSerializeSize(
      const void* vec_data, auto&& columns_schema) {
    uint32_t size = 0;
    auto vec = reinterpret_cast<const StaticFieldRef*>(vec_data);
    for (uint32_t index = 0; auto& a : columns_schema) {
      size += vec[index].size(a.type_, a.size_);
      if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
        size += sizeof(uint32_t);
      }
      index += 1;
    }
    return size;
  }
  // data_ptr is the dst.
  // vec_data is the src, pointing to an array of StaticFieldRef.
  // storage_cols is the storage column schema.
  // shuffle is a mapping from storage column index to logical column index.
  static void Serialize(void* data_ptr, const void* vec_data,
      auto&& storage_cols, auto&& shuffle) {
    auto in = reinterpret_cast<uint8_t*>(data_ptr);
    auto vec = reinterpret_cast<const StaticFieldRef*>(vec_data);
    uint32_t offset = 0;
    for (uint32_t _index = 0; auto& a : storage_cols) {
      auto index = shuffle[_index];
      if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
        // We ensure that fields after it are all VARCHARs or CHARs (now CHAR is
        // considered as VARCHAR). size of offset table
        offset += sizeof(uint32_t) * (storage_cols.size() - _index);
        // Get the offsets
        for (auto _temp_index = _index; _temp_index < storage_cols.size();
             _temp_index += 1) {
          auto temp_index = shuffle[_temp_index];
          *reinterpret_cast<uint32_t*>(in) = offset;
          offset += vec[temp_index].size(FieldType::VARCHAR, 0);
          in += sizeof(uint32_t);
        }
        // Write the strings
        for (auto _temp_index = _index; _temp_index < storage_cols.size();
             _temp_index += 1) {
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
      offset += vec[index].size(a.type_, a.size_);
      in = vec[index].Write(a.type_, a.size_, in);
      _index += 1;
    }
  }
  // output_ptr is the dst, pointing to an array of StaticFieldRef.
  // data_ptr is the src.
  // columns_schema is the storage column schema.
  static void DeSerialize(
      void* output_ptr, const void* data_ptr, auto&& columns_schema) {
    auto out = reinterpret_cast<uint8_t*>(output_ptr);
    auto in = reinterpret_cast<const uint8_t*>(data_ptr);
    uint32_t offset = 0;
    uint32_t str_id = 0;
    for (uint32_t i = 0; const auto& a : columns_schema) {
      if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
        reinterpret_cast<StaticFieldRef*>(out)[i].Read(a.type_, a.size_,
            in + *reinterpret_cast<const uint32_t*>(
                     in + Tuple::GetOffsetsOfStrings(offset, str_id)));
        str_id += 1;
      } else {
        reinterpret_cast<StaticFieldRef*>(out)[i].Read(
            a.type_, a.size_, in + Tuple::GetOffsetOfStaticField(offset));
        offset += a.size_;
      }
      i += 1;
    }
  }
  // Get std::string_view of a field in raw data.
  // data_ptr is the pointer to raw data.
  // offset is either offset of invariant size field, or offset in string offset
  // table. type is the type of the field. For convenient, the size of invariant
  // size field is passed. Although it can be calculated through type.
  static std::string_view GetFieldView(
      const void* data_ptr, uint32_t offset, FieldType type, uint32_t size) {
    auto in = reinterpret_cast<const uint8_t*>(data_ptr);
    if (type == FieldType::CHAR || type == FieldType::VARCHAR) {
      auto offset_pos = *reinterpret_cast<const uint32_t*>(in + offset);
      return StaticFieldRef::CreateStringRef(
          reinterpret_cast<const StaticStringField*>(in + offset_pos))
          .ReadStringView();
    } else {
      return {reinterpret_cast<const char*>(in + offset), size};
    }
  }
  // Get offset of a field in raw data.
  // index is the index of the field in storage column schema. See
  // DeleteExecutor.
  static uint32_t GetOffset(uint32_t index, auto&& storage_column_schema) {
    uint32_t sum = 0;
    int32_t str_id = 0;
    for (uint32_t i = 0; i < index; i++) {
      if (storage_column_schema[i].type_ == FieldType::CHAR ||
          storage_column_schema[i].type_ == FieldType::VARCHAR) {
        str_id += 1;
      } else {
        sum += storage_column_schema[i].size_;
      }
    }
    if (storage_column_schema[index].type_ == FieldType::CHAR ||
        storage_column_schema[index].type_ == FieldType::VARCHAR) {
      return GetOffsetsOfStrings(sum, str_id);
    } else {
      return sum;
    }
  }
};

}  // namespace wing
