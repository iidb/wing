#pragma once

#include "common/exception.hpp"
#include "common/serde.hpp"

namespace wing {

enum class FieldType : uint8_t {
  INT32 = 0,
  INT64,
  FLOAT64,
  CHAR,
  VARCHAR,
  EMPTY
};

/**
 * LogicalType can be converted to FieldType:
 *  LogicalType::INT - FieldType::INT64
 *  LogicalType::FLOAT - FieldType::FLOAT64
 *  LogicalType::STRING - FieldType::VARCHAR
 */
enum class LogicalType : uint8_t { INT = 0, FLOAT, STRING };

inline size_t GetTypeSize(LogicalType t) {
  switch (t) {
    case LogicalType::INT:
      return 8;
    case LogicalType::FLOAT:
      return 8;
    case LogicalType::STRING:
      return 8;
    default:
      throw DBException("Unrecognized LogicalType {}!", size_t(t));
  }
}

template <typename S>
void tag_invoke(serde::tag_t<serde::serialize>, FieldType x, S s) {
  serde::serialize(static_cast<uint8_t>(x), s);
}

template <typename D>
auto tag_invoke(
    serde::tag_t<serde::deserialize>, serde::type_tag_t<wing::FieldType>, D d)
    -> Result<wing::FieldType, typename D::Error> {
  return static_cast<wing::FieldType>(
      EXTRACT_RESULT(serde::deserialize(serde::type_tag<uint8_t>, d)));
}

}  // namespace wing
