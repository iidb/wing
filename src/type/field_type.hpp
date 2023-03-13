#ifndef SAKURA_FIELD_TYPE_H__
#define SAKURA_FIELD_TYPE_H__

#include "common/serde.hpp"

namespace wing {

enum class FieldType : uint8_t { INT32 = 0, INT64, FLOAT64, CHAR, VARCHAR, EMPTY };

template <typename S>
void tag_invoke(serde::tag_t<serde::serialize>, FieldType x, S s) {
  s.serialize_u8(static_cast<uint8_t>(x));
}

namespace serde {

template <>
struct Deserialize<wing::FieldType> {
	template <typename D>
	static Result<wing::FieldType, typename D::Error> deserialize(D d) {
		return static_cast<wing::FieldType>(EXTRACT_RESULT(d.deserialize_u8()));
	}
};

} // namespace serde
} // namespace wing

#endif