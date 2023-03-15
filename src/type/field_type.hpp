#ifndef SAKURA_FIELD_TYPE_H__
#define SAKURA_FIELD_TYPE_H__

#include "common/serde.hpp"

namespace wing {

enum class FieldType : uint8_t { INT32 = 0, INT64, FLOAT64, CHAR, VARCHAR, EMPTY };

template <typename S>
void tag_invoke(serde::tag_t<serde::serialize>, FieldType x, S s) {
	serde::serialize(static_cast<uint8_t>(x), s);
}

template <typename D>
auto tag_invoke(
	serde::tag_t<serde::deserialize>, serde::type_tag_t<wing::FieldType>, D d
) -> Result<wing::FieldType, typename D::Error> {
	return static_cast<wing::FieldType>(EXTRACT_RESULT(
		serde::deserialize(serde::type_tag<uint8_t>, d)));
}

} // namespace wing

#endif