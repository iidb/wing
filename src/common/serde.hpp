#ifndef SERDE_H_
#define SERDE_H_

#include "common/error.hpp"

#include <variant>
#include <vector>
#include <map>
#include <sstream>

namespace wing {
namespace serde {

template <typename T>
struct type_tag_t {};
template <typename T>
inline constexpr type_tag_t<T> type_tag{};

namespace detail {
// Interfaces
struct serialize_t {
  template <typename T, typename S>
  void operator()(T x, S s) {
    tag_invoke(serialize_t{}, x, s);
  }
};

struct deserialize_t {
	template <typename T, typename D>
	Result<T, typename D::Error> operator()(type_tag_t<T>, D d) {
		return tag_invoke(deserialize_t{}, type_tag<T>, d);
	}
};

// Implementations for serialize_t
template <typename S>
void tag_invoke(serialize_t, bool x, S s) {
  s.serialize_bool(x);
}

template <typename S>
void tag_invoke(serialize_t, unsigned char x, S s) {
  s.serialize_uc(x);
}

template <typename S>
void tag_invoke(serialize_t, unsigned int x, S s) {
  s.serialize_u(x);
}

template <typename S>
void tag_invoke(serialize_t, unsigned long x, S s) {
  s.serialize_ul(x);
}

template <typename S>
void tag_invoke(serialize_t, unsigned long long x, S s) {
  s.serialize_ull(x);
}

template <typename S>
void tag_invoke(serialize_t, std::string_view x, S s) {
  s.serialize_str(x);
}

template <typename S>
void tag_invoke(serialize_t, const std::string& x, S s) {
  s.serialize_str(x);
}

template <typename T, typename S>
void tag_invoke(serialize_t, const std::vector<T>& v, S s) {
  tag_invoke(serialize_t{}, v.size(), s);
  for (const T& x : v)
    tag_invoke(serialize_t{}, x, s);
}

template <typename K, typename V, typename C, typename A, typename S>
void tag_invoke(serialize_t, const std::map<K, V, C, A>& m, S s) {
  tag_invoke(serialize_t{}, m.size(), s);
  for (const auto& kv : m) {
    tag_invoke(serialize_t{}, kv.first, s);
    tag_invoke(serialize_t{}, kv.second, s);
  }
}

// Implementations for deserialize_t
template <typename D>
auto tag_invoke(
  deserialize_t, type_tag_t<bool>, D d
) -> Result<bool, typename D::Error> {
  return d.deserialize_bool();
}

template <typename D>
auto tag_invoke(
  deserialize_t, type_tag_t<unsigned char>, D d
) -> Result<unsigned char, typename D::Error> {
  return d.deserialize_uc();
}

template <typename D>
auto tag_invoke(
  deserialize_t, type_tag_t<unsigned int>, D d
) -> Result<unsigned int, typename D::Error> {
  return d.deserialize_u();
}

template <typename D>
auto tag_invoke(
  deserialize_t, type_tag_t<unsigned long>, D d
) -> Result<unsigned long, typename D::Error> {
  return d.deserialize_ul();
}

template <typename D>
auto tag_invoke(
  deserialize_t, type_tag_t<unsigned long long>, D d
) -> Result<unsigned long long, typename D::Error> {
  return d.deserialize_ull();
}

template <typename D>
auto tag_invoke(
  deserialize_t, type_tag_t<std::string>, D d
) -> Result<std::string, typename D::Error> {
  return d.deserialize_string();
}

template <typename T, typename D>
auto tag_invoke(
  deserialize_t, type_tag_t<std::vector<T>>, D d
) -> Result<std::vector<T>, typename D::Error> {
  size_t size =
    EXTRACT_RESULT(tag_invoke(deserialize_t{}, type_tag<size_t>, d));
  std::vector<T> v;
  v.reserve(size);
  for (; size; size -= 1)
    v.push_back(EXTRACT_RESULT(tag_invoke(deserialize_t{}, type_tag<T>, d)));
  return v;
}

template <typename K, typename V, typename C, typename A, typename D>
auto tag_invoke(
  deserialize_t, type_tag_t<std::map<K, V, C, A>>, D d
) -> Result<std::map<K, V, C, A>, typename D::Error> {
  size_t size =
    EXTRACT_RESULT(tag_invoke(deserialize_t{}, type_tag<size_t>, d));
  std::map<K, V, C, A> m;
  for (; size; size -= 1) {
    K key = EXTRACT_RESULT(tag_invoke(deserialize_t{}, type_tag<K>, d));
    V val = EXTRACT_RESULT(tag_invoke(deserialize_t{}, type_tag<V>, d));
    auto ret = m.insert(std::make_pair(std::move(key), std::move(val)));
    crash_if(ret.second == false,
      "Deserialize std::map<K, V>: Repeated key on disk");
  }
  return m;
}
} // namespace detail
template <auto& Tag>
using tag_t = std::decay_t<decltype(Tag)>;

inline detail::serialize_t serialize{};
inline detail::deserialize_t deserialize{};

namespace bin_stream {
class Serializer {
 public:
  Serializer(std::ostream& out) : out_(out) {}
  void serialize_bool(bool x) {
    serialize_fixed(x);
  }
  void serialize_uc(unsigned char x) {
    serialize_fixed(x);
  }
  void serialize_u(unsigned int x) {
    serialize_fixed(x);
  }
  void serialize_ul(unsigned long x) {
    serialize_fixed(x);
  }
  void serialize_ull(unsigned long long x) {
    serialize_fixed(x);
  }
  void serialize_str(std::string_view x) {
    serialize_fixed(x.size());
    out_.write(x.data(), x.size());
  }
 private:
  template <typename T>
  void serialize_fixed(T x) {
    out_.write(reinterpret_cast<const char *>(&x), sizeof(x));
  }
  std::ostream& out_;
};

class Deserializer {
 public:
  typedef io::Error Error;
  Deserializer(std::istream& in) : in_(in) {}
  Result<bool, Error> deserialize_bool() {
    return deserialize_fixed<bool>();
  }
  Result<unsigned char, Error> deserialize_uc() {
    return deserialize_fixed<unsigned char>();
  }
  Result<unsigned int, Error> deserialize_u() {
    return deserialize_fixed<unsigned int>();
  }
  Result<unsigned long, Error> deserialize_ul() {
    return deserialize_fixed<unsigned long>();
  }
  Result<unsigned long long, Error> deserialize_ull() {
    return deserialize_fixed<unsigned long long>();
  }
  Result<std::string, Error> deserialize_string() {
    size_t size = EXTRACT_RESULT(deserialize(type_tag<size_t>, *this));
    // Any way to avoid the redundant clearing?
    std::string x(size, 0);
    in_.read(reinterpret_cast<char *>(x.data()), size);
    if (!in_.good())
      return Error::New(io::ErrorKind::Other,
        "stream::Deserializer::deserialize_string failed");
    return x;
  }
  Result<std::vector<uint8_t>, Error> deserialize_bytes() {
    size_t size = EXTRACT_RESULT(deserialize(type_tag<size_t>, *this));
    std::vector<uint8_t> x(size);
    in_.read(reinterpret_cast<char *>(x.data()), size);
    if (!in_.good())
      return Error::New(io::ErrorKind::Other,
        "stream::Deserializer::deserialize_bytes failed");
    return x;
  }
 private:
  template <typename T>
  Result<T, Error> deserialize_fixed() {
    T x;
    in_.read(reinterpret_cast<char *>(&x), sizeof(x));
    if (!in_.good())
      return Error::New(io::ErrorKind::Other, "stream::Deserializer failed");
    return x;
  }
  std::istream& in_;
};

template <typename T>
std::string to_string(T x) {
  std::ostringstream out;
  serde::serialize(x, Serializer(out));
  return std::move(out).str();
}
template <typename T>
Result<T, Deserializer::Error> from_string(std::string&& x) {
  std::istringstream in(std::move(x));
  return tag_invoke(serde::tag_t<serde::deserialize>{}, type_tag<T>, Deserializer(in));
}
} // namespace bin_stream

} // namespace serde
} // namespace wing

#endif // SERDE_H_
