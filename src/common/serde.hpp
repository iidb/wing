#ifndef SERDE_H_
#define SERDE_H_

#include "common/error.hpp"

#include <variant>
#include <vector>
#include <map>
#include <sstream>

namespace wing {
namespace serde {
namespace detail {
// Interfaces
struct serialize_t {
  template <typename T, typename S>
  void operator()(T x, S s) {
    tag_invoke(serialize_t{}, x, s);
  }
};
template <typename T, typename S>
void tag_invoke(serialize_t, T x, S s);

// Implementations for serialize_t
template <typename S>
void tag_invoke(serialize_t, bool x, S s) {
  s.serialize_bool(x);
}

template <typename S>
void tag_invoke(serialize_t, uint32_t x, S s) {
  s.serialize_u32(x);
}

template <typename S>
void tag_invoke(serialize_t, uint64_t x, S s) {
  s.serialize_u64(x);
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
  s.serialize_u64(v.size());
  for (const T& x : v)
    tag_invoke(serialize_t{}, x, s);
}

template <typename K, typename V, typename C, typename A, typename S>
void tag_invoke(serialize_t, const std::map<K, V, C, A>& m, S s) {
  s.serialize_u64(m.size());
  for (const auto& kv : m) {
    tag_invoke(serialize_t{}, kv.first, s);
    tag_invoke(serialize_t{}, kv.second, s);
  }
}
} // namespace detail
template <auto& Tag>
using tag_t = std::decay_t<decltype(Tag)>;

inline detail::serialize_t serialize{};

template <typename T>
struct Deserialize {
  template <typename D>
  static Result<T, typename D::Error> deserialize(D d);
};

template <>
struct Deserialize<bool> {
  template <typename D>
  static Result<bool, typename D::Error> deserialize(D d) {
    return d.deserialize_bool();
  }
};

template <>
struct Deserialize<uint32_t> {
  template <typename D>
  static Result<uint32_t, typename D::Error> deserialize(D d) {
    return d.deserialize_u32();
  }
};

template <>
struct Deserialize<uint64_t> {
  template <typename D>
  static Result<uint64_t, typename D::Error> deserialize(D d) {
    return d.deserialize_u64();
  }
};

template <>
struct Deserialize<std::string> {
  template <typename D>
  static Result<std::string, typename D::Error> deserialize(D d) {
    return d.deserialize_string();
  }
};

template <typename T>
struct Deserialize<std::vector<T>> {
  template <typename D>
  static Result<std::vector<T>, typename D::Error> deserialize(D d) {
    size_t size = EXTRACT_RESULT(Deserialize<uint64_t>::deserialize(d));
    std::vector<T> v;
    v.reserve(size);
    for (; size; size -= 1)
      v.push_back(EXTRACT_RESULT(Deserialize<T>::deserialize(d)));
    return v;
  }
};

template <typename K, typename V, typename C, typename A>
struct Deserialize<std::map<K, V, C, A>> {
  template <typename D>
  static Result<std::map<K, V, C, A>, typename D::Error> deserialize(D d) {
    size_t size = EXTRACT_RESULT(Deserialize<uint64_t>::deserialize(d));
    std::map<K, V, C, A> m;
    for (; size; size -= 1) {
      K key = EXTRACT_RESULT(Deserialize<K>::deserialize(d));
      V val = EXTRACT_RESULT(Deserialize<V>::deserialize(d));
      auto ret = m.insert(std::make_pair(std::move(key), std::move(val)));
      crash_if(ret.second == false,
        "Deserialize<std::map<K, V>>: Repeated key on disk");
    }
    return m;
  }
};

namespace bin_stream {
class Serializer {
 public:
  Serializer(std::ostream& out) : out_(out) {}
  void serialize_bool(bool x) {
    serialize_fixed(x);
  }
  void serialize_u8(uint8_t x) {
    serialize_fixed(x);
  }
  void serialize_u32(uint32_t x) {
    serialize_fixed(x);
  }
  void serialize_u64(uint64_t x) {
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
  Result<uint8_t, Error> deserialize_u8() {
    return deserialize_fixed<uint8_t>();
  }
  Result<uint32_t, Error> deserialize_u32() {
    return deserialize_fixed<uint32_t>();
  }
  Result<uint64_t, Error> deserialize_u64() {
    return deserialize_fixed<uint64_t>();
  }
  Result<std::string, Error> deserialize_string() {
    size_t size = EXTRACT_RESULT(deserialize_u64());
    // Any way to avoid the redundant clearing?
    std::string x(size, 0);
    in_.read(reinterpret_cast<char *>(x.data()), size);
    if (!in_.good())
      return Error::New(io::ErrorKind::Other,
        "stream::Deserializer::deserialize_string failed");
    return x;
  }
  Result<std::vector<uint8_t>, Error> deserialize_bytes() {
    size_t size = EXTRACT_RESULT(deserialize_u64());
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
  return Deserialize<T>::deserialize(Deserializer(in));
}
} // namespace bin_stream

} // namespace serde
} // namespace wing

#endif // SERDE_H_
