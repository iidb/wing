#pragma once

#include <cstring>
#include <memory>

namespace wing {

namespace utils {

class Serializer {
 public:
  Serializer(void* ptr) : ptr_(reinterpret_cast<char*>(ptr)) {}

  Serializer& WriteString(std::string_view s) {
    memcpy(ptr_, s.data(), s.size());
    ptr_ += s.size();
    return *this;
  }

  template <typename T>
  Serializer& Write(T t) {
    *reinterpret_cast<T*>(ptr_) = t;
    ptr_ += sizeof(T);
    return *this;
  }

  char* data() const { return ptr_; }

 private:
  char* ptr_;
};

class Deserializer {
 public:
  Deserializer(const void* ptr) : ptr_(reinterpret_cast<const char*>(ptr)) {}

  std::string ReadString(size_t len) {
    auto ret = std::string(ptr_, len);
    ptr_ += len;
    return ret;
  }

  template <typename T>
  T Read() {
    T ret = *reinterpret_cast<const T*>(ptr_);
    ptr_ += sizeof(T);
    return ret;
  }

  const char* data() const { return ptr_; }

 private:
  const char* ptr_;
};

}  // namespace utils

}  // namespace wing
