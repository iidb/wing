#pragma once

#include <memory>
#include <string>

namespace wing {

namespace utils {

// https://github.com/hhrhhr/MurmurHash-for-Lua/blob/master/MurmurHash64A.c
size_t Hash(const char* _data, size_t n, size_t seed);

size_t Hash(std::string_view str, size_t seed);

size_t Hash8(const void* _data, size_t seed);

size_t Hash8(size_t data, size_t seed);

}  // namespace utils

}  // namespace wing
