#pragma once

#include "common/murmurhash.hpp"

namespace wing {

namespace utils {

class BloomFilter {
 public:
  static size_t BloomHash(std::string_view key) {
    return Hash(key.data(), key.size(), 0x1145141919810);
  }

  /* Create a bloom filter buffer */
  static void Create(
      size_t key_n, size_t bits_per_key, std::string& bloom_bits);

  /* Add a key to the bloom filter */
  static void Add(std::string_view key, std::string& bloom_bits);

  /* Add a key hash (i.e. BloomHash(key)) to the bloom filter */
  static void Add(size_t hash1, std::string& bloom_bits);

  /* Check if a key may be added */
  static bool Find(std::string_view key, std::string_view bloom_bits);

  /* Check if a key (i.e. BloomHash(key)) may be added */
  static bool Find(size_t hash1, std::string_view bloom_bits);
};

}  // namespace utils

}  // namespace wing
