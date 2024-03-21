#include "common/bloomfilter.hpp"

#include "common/serializer.hpp"

namespace wing {

namespace utils {

void BloomFilter::Create(
    size_t key_n, size_t bits_per_key, std::string& bloom_bits) {
  size_t bits = key_n * bits_per_key;
  bits = std::max<size_t>(64, bits);
  size_t bytes = (bits + 7) / 8;
  bloom_bits.resize(bytes + sizeof(uint64_t) * 3, 0);
  utils::Serializer(bloom_bits.data())
      .Write<uint64_t>(bits)
      .Write<uint64_t>(key_n)
      .Write<uint64_t>(bits_per_key);
}

void BloomFilter::Add(std::string_view key, std::string& bloom_bits) {
  size_t h = BloomHash(key);
  Add(h, bloom_bits);
}

void BloomFilter::Add(size_t h, std::string& bloom_bits) {
  auto des = utils::Deserializer(bloom_bits.data());
  size_t bits = des.Read<uint64_t>();
  size_t key_n = des.Read<uint64_t>();
  size_t bits_per_key = des.Read<uint64_t>();
  size_t hash_num =
      std::min<size_t>(30, std::max<size_t>(1, bits_per_key * 0.69));
  auto* array = const_cast<char*>(des.data());
  // use the double-hashing in leveldb, i.e. h1 + i * h2
  const size_t delta = Hash8(h, 0x202403211957);
  size_t bitpos = h % bits, dpos = delta % bits;
  for (size_t j = 0; j < hash_num; j++) {
    array[bitpos / 8] |= (1 << (bitpos & 7));
    bitpos += dpos, bitpos >= bits ? bitpos -= bits : 0;
  }
}

bool BloomFilter::Find(std::string_view key, std::string_view bloom_bits) {
  size_t h = BloomHash(key);
  return Find(h, bloom_bits);
}

bool BloomFilter::Find(size_t h, std::string_view bloom_bits) {
  auto des = utils::Deserializer(bloom_bits.data());
  size_t bits = des.Read<uint64_t>();
  size_t key_n = des.Read<uint64_t>();
  size_t bits_per_key = des.Read<uint64_t>();
  size_t hash_num =
      std::min<size_t>(30, std::max<size_t>(1, bits_per_key * 0.69));
  auto* array = des.data();
  // use the double-hashing in leveldb, i.e. h_k = h1 + k * h2
  const size_t delta = Hash8(h, 0x202403211957);
  size_t bitpos = h % bits, dpos = delta % bits;
  for (size_t j = 0; j < hash_num; j++) {
    if (!(array[bitpos / 8] & (1 << (bitpos & 7)))) {
      return false;
    }
    bitpos += dpos, bitpos >= bits ? bitpos -= bits : 0;
  }
  return true;
}

}  // namespace utils

}  // namespace wing
