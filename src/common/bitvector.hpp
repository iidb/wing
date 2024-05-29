#pragma once

#include <cstring>
#include <memory>

#include "common/logging.hpp"

namespace wing {

/**
 * Used to store a variant-length bit vector.
 * But the length of the bit vector is known when it is created, so we don't
 * need resize. Support &, |, ^. If two bitvectors are of different length, the
 * result is of the maximum length. Initially, all the bits are 0.
 */
class BitVector {
 public:
  const static uint32_t kBitSize = 64;
  class BitRef {
   public:
    BitRef(uint64_t& mem, uint32_t bit) : mem_(mem), bit_(bit) {}
    bool operator=(const bool b) {
      mem_ = mem_ ^ (uint64_t(bool(*this) ^ b) << bit_);
      return b;
    }
    operator bool() const { return (mem_ >> bit_) & 1; }

   private:
    uint64_t& mem_;
    uint32_t bit_;
  };
  BitVector() : BitVector(1) {}
  BitVector(uint32_t size)
    : size_(size),
      mem_(
          std::unique_ptr<uint64_t[]>(new uint64_t[_get_alloc_size64(size_)])) {
    alloc_size_ = _get_alloc_size64(size_) * kBitSize / 8;
    std::memset(mem_.get(), 0, alloc_size_);
  }
  BitVector(const BitVector& v)
    : size_(v.size_),
      mem_(std::unique_ptr<uint64_t[]>(
          new uint64_t[_get_alloc_size64(v.size_)])) {
    alloc_size_ = _get_alloc_size64(size_) * kBitSize / 8;
    std::memcpy(mem_.get(), v.mem_.get(), alloc_size_);
  }
  BitVector(BitVector&& v) : size_(v.size_), mem_(std::move(v.mem_)) {}
  BitVector& operator=(const BitVector& v) {
    size_ = v.size_;
    mem_ = std::unique_ptr<uint64_t[]>(new uint64_t[_get_alloc_size64(size_)]);
    alloc_size_ = _get_alloc_size64(size_) * kBitSize / 8;
    std::memcpy(mem_.get(), v.mem_.get(), alloc_size_);
    return *this;
  }

  BitVector& operator=(BitVector&& v) {
    size_ = v.size_;
    mem_ = std::move(v.mem_);
    alloc_size_ = v.alloc_size_;
    return *this;
  }

  BitRef operator[](uint32_t pos) {
    return {*(mem_.get() + pos / kBitSize), pos % kBitSize};
  }
  const BitRef operator[](uint32_t pos) const {
    return {*(mem_.get() + pos / kBitSize), pos % kBitSize};
  }
  BitVector operator&(const BitVector& v) const {
    BitVector ret(std::max(size_, v.size_));
    for (uint32_t i = 0; i < _get_alloc_size64(std::min(size_, v.size_)); i++) {
      ret.mem_.get()[i] = mem_.get()[i] & v.mem_.get()[i];
    }
    return ret;
  }
  BitVector operator|(const BitVector& v) const {
    if (size_ <= v.size_) {
      BitVector ret(v.size_);
      for (uint32_t i = 0; i < _get_alloc_size64(size_); i++) {
        ret.mem_.get()[i] = mem_.get()[i] | v.mem_.get()[i];
      }
      for (uint32_t i = _get_alloc_size64(size_);
           i < _get_alloc_size64(v.size_); i++) {
        ret.mem_.get()[i] = v.mem_.get()[i];
      }
      return ret;
    } else {
      BitVector ret(size_);
      for (uint32_t i = 0; i < _get_alloc_size64(v.size_); i++) {
        ret.mem_.get()[i] = mem_.get()[i] | v.mem_.get()[i];
      }
      for (uint32_t i = _get_alloc_size64(v.size_);
           i < _get_alloc_size64(size_); i++) {
        ret.mem_.get()[i] = mem_.get()[i];
      }
      return ret;
    }
  }
  BitVector operator^(const BitVector& v) const {
    if (size_ <= v.size_) {
      BitVector ret(v.size_);
      for (uint32_t i = 0; i < _get_alloc_size64(size_); i++) {
        ret.mem_.get()[i] = mem_.get()[i] ^ v.mem_.get()[i];
      }
      for (uint32_t i = _get_alloc_size64(size_);
           i < _get_alloc_size64(v.size_); i++) {
        ret.mem_.get()[i] = v.mem_.get()[i];
      }
      return ret;
    } else {
      BitVector ret(size_);
      for (uint32_t i = 0; i < _get_alloc_size64(v.size_); i++) {
        ret.mem_.get()[i] = mem_.get()[i] ^ v.mem_.get()[i];
      }
      for (uint32_t i = _get_alloc_size64(v.size_);
           i < _get_alloc_size64(size_); i++) {
        ret.mem_.get()[i] = mem_.get()[i];
      }
      return ret;
    }
  }
  operator bool() const {
    for (uint32_t i = 0; i < _get_alloc_size64(size_); i++)
      if (mem_.get()[i]) {
        return true;
      }
    return false;
  }

  uint32_t size() const { return size_; }

  std::string ToString() const {
    std::string ret;
    for (uint32_t i = 0; i < size_; i++) {
      if (mem_.get()[i / kBitSize] >> (i % kBitSize) & 1)
        ret += "1";
      else
        ret += "0";
    }
    return ret;
  }

  bool HasIntersection(const BitVector& v) const {
    for (uint32_t i = 0; i < _get_alloc_size64(std::min(size_, v.size_)); i++) {
      auto x = mem_.get()[i] & v.mem_.get()[i];
      if (x)
        return true;
    }
    return false;
  }

  void SetZeros() {
    if (mem_) {
      std::memset(mem_.get(), 0, alloc_size_);
    }
  }

  void Resize(size_t new_size) {
    uint32_t new_alloc_size = _get_alloc_size64(new_size) * kBitSize / 8;
    auto new_mem =
        std::unique_ptr<uint64_t[]>(new uint64_t[_get_alloc_size64(new_size)]);
    std::memset(new_mem.get(), 0, new_alloc_size);
    std::memcpy(
        new_mem.get(), mem_.get(), std::min(new_alloc_size, alloc_size_));
    mem_ = std::move(new_mem);
    size_ = new_size;
    alloc_size_ = new_alloc_size;
  }

  size_t Count() const {
    size_t ret = 0;
    for (uint32_t i = 0; i < alloc_size_ * 8 / kBitSize; i++) {
      ret += __builtin_popcountll(mem_[i]);
    }
    return ret;
  }

 private:
  static uint32_t _get_alloc_size64(uint32_t size) {
    return (size + kBitSize - 1) / kBitSize;
  }
  uint32_t size_{0};
  uint32_t alloc_size_{0};
  std::unique_ptr<uint64_t[]> mem_;
};

}  // namespace wing
