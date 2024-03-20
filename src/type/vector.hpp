#pragma once

#include "type/field_type.hpp"
#include "type/vector_buffer.hpp"

namespace wing {

enum class VectorType {
  Flat = 0, /* Uncompressed vectors */
  Constant, /* Vectors containing one constant element */
};

/**
 * Vector stores several elements of the same type
 */
class Vector {
 public:
  Vector() : type_(VectorType::Flat), elem_type_(LogicalType::INT), size_(0) {}

  Vector(VectorType type, LogicalType element_type, size_t size)
    : type_(type), elem_type_(element_type), size_(size) {
    if (type_ == VectorType::Flat) {
      buf_ = VectorBuffer::CreateStandardBuffer(elem_type_, size_);
    } else if (type_ == VectorType::Constant) {
      buf_ = VectorBuffer::CreateConstantBuffer(elem_type_);
    }
    data_ = buf_->Data();
  }

  Vector(const Vector& vec) {
    data_ = vec.data_;
    buf_ = vec.buf_;
    size_ = vec.size_;
    type_ = vec.type_;
    elem_type_ = vec.elem_type_;
    aux_ = vec.aux_;
  }

  StaticFieldRef* Data() { return reinterpret_cast<StaticFieldRef*>(data_); }

  const StaticFieldRef* Data() const {
    return reinterpret_cast<const StaticFieldRef*>(data_);
  }

  size_t size() const { return size_; }

  void SetAux(std::shared_ptr<VectorBuffer> auxbuf) { aux_ = auxbuf; }

  std::shared_ptr<VectorBuffer> GetAux() const { return aux_; }

  LogicalType GetElemType() const { return elem_type_; }

  VectorType GetVectorType() const { return type_; }

  inline StaticFieldRef Get(size_t tuple_idx) const {
    if (type_ == VectorType::Constant) {
      return Data()[0];
    } else {
      return Data()[tuple_idx];
    }
  }

  inline void Set(size_t tuple_idx, StaticFieldRef val) {
    if (type_ == VectorType::Constant) {
      Data()[0] = val;
    } else {
      Data()[tuple_idx] = val;
    }
  }

  Vector Slice(size_t begin, size_t count) const {
    Vector ret = *this;
    begin = std::min(begin, size_);
    count = std::min(size_ - begin, count);
    ret.data_ = data_ + sizeof(StaticFieldRef) * begin;
    ret.size_ = count;
    return ret;
  }

  void Resize(size_t new_size) {
    if (type_ == VectorType::Constant) {
      return;
    }
    if (new_size <= size_) {
      size_ = new_size;
      return;
    }
    auto new_buf = VectorBuffer::CreateStandardBuffer(elem_type_, new_size);
    std::memcpy(new_buf->Data(), buf_->Data(), size_ * sizeof(StaticFieldRef));
    buf_ = new_buf;
    data_ = buf_->Data();
    size_ = new_size;
  }

 private:
  /* pointer to data */
  uint8_t* data_{nullptr};
  /* main buffer storing data */
  std::shared_ptr<VectorBuffer> buf_;
  /* the type of vector */
  VectorType type_;
  /* the type of elements. */
  LogicalType elem_type_;
  /* amount of elements. */
  size_t size_{0};
  /* aux data (for example, string data)*/
  std::shared_ptr<VectorBuffer> aux_;
};

};  // namespace wing
