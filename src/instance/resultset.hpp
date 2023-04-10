#ifndef SAKURA_RESULT_SET_H__
#define SAKURA_RESULT_SET_H__

#include <memory>
#include <string>

#include "type/field.hpp"
#include "type/vector.hpp"

namespace wing {

class ResultSet {
 public:
  class ResultData {
   public:
    ResultData(const uint8_t* data) : data_(data) {}
    int64_t ReadInt(size_t id) const {
      return *reinterpret_cast<const int64_t*>(data_ + id * 8);
    }
    double ReadFloat(size_t id) const {
      return *reinterpret_cast<const double*>(data_ + id * 8);
    }
    std::string_view ReadString(size_t id) const {
      return reinterpret_cast<const StaticStringField*>(
          *reinterpret_cast<const int64_t*>(data_ + id * 8))
          ->ReadStringView();
    }
    operator bool() const { return data_ != nullptr; }

   private:
    const uint8_t* data_{nullptr};
  };
  ResultSet() { parse_error_msg_ = "null resultset"; }
  ResultSet(TupleStore&& store) : tuple_store_(std::move(store)) {}
  ResultSet(std::string_view parser_error, std::string_view execute_error)
    : parse_error_msg_(parser_error), execute_error_msg_(execute_error) {}
  ResultData Next() {
    if (offset_ < tuple_store_.GetPointerVec().size()) {
      return tuple_store_.GetPointerVec()[offset_++];
    }
    return nullptr;
  }

  bool Valid() const {
    return parse_error_msg_ == "" && execute_error_msg_ == "";
  }

  bool ParseValid() const { return parse_error_msg_ == ""; }

  std::string GetErrorMsg() const {
    return parse_error_msg_ == "" ? execute_error_msg_ : parse_error_msg_;
  }

 private:
  std::string parse_error_msg_;
  std::string execute_error_msg_;
  TupleStore tuple_store_;
  size_t offset_{0};
};

}  // namespace wing

#endif