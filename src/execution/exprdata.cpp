#include "execution/exprdata.hpp"

#include "type/tuple.hpp"

namespace wing {

/**
 * Produce a std::function from Expr.
 */
namespace __detail {

template <typename RetValueType, typename U, typename... T>
std::function<RetValueType(U, T...)> GenerateColumnExpr(uint32_t offset) {
  return [=](U t, T...) { return t.template Read<RetValueType>(offset); };
}

template <typename U, typename... T>
std::function<StaticFieldRef(U, T...)> GenerateStringColumnExpr(
    uint32_t offset) {
  return [=](U t, T...) { return t.CreateStringRef(offset); };
}

template <typename V>
double ReadFromAggregateIntermediateData(V t, uint32_t id, double) {
  return t[id].data_.double_data;
}
template <typename V>
int64_t ReadFromAggregateIntermediateData(V t, uint32_t id, int64_t) {
  return t[id].data_.int_data;
}
template <typename V>
int32_t ReadFromAggregateIntermediateData(V t, uint32_t id, int32_t) {
  return t[id].data_.int_data;
}
template <typename V>
StaticFieldRef ReadFromAggregateIntermediateData(
    V t, uint32_t id, StaticFieldRef) {
  return t[id].data_.int_data;
}

template <typename RetValueType, typename U, typename V, typename... T>
std::function<RetValueType(U, V, T...)> GenerateColumnExpr2(uint32_t id) {
  return [=](U, V t, T...) {
    return ReadFromAggregateIntermediateData(t, id, RetValueType());
  };
}

template <typename RetValueType, typename U, typename V, typename... T>
std::function<RetValueType(U, V, T...)> GenerateColumnAvgExpr2(uint32_t id) {
  return [=](U, V t, T...) {
    return RetValueType(t[id].data_.double_data / t[id].size_);
  };
}

template <typename RetValueType, typename U, typename V, typename... T>
std::function<RetValueType(U, V, T...)> GenerateColumnCountExpr2(uint32_t id) {
  return [=](U, V t, T...) { return RetValueType(t[id].size_); };
}

#define FUNC_USED_FOR_COMPILE(func_name)               \
  template <typename RetValueType, typename U>         \
  std::function<RetValueType(U)> func_name(uint32_t) { \
    return [=](U) { return RetValueType(); };          \
  }

FUNC_USED_FOR_COMPILE(GenerateColumnCountExpr2)
FUNC_USED_FOR_COMPILE(GenerateColumnAvgExpr2)
FUNC_USED_FOR_COMPILE(GenerateColumnExpr2)

#undef FUNC_USED_FOR_COMPILE

template <typename RetValueType, typename... T>
std::function<RetValueType(T...)> GenerateExprFunction(const Expr* expr,
    const OutputSchema& input_schema,
    const OutputSchema* right_input_schema = nullptr,
    std::vector<const Expr*>* aggregate_expr_vec = nullptr,
    std::vector<std::function<void(AggregateIntermediateData&,
        StaticFieldRef)>>* aggregate_func_vec = nullptr);

template <typename... T>
std::function<StaticFieldRef(T...)> GenerateStringExprFunction(const Expr* expr,
    const OutputSchema& input_schema,
    const OutputSchema* right_input_schema = nullptr,
    std::vector<const Expr*>* aggregate_expr_vec = nullptr,
    std::vector<std::function<void(AggregateIntermediateData&,
        StaticFieldRef)>>* aggregate_func_vec = nullptr);

template <typename RetValueType, typename... T>
std::function<RetValueType(T...)> GenerateExprFunction(const Expr* expr,
    const OutputSchema& input_schema, const OutputSchema* right_input_schema,
    std::vector<const Expr*>* aggregate_expr_vec,
    std::vector<std::function<void(
        AggregateIntermediateData&, StaticFieldRef)>>* aggregate_func_vec) {
  if (expr->type_ == ExprType::LITERAL_INTEGER) {
    return [x = static_cast<const LiteralIntegerExpr*>(expr)->literal_value_](
               T...) { return static_cast<RetValueType>(x); };
  } else if (expr->type_ == ExprType::LITERAL_FLOAT) {
    return [x = static_cast<const LiteralFloatExpr*>(expr)->literal_value_](
               T...) { return static_cast<RetValueType>(x); };
  } else if (expr->type_ == ExprType::UNARYOP) {
    auto this_expr = static_cast<const UnaryExpr*>(expr);
    auto ch = GenerateExprFunction<RetValueType, T...>(this_expr->ch0_.get(),
        input_schema, right_input_schema, aggregate_expr_vec,
        aggregate_func_vec);
    return [=](T... t) { return -ch(t...); };
  } else if (expr->type_ == ExprType::UNARYCONDOP) {
    auto this_expr = static_cast<const UnaryConditionExpr*>(expr);
    auto ch = GenerateExprFunction<RetValueType, T...>(this_expr->ch0_.get(),
        input_schema, right_input_schema, aggregate_expr_vec,
        aggregate_func_vec);
    return [=](T... t) { return !ch(t...); };
  } else if (expr->type_ == ExprType::BINOP) {
    auto this_expr = static_cast<const BinaryExpr*>(expr);

#define GEN_FUNC(op_name, op)                \
  {                                          \
    if (this_expr->op_ == OpType::op_name) { \
      return [=](T... t) {                   \
        auto x = ch0(t...);                  \
        auto y = ch1(t...);                  \
        return x op y;                       \
      };                                     \
    }                                        \
  }

    if (this_expr->ch0_->ret_type_ == RetType::FLOAT) {
      auto ch0 = GenerateExprFunction<double, T...>(this_expr->ch0_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      auto ch1 = GenerateExprFunction<double, T...>(this_expr->ch1_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      GEN_FUNC(ADD, +);
      GEN_FUNC(SUB, -);
      GEN_FUNC(MUL, *);
      GEN_FUNC(DIV, /);
      DB_ERR("Internal Error: Invalid operator between two real numbers.");
    } else if (this_expr->ch0_->ret_type_ == RetType::INT) {
      auto ch0 = GenerateExprFunction<int64_t, T...>(this_expr->ch0_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      auto ch1 = GenerateExprFunction<int64_t, T...>(this_expr->ch1_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      GEN_FUNC(ADD, +);
      GEN_FUNC(SUB, -);
      GEN_FUNC(MUL, *);
      GEN_FUNC(DIV, /);
      GEN_FUNC(MOD, %);
      GEN_FUNC(BITAND, &);
      GEN_FUNC(BITOR, |);
      GEN_FUNC(BITXOR, ^);
      GEN_FUNC(BITLSH, <<);
      GEN_FUNC(BITRSH, >>);
      DB_ERR("Internal Error: Invalid operator between two integer numbers.");
    }
  } else if (expr->type_ == ExprType::BINCONDOP) {
    auto this_expr = static_cast<const BinaryConditionExpr*>(expr);
    if (this_expr->ch0_->ret_type_ == RetType::STRING) {
      auto ch0 =
          GenerateStringExprFunction<T...>(this_expr->ch0_.get(), input_schema,
              right_input_schema, aggregate_expr_vec, aggregate_func_vec);
      auto ch1 =
          GenerateStringExprFunction<T...>(this_expr->ch1_.get(), input_schema,
              right_input_schema, aggregate_expr_vec, aggregate_func_vec);
#define GEN_STR_FUNC(op_name, op)                                      \
  {                                                                    \
    if (this_expr->op_ == OpType::op_name) {                           \
      return [=](T... t) {                                             \
        return static_cast<RetValueType>(                              \
            ch0(t...).ReadStringView() op ch1(t...).ReadStringView()); \
      };                                                               \
    }                                                                  \
  }
      GEN_STR_FUNC(LT, <);
      GEN_STR_FUNC(GT, >);
      GEN_STR_FUNC(LEQ, <=);
      GEN_STR_FUNC(GEQ, >=);
      GEN_STR_FUNC(EQ, ==);
      GEN_STR_FUNC(NEQ, !=);
#undef GEN_STR_FUNC
      DB_ERR("Internal Error: Invalid operator on strings.");
    } else if (this_expr->ch0_->ret_type_ == RetType::FLOAT) {
      auto ch0 = GenerateExprFunction<double, T...>(this_expr->ch0_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      auto ch1 = GenerateExprFunction<double, T...>(this_expr->ch1_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      GEN_FUNC(LT, <);
      GEN_FUNC(GT, >);
      GEN_FUNC(LEQ, <=);
      GEN_FUNC(GEQ, >=);
      GEN_FUNC(EQ, ==);
      GEN_FUNC(NEQ, !=);
      DB_ERR("Internal Error: Invalid operator between two strings.");
    } else if (this_expr->ch0_->ret_type_ == RetType::INT) {
      auto ch0 = GenerateExprFunction<int64_t, T...>(this_expr->ch0_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      auto ch1 = GenerateExprFunction<int64_t, T...>(this_expr->ch1_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      GEN_FUNC(LT, <);
      GEN_FUNC(GT, >);
      GEN_FUNC(LEQ, <=);
      GEN_FUNC(GEQ, >=);
      GEN_FUNC(EQ, ==);
      GEN_FUNC(NEQ, !=);
      GEN_FUNC(AND, &&);
      GEN_FUNC(OR, ||);
      DB_ERR("Internal Error: Invalid operator between two integer numbers.");
    }
#undef GEN_FUNC
  } else if (expr->type_ == ExprType::AGGR) {
    DB_ASSERT(aggregate_func_vec != nullptr && aggregate_expr_vec != nullptr);
    auto this_expr = static_cast<const AggregateFunctionExpr*>(expr);
    int id = aggregate_expr_vec->size();
    aggregate_expr_vec->push_back(this_expr->ch0_.get());

    // id * 2 is the initial function, which is executed in FirstEvaluate.
    // id * 2 + 1 is the aggregate function, which is executed in Aggregate.
#define AGGR_FUNC(statement)     \
  aggregate_func_vec->push_back( \
      [](AggregateIntermediateData& x, StaticFieldRef y) { statement; })
    if (this_expr->func_name_ == "max") {
      if (this_expr->ret_type_ == RetType::INT) {
        AGGR_FUNC({ x.data_.int_data = y.data_.int_data; });
        AGGR_FUNC({
          x.data_.int_data = std::max(x.data_.int_data, y.data_.int_data);
        });
      } else if (this_expr->ret_type_ == RetType::FLOAT) {
        AGGR_FUNC({ x.data_.double_data = y.data_.double_data; });
        AGGR_FUNC({
          x.data_.double_data =
              std::max(x.data_.double_data, y.data_.double_data);
        });
      }
    } else if (this_expr->func_name_ == "min") {
      if (this_expr->ret_type_ == RetType::INT) {
        AGGR_FUNC({ x.data_.int_data = y.data_.int_data; });
        AGGR_FUNC({
          x.data_.int_data = std::min(x.data_.int_data, y.data_.int_data);
        });
      } else if (this_expr->ret_type_ == RetType::FLOAT) {
        AGGR_FUNC({ x.data_.double_data = y.data_.double_data; });
        AGGR_FUNC({
          x.data_.double_data =
              std::min(x.data_.double_data, y.data_.double_data);
        });
      }
    } else if (this_expr->func_name_ == "count") {
      AGGR_FUNC({ x.size_ = 1; });
      AGGR_FUNC({ x.size_ += 1; });
    } else if (this_expr->func_name_ == "sum") {
      if (this_expr->ret_type_ == RetType::INT) {
        AGGR_FUNC({ x.data_.int_data = y.data_.int_data; });
        AGGR_FUNC({ x.data_.int_data = x.data_.int_data + y.data_.int_data; });
      } else if (this_expr->ret_type_ == RetType::FLOAT) {
        AGGR_FUNC({ x.data_.double_data = y.data_.double_data; });
        AGGR_FUNC({
          x.data_.double_data = x.data_.double_data + y.data_.double_data;
        });
      }
    } else if (this_expr->func_name_ == "avg") {
      if (this_expr->ch0_->ret_type_ == RetType::INT) {
        AGGR_FUNC({
          x.data_.double_data = y.data_.int_data;
          x.size_ = 1;
        });
        AGGR_FUNC({
          x.data_.double_data = x.data_.double_data + y.data_.int_data;
          x.size_ += 1;
        });
      } else if (this_expr->ch0_->ret_type_ == RetType::FLOAT) {
        AGGR_FUNC({
          x.data_.double_data = y.data_.double_data;
          x.size_ = 1;
        });
        AGGR_FUNC({
          x.data_.double_data = x.data_.double_data + y.data_.double_data;
          x.size_ += 1;
        });
      } else {
        DB_ERR("Internal Error: Invalid Aggregate Function type.");
      }
    }
    if (this_expr->func_name_ == "max" || this_expr->func_name_ == "min" ||
        this_expr->func_name_ == "sum") {
      return GenerateColumnExpr2<RetValueType, T...>(id);
    } else if (this_expr->func_name_ == "avg") {
      return GenerateColumnAvgExpr2<RetValueType, T...>(id);
    } else if (this_expr->func_name_ == "count") {
      return GenerateColumnCountExpr2<RetValueType, T...>(id);
    }
  } else if (expr->type_ == ExprType::COLUMN) {
    auto this_expr = static_cast<const ColumnExpr*>(expr);
    uint32_t id = ~0u, offset = 0, flag_join = right_input_schema ? 2 : 1;
    for (uint32_t index = 0; auto& col : input_schema.GetCols()) {
      if (col.id_ == this_expr->id_in_column_name_table_) {
        id = index;
        break;
      }
      offset += col.size_;
      index += 1;
    }

    if (id != (~0u)) {
      if (!input_schema.IsRaw() || right_input_schema != nullptr) {
        // This means that the input is an array of StaticFieldRef.
        return GenerateColumnExpr<RetValueType, T...>(
            id * sizeof(StaticFieldRef) * flag_join);
      } else {
        // RetValueType is not string
        // And we ensure that strings are behind static fields.
        // So we can use the offset directly.
        // Tuple layout: (tuple size 4 byte), (static fields), (offsets of
        // varchars), (varchars)
        if (input_schema[id].type_ == FieldType::INT32) {
          return GenerateColumnExpr<int32_t, T...>(
              Tuple::GetOffsetOfStaticField(offset) * flag_join);
        }
        return GenerateColumnExpr<RetValueType, T...>(
            Tuple::GetOffsetOfStaticField(offset) * flag_join);
      }
    }

    if (right_input_schema != nullptr) {
      // Join

      // Join use the last bit of offset to identify the table that the field is
      // in. i.e. offset = (the true offset) * 2 + (flag)
      offset = 0;
      for (uint32_t index = 0; auto& col : right_input_schema->GetCols()) {
        if (col.id_ == this_expr->id_in_column_name_table_) {
          id = index;
          break;
        }
        offset += col.size_;
        index += 1;
      }
    }

    if (id == (~0u)) {
      DB_ERR("Internal Error: Expression contains invalid parameters.");
    }

    if (!right_input_schema->IsRaw()) {
      return GenerateColumnExpr<RetValueType, T...>(
          id * sizeof(StaticFieldRef) * 2 + 1);
    } else {
      if ((*right_input_schema)[id].type_ == FieldType::INT32) {
        return GenerateColumnExpr<int32_t, T...>(
            Tuple::GetOffsetOfStaticField(offset) * 2 + 1);
      }
      return GenerateColumnExpr<RetValueType, T...>(
          Tuple::GetOffsetOfStaticField(offset) * 2 + 1);
    }

  } else if (expr->type_ == ExprType::CAST) {
    auto this_expr = static_cast<const CastExpr*>(expr);
    if (expr->ret_type_ == RetType::FLOAT &&
        this_expr->ch0_->ret_type_ == RetType::INT) {
      auto ch = GenerateExprFunction<int64_t, T...>(this_expr->ch0_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      return [=](T... t) { return static_cast<RetValueType>(ch(t...)); };
    } else if (expr->ret_type_ == RetType::INT &&
               this_expr->ch0_->ret_type_ == RetType::FLOAT) {
      auto ch = GenerateExprFunction<double, T...>(this_expr->ch0_.get(),
          input_schema, right_input_schema, aggregate_expr_vec,
          aggregate_func_vec);
      return [=](T... t) { return static_cast<RetValueType>(ch(t...)); };
    } else
      DB_ERR("Internal Error: Invalid CastExpr.");
  }
  DB_ERR("Internal Error: Invalid Expr.");
}
template <typename... T>
std::function<StaticFieldRef(T...)> GenerateStringExprFunction(const Expr* expr,
    const OutputSchema& input_schema, const OutputSchema* right_input_schema,
    std::vector<const Expr*>*,
    std::vector<
        std::function<void(AggregateIntermediateData&, StaticFieldRef)>>*) {
  if (expr->type_ == ExprType::LITERAL_STRING) {
    auto str = std::shared_ptr<StaticStringField>(
        StaticStringField::Generate(
            static_cast<const LiteralStringExpr*>(expr)->literal_value_),
        StaticStringField::FreeFromGenerate);
    return [str = std::move(str)](
               T...) { return StaticFieldRef::CreateStringRef(str.get()); };
  } else if (expr->type_ == ExprType::COLUMN) {
    auto this_expr = static_cast<const ColumnExpr*>(expr);
    uint32_t id = ~0u, offset = 0, id_in_str = 0,
             flag_join = right_input_schema ? 2 : 1;
    for (uint32_t index = 0; auto& col : input_schema.GetCols()) {
      if (col.id_ == this_expr->id_in_column_name_table_) {
        id = index;
        break;
      }
      if (col.type_ == FieldType::VARCHAR || col.type_ == FieldType::CHAR) {
        // TODO: support CHAR
        id_in_str += 1;
      } else {
        offset += col.size_;
      }
      index += 1;
    }
    if (id != (~0u)) {
      if (!input_schema.IsRaw() || right_input_schema != nullptr) {
        // This means that the input is an array of StaticFieldRef.
        return GenerateColumnExpr<StaticFieldRef, T...>(
            id * sizeof(StaticFieldRef) * flag_join);
      } else {
        // Tuple layout: (tuple size 4 byte), (static fields), (offsets of
        // varchars), (varchars) We must find the offsets of varchars. Then find
        // the corresponding offset.

        // This function requires the offset of the entry in the offsets of
        // varchars.
        return GenerateStringColumnExpr<T...>(
            Tuple::GetOffsetsOfStrings(offset, id_in_str) * flag_join);
      }
    }

    if (right_input_schema != nullptr) {
      // Join

      // Join use the last bit of offset to identify the table that the field is
      // in. i.e. offset = (the true offset) * 2 + (flag)
      offset = 0;
      id_in_str = 0;
      for (uint32_t index = 0; auto& col : right_input_schema->GetCols()) {
        if (col.id_ == this_expr->id_in_column_name_table_) {
          id = index;
          break;
        }
        if (col.type_ == FieldType::VARCHAR || col.type_ == FieldType::CHAR) {
          id_in_str += 1;
        } else {
          offset += col.size_;
        }
        index += 1;
      }
    }

    if (id == (~0u)) {
      DB_ERR("Internal Error: Expression contains invalid parameters.");
    }

    if (!right_input_schema->IsRaw()) {
      return GenerateColumnExpr<StaticFieldRef, T...>(
          id * sizeof(StaticFieldRef) * 2 + 1);
    } else {
      return GenerateStringColumnExpr<T...>(
          Tuple::GetOffsetsOfStrings(offset, id_in_str) * 2 + 1);
    }

  } else {
    DB_ERR("Internal Error: Invalid Expr.");
  }
}

}  // namespace __detail

/**
 * @brief Generate a std::function object from an Expr. The std::function
 * receives an iterator to parameters and returns the result as a Field.
 *
 * @tparam T
 * @param expr
 * @return std::function<Field(T)>
 */
template <typename... T>
std::function<StaticFieldRef(T...)> GenerateFunction(const Expr* expr,
    const OutputSchema& input_schema,
    const OutputSchema* right_input_schema = nullptr,
    std::vector<const Expr*>* aggregate_expr_vec = nullptr,
    std::vector<std::function<void(AggregateIntermediateData&,
        StaticFieldRef)>>* aggregate_func_vec = nullptr) {
  if (expr->ret_type_ == RetType::STRING) {
    return __detail::GenerateStringExprFunction<T...>(expr, input_schema,
        right_input_schema, aggregate_expr_vec, aggregate_func_vec);
  } else if (expr->ret_type_ == RetType::INT) {
    return __detail::GenerateExprFunction<int64_t, T...>(expr, input_schema,
        right_input_schema, aggregate_expr_vec, aggregate_func_vec);
  } else if (expr->ret_type_ == RetType::FLOAT) {
    return __detail::GenerateExprFunction<double, T...>(expr, input_schema,
        right_input_schema, aggregate_expr_vec, aggregate_func_vec);
  } else {
    DB_ERR("Internal Error: Invalid Expr.");
  }
}

ExprFunction::ExprFunction(const Expr* expr, const OutputSchema& input_schema) {
  if (expr == nullptr) {
    func_ = nullptr;
  } else {
    func_ = GenerateFunction<InputTuplePtr>(expr, input_schema);
  }
}

StaticFieldRef ExprFunction::Evaluate(InputTuplePtr input) const {
  return func_(input);
}

ExprFunction::operator bool() const { return bool(func_); }

AggregateExprFunction::AggregateExprFunction(
    const Expr* expr, const OutputSchema& input_schema) {
  if (expr == nullptr) {
    func_ = nullptr;
  } else {
    std::vector<const Expr*> aggregate_exprs;
    auto input_schema_ = input_schema;
    // Input rows are stored as std::vector<StaticFieldRef>
    // And expressions are evaluated on it.
    input_schema_.SetRaw(false);
    func_ = GenerateFunction<InputTuplePtr, const AggregateIntermediateData*>(
        expr, input_schema_, nullptr, &aggregate_exprs, &aggregate_func_);
    aggregate_exprs.reserve(aggregate_exprs.size());
    for (const auto& a : aggregate_exprs) {
      aggregate_exprfunction_.push_back(ExprFunction(a, input_schema));
    }
  }
}

void AggregateExprFunction::FirstEvaluate(
    AggregateIntermediateData* aggregate_data, InputTuplePtr input) {
  for (uint32_t id = 0; const auto& a : aggregate_exprfunction_) {
    aggregate_func_[id * 2](aggregate_data[id], a.Evaluate(input));
    id += 1;
  }
}

void AggregateExprFunction::Aggregate(
    AggregateIntermediateData* aggregate_data, InputTuplePtr input) {
  for (uint32_t id = 0; const auto& a : aggregate_exprfunction_) {
    aggregate_func_[id * 2 + 1](aggregate_data[id], a.Evaluate(input));
    id += 1;
  }
}

StaticFieldRef AggregateExprFunction::LastEvaluate(
    AggregateIntermediateData* aggregate_data, InputTuplePtr stored_parameter) {
  return func_(stored_parameter, aggregate_data);
}

AggregateExprFunction::operator bool() const { return bool(func_); }

//
class JoinInputTuplePtr {
 public:
  JoinInputTuplePtr() = default;
  JoinInputTuplePtr(InputTuplePtr vec0, InputTuplePtr vec1)
    : vec0_(vec0), vec1_(vec1) {}
  template <typename T>
  T Read(uint32_t offset) const {
    return ~offset & 1 ? vec0_.Read<T>(offset >> 1)
                       : vec1_.Read<T>(offset >> 1);
  }
  StaticFieldRef CreateStringRef(uint32_t offset) const {
    return ~offset & 1 ? vec0_.CreateStringRef(offset >> 1)
                       : vec1_.CreateStringRef(offset >> 1);
  }

 private:
  InputTuplePtr vec0_;
  InputTuplePtr vec1_;
};

JoinExprFunction::JoinExprFunction(const Expr* expr,
    const OutputSchema& left_input_schema,
    const OutputSchema& right_input_schema) {
  if (expr == nullptr) {
    func_ = nullptr;
  } else {
    auto ch = GenerateFunction<JoinInputTuplePtr>(
        expr, left_input_schema, &right_input_schema);
    func_ = [=](InputTuplePtr x, InputTuplePtr y) {
      return ch(JoinInputTuplePtr(x, y));
    };
  }
}

StaticFieldRef JoinExprFunction::Evaluate(
    InputTuplePtr input1, InputTuplePtr input2) const {
  return func_(input1, input2);
}

JoinExprFunction::operator bool() const { return bool(func_); }

}  // namespace wing