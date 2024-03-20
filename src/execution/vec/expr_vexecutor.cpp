#include "execution/vec/expr_vexecutor.hpp"

namespace wing {

void ExprVecExecutor::Init() {
  for (auto& ch : ch_)
    ch.Init();
  imm_.clear();
}

void ExprVecExecutor::Evaluate(
    std::span<Vector> input, size_t count, Vector& result) {
  if (!ch_.size()) {
    // The leaf node.
    func_(input, count, result);
    return;
  }
  imm_.resize(ch_.size());
  for (uint32_t i = 0; i < ch_.size(); i++) {
    ch_[i].Evaluate(input, count, imm_[i]);
  }
  func_(imm_, count, result);
}

/* Set correct element type and vector type and amount of elements. If
 * necessary, reallocate the buffer. */
void FitType(Vector& result, VectorType vec_type, LogicalType result_type,
    size_t count) {
  if (result.GetElemType() != result_type ||
      result.GetVectorType() != vec_type || result.size() < count) {
    result = Vector(vec_type, result_type, count);
  } else {
    result.Resize(count);
  }
}

VectorType GetVecType(std::span<Vector> input) {
  VectorType vec_type = VectorType::Constant;
  for (auto& v : input) {
    if (v.GetVectorType() != VectorType::Constant) {
      vec_type = VectorType::Flat;
    }
  }
  return vec_type;
}

ExprVecExecutor ExprVecExecutor::CreateInternal(const Expr* expr,
    const OutputSchema& input_schema, CreateInternalState& state) {
  ExprVecExecutor ret;
  if (expr == nullptr) {
    return ret;
  }
  if (expr->type_ == ExprType::COLUMN) {
    auto this_expr = static_cast<const ColumnExpr*>(expr);
    uint32_t id = ~0u;
    for (uint32_t index = 0; auto& col : input_schema.GetCols()) {
      if (col.id_ == this_expr->id_in_column_name_table_) {
        id = index;
        break;
      }
      index += 1;
    }

    if (id != (~0u)) {
      ret.func_ = [id](std::span<Vector> input, size_t, Vector& result) -> int {
        result = input[id];
        return 0;
      };
    }

    if (id == (~0u)) {
      DB_ERR("Internal Error: Expression contains invalid parameters.");
    }
  } else if (expr->type_ == ExprType::LITERAL_FLOAT) {
    ret.func_ =
        [x = static_cast<const LiteralFloatExpr*>(expr)->literal_value_](
            std::span<Vector> input, size_t count, Vector& result) -> int {
      result = Vector(VectorType::Constant, LogicalType::FLOAT, count);
      result.Set(0, x);
      return 0;
    };
  } else if (expr->type_ == ExprType::LITERAL_INTEGER) {
    ret.func_ =
        [x = static_cast<const LiteralIntegerExpr*>(expr)->literal_value_](
            std::span<Vector> input, size_t count, Vector& result) -> int {
      result = Vector(VectorType::Constant, LogicalType::INT, count);
      result.Set(0, x);
      return 0;
    };
  } else if (expr->type_ == ExprType::LITERAL_STRING) {
    ret.func_ =
        [x = static_cast<const LiteralStringExpr*>(expr)->literal_value_](
            std::span<Vector> input, size_t count, Vector& result) -> int {
      result = Vector(VectorType::Constant, LogicalType::INT, count);
      auto buf = StringVectorBuffer::Create();
      auto val = buf->AddString(x);
      result.SetAux(buf);
      result.Set(0, val);
      return 0;
    };
  } else if (expr->type_ == ExprType::UNARYOP) {
    ret.ch_.push_back(CreateInternal(expr->ch0_.get(), input_schema, state));
    if (expr->ret_type_ == LogicalType::FLOAT) {
      ret.func_ = [](std::span<Vector> v, size_t count, Vector& result) -> int {
        FitType(result, GetVecType(v), LogicalType::FLOAT, count);
        uint32_t _end =
            result.GetVectorType() == VectorType::Constant ? 1 : result.size();
        for (uint32_t i = 0; i < _end; i++) {
          result.Set(i, -v[0].Get(i).ReadFloat());
        }
        return 0;
      };
    } else if (expr->ret_type_ == LogicalType::INT) {
      ret.func_ = [](std::span<Vector> v, size_t count, Vector& result) -> int {
        FitType(result, GetVecType(v), LogicalType::INT, count);
        uint32_t _end =
            result.GetVectorType() == VectorType::Constant ? 1 : result.size();
        for (uint32_t i = 0; i < _end; i++) {
          result.Set(i, -v[0].Get(i).ReadInt());
        }
        return 0;
      };
    }
  } else if (expr->type_ == ExprType::UNARYCONDOP) {
    ret.ch_.push_back(CreateInternal(expr->ch0_.get(), input_schema, state));
    if (expr->ret_type_ == LogicalType::INT) {
      ret.func_ = [](std::span<Vector> v, size_t count, Vector& result) -> int {
        FitType(result, GetVecType(v), LogicalType::INT, count);
        uint32_t _end =
            result.GetVectorType() == VectorType::Constant ? 1 : result.size();
        for (uint32_t i = 0; i < _end; i++) {
          result.Set(i, int64_t(!v[0].Get(i).ReadInt()));
        }
        return 0;
      };
    }
  } else if (expr->type_ == ExprType::BINOP) {
    auto this_expr = static_cast<const BinaryExpr*>(expr);
    ret.ch_.push_back(CreateInternal(expr->ch0_.get(), input_schema, state));
    ret.ch_.push_back(CreateInternal(expr->ch1_.get(), input_schema, state));

#define GEN_FUNC(op_name, op, type, readfunc, cast_type)                       \
  {                                                                            \
    if (this_expr->op_ == OpType::op_name) {                                   \
      ret.func_ = [](std::span<Vector> v, size_t count,                        \
                      Vector& result) -> int {                                 \
        FitType(result, GetVecType(v), LogicalType::type, count);              \
        uint32_t _end = result.GetVectorType() == VectorType::Constant         \
                            ? 1                                                \
                            : result.size();                                   \
        for (uint32_t i = 0; i < _end; i++) {                                  \
          result.Set(                                                          \
              i, cast_type(v[0].Get(i).readfunc() op v[1].Get(i).readfunc())); \
        }                                                                      \
        return 0;                                                              \
      };                                                                       \
      return ret;                                                              \
    }                                                                          \
  }

    if (this_expr->ch0_->ret_type_ == LogicalType::FLOAT) {
      GEN_FUNC(ADD, +, FLOAT, ReadFloat, StaticFieldRef::CreateFloat);
      GEN_FUNC(SUB, -, FLOAT, ReadFloat, StaticFieldRef::CreateFloat);
      GEN_FUNC(MUL, *, FLOAT, ReadFloat, StaticFieldRef::CreateFloat);
      GEN_FUNC(DIV, /, FLOAT, ReadFloat, StaticFieldRef::CreateFloat);
      DB_ERR(
          "Internal Error: Invalid operator between two real numbers. "
          "Operator: {}",
          int64_t(this_expr->op_));
    } else if (this_expr->ch0_->ret_type_ == LogicalType::INT) {
      GEN_FUNC(ADD, +, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(SUB, -, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(MUL, *, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(DIV, /, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(MOD, %, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(BITAND, &, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(BITOR, |, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(BITXOR, ^, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(BITLSH, <<, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(BITRSH, >>, INT, ReadInt, StaticFieldRef::CreateInt);
      DB_ERR(
          "Internal Error: Invalid operator between two integer numbers. "
          "Operator: {}",
          int64_t(this_expr->op_));
    }
  } else if (expr->type_ == ExprType::BINCONDOP) {
    auto this_expr = static_cast<const BinaryConditionExpr*>(expr);
    ret.ch_.push_back(CreateInternal(expr->ch0_.get(), input_schema, state));
    ret.ch_.push_back(CreateInternal(expr->ch1_.get(), input_schema, state));
    if (this_expr->ch0_->ret_type_ == LogicalType::STRING) {
      GEN_FUNC(LT, <, INT, ReadStringView, StaticFieldRef::CreateInt);
      GEN_FUNC(GT, >, INT, ReadStringView, StaticFieldRef::CreateInt);
      GEN_FUNC(LEQ, <=, INT, ReadStringView, StaticFieldRef::CreateInt);
      GEN_FUNC(GEQ, >=, INT, ReadStringView, StaticFieldRef::CreateInt);
      GEN_FUNC(EQ, ==, INT, ReadStringView, StaticFieldRef::CreateInt);
      GEN_FUNC(NEQ, !=, INT, ReadStringView, StaticFieldRef::CreateInt);
      DB_ERR("Internal Error: Invalid operator on strings.");
    } else if (this_expr->ch0_->ret_type_ == LogicalType::FLOAT) {
      GEN_FUNC(LT, <, INT, ReadFloat, StaticFieldRef::CreateInt);
      GEN_FUNC(GT, >, INT, ReadFloat, StaticFieldRef::CreateInt);
      GEN_FUNC(LEQ, <=, INT, ReadFloat, StaticFieldRef::CreateInt);
      GEN_FUNC(GEQ, >=, INT, ReadFloat, StaticFieldRef::CreateInt);
      GEN_FUNC(EQ, ==, INT, ReadFloat, StaticFieldRef::CreateInt);
      GEN_FUNC(NEQ, !=, INT, ReadFloat, StaticFieldRef::CreateInt);
      DB_ERR("Internal Error: Invalid operator between two strings.");
    } else if (this_expr->ch0_->ret_type_ == LogicalType::INT) {
      GEN_FUNC(LT, <, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(GT, >, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(LEQ, <=, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(GEQ, >=, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(EQ, ==, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(NEQ, !=, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(AND, &&, INT, ReadInt, StaticFieldRef::CreateInt);
      GEN_FUNC(OR, ||, INT, ReadInt, StaticFieldRef::CreateInt);
      DB_ERR("Internal Error: Invalid operator between two integer numbers.");
    }
#undef GEN_FUNC
  } else if (expr->type_ == ExprType::CAST) {
    auto this_expr = static_cast<const CastExpr*>(expr);
    if (expr->ret_type_ == LogicalType::FLOAT &&
        this_expr->ch0_->ret_type_ == LogicalType::INT) {
      ret.ch_.push_back(CreateInternal(expr->ch0_.get(), input_schema, state));
      ret.func_ = [](std::span<Vector> v, size_t count, Vector& result) -> int {
        FitType(result, GetVecType(v), LogicalType::FLOAT, count);
        uint32_t _end =
            result.GetVectorType() == VectorType::Constant ? 1 : result.size();
        for (uint32_t i = 0; i < _end; i++) {
          result.Set(i, double(v[0].Get(i).ReadInt()));
        }
        return 0;
      };
    } else if (expr->ret_type_ == LogicalType::INT &&
               this_expr->ch0_->ret_type_ == LogicalType::FLOAT) {
      ret.ch_.push_back(CreateInternal(expr->ch0_.get(), input_schema, state));
      ret.func_ = [](std::span<Vector> v, size_t count, Vector& result) -> int {
        FitType(result, GetVecType(v), LogicalType::FLOAT, count);
        uint32_t _end =
            result.GetVectorType() == VectorType::Constant ? 1 : result.size();
        for (uint32_t i = 0; i < _end; i++) {
          result.Set(i, int64_t(v[0].Get(i).ReadFloat()));
        }
        return 0;
      };
    } else {
      DB_ERR("Internal Error: Invalid CastExpr.");
    }
  } else if (expr->type_ == ExprType::AGGR) {
    auto id = state.agg_id_ + input_schema.size();
    state.agg_id_ += 1;
    ret.func_ = [id](std::span<Vector> input, size_t, Vector& result) -> int {
      result = input[id];
      return 0;
    };
    state.aggs_.push_back(
        CreateInternal(expr->ch0_.get(), input_schema, state));
    state.agg_metadata_.emplace_back(
        static_cast<const AggregateFunctionExpr*>(expr)->func_name_,
        expr->ch0_->ret_type_);
  } else {
    DB_ERR("Unknown expr type {}!", size_t(expr->type_));
  }
  return ret;
}

ExprVecExecutor ExprVecExecutor::Create(
    const Expr* expr, const OutputSchema& input_schema) {
  CreateInternalState internal_state;
  return CreateInternal(expr, input_schema, internal_state);
}

void AggExprVecExecutor::Aggregate(
    AggIntermediateData* data, TupleBatch::SingleTuple input) {
  for (uint32_t i = 0; i < agg_func_.size(); i++) {
    agg_func_[i](data[i], input[i]);
  }
}

AggIntermediateData* AggExprVecExecutor::CreateAggData() {
  auto ret = reinterpret_cast<AggIntermediateData*>(
      alloc_.Allocate(sizeof(AggIntermediateData) * agg_para_.size()));
  for (uint32_t i = 0; i < agg_para_.size(); i++) {
    ret[i].Init();
  }
  return ret;
}

void AggExprVecExecutor::EvaluateAggParas(
    std::span<Vector> input, size_t count, std::vector<Vector>& result) {
  result.resize(agg_para_.size());
  for (uint32_t i = 0; i < agg_para_.size(); i++) {
    agg_para_[i].Evaluate(input, count, result[i]);
  }
}

void AggExprVecExecutor::FinalEvaluate(std::span<AggIntermediateData*> data,
    std::span<Vector> input, Vector& result) {
  std::vector<Vector> input0(input.size());
  for (uint32_t i = 0; i < input.size(); i++)
    input0[i] = input[i];
  for (uint32_t i = 0; i < agg_para_.size(); i++) {
    Vector agg_result;
    agg_final_func_[i](data, agg_result);
    input0.push_back(agg_result);
  }
  expr_.Evaluate(input0, data.size(), result);
}

AggExprVecExecutor AggExprVecExecutor::Create(
    const Expr* expr, const OutputSchema& input_schema) {
  ExprVecExecutor::CreateInternalState state;
  AggExprVecExecutor ret;
  ret.expr_ = ExprVecExecutor::CreateInternal(expr, input_schema, state);
  ret.agg_para_ = state.aggs_;
  for (uint32_t i = 0; i < state.aggs_.size(); i++) {
    auto& [name, ty] = state.agg_metadata_[i];
#define AGGR_FUNC(                                                     \
    type, init_statement, update_statement, final_value_statement)     \
  ret.agg_func_.push_back(                                             \
      [](AggIntermediateData& x, StaticFieldRef v) -> void {           \
        if (x.size_ == 0) {                                            \
          init_statement;                                              \
        } else {                                                       \
          update_statement;                                            \
        }                                                              \
        x.size_ += 1;                                                  \
      });                                                              \
  ret.agg_final_func_.push_back(                                       \
      [i](std::span<AggIntermediateData*> A, Vector& result) -> void { \
        FitType(result, VectorType::Flat, type, A.size());             \
        for (uint32_t j = 0; j < A.size(); j++) {                      \
          result.Set(j, (final_value_statement));                      \
        }                                                              \
      })
    if (name == "max") {
      if (ty == LogicalType::INT) {
        AGGR_FUNC(
            LogicalType::INT, { x.data_.int_data = v.ReadInt(); },
            { x.data_.int_data = std::max(x.data_.int_data, v.ReadInt()); },
            A[j][i].data_.int_data);
      } else if (ty == LogicalType::FLOAT) {
        AGGR_FUNC(
            LogicalType::FLOAT, { x.data_.double_data = v.ReadFloat(); },
            {
              x.data_.double_data =
                  std::max(x.data_.double_data, v.ReadFloat());
            },
            A[j][i].data_.double_data);
      }
    }

    else if (name == "min") {
      if (ty == LogicalType::INT) {
        AGGR_FUNC(
            LogicalType::INT, { x.data_.int_data = v.ReadInt(); },
            { x.data_.int_data = std::min(x.data_.int_data, v.ReadInt()); },
            A[j][i].data_.int_data);
      } else if (ty == LogicalType::FLOAT) {
        AGGR_FUNC(
            LogicalType::FLOAT, { x.data_.double_data = v.ReadFloat(); },
            {
              x.data_.double_data =
                  std::min(x.data_.double_data, v.ReadFloat());
            },
            A[j][i].data_.double_data);
      }
    }

    else if (name == "avg") {
      if (ty == LogicalType::INT) {
        AGGR_FUNC(
            LogicalType::INT, { x.data_.int_data = v.ReadInt(); },
            { x.data_.int_data += v.ReadInt(); },
            A[j][i].data_.int_data / (double)A[j][i].size_);
      } else if (ty == LogicalType::FLOAT) {
        AGGR_FUNC(
            LogicalType::FLOAT, { x.data_.double_data = v.ReadFloat(); },
            { x.data_.double_data += v.ReadFloat(); },
            A[j][i].data_.double_data / (double)A[j][i].size_);
      }
    }

    else if (name == "count") {
      AGGR_FUNC(
          LogicalType::INT, {}, {}, StaticFieldRef::CreateInt(A[j][i].size_));
    }

    else if (name == "sum") {
      if (ty == LogicalType::INT) {
        AGGR_FUNC(
            LogicalType::INT, { x.data_.int_data = v.ReadInt(); },
            { x.data_.int_data += v.ReadInt(); }, A[j][i].data_.int_data);
      } else if (ty == LogicalType::FLOAT) {
        AGGR_FUNC(
            LogicalType::FLOAT, { x.data_.double_data = v.ReadFloat(); },
            { x.data_.double_data += v.ReadFloat(); },
            A[j][i].data_.double_data);
      }
    } else {
      throw DBException("Cannot recognize aggregation function name {}", name);
    }
#undef AGGR_FUNC
  }
  return ret;
}

void AggExprVecExecutor::Init() {
  expr_.Init();
  alloc_.Clear();
  for (auto& a : agg_para_)
    a.Init();
}

}  // namespace wing
