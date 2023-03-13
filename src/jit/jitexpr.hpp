#ifndef SAKURA_JITEXPR_H__
#define SAKURA_JITEXPR_H__

#include "jit/llvmheaders.hpp"
#include "parser/expr.hpp"

namespace wing {

std::pair<llvm::Value*, llvm::Value*> JitGetStringFromPointer(llvm::Value* input, llvm::IRBuilder<>& builder) {
  using namespace llvm;
  auto& C = builder.getContext();
  // Size is stored in the first 4B.
  Value* size = builder.CreateLoad(Type::getInt32Ty(C), builder.CreateBitOrPointerCast(input, Type::getInt32PtrTy(C)));
  // Next is the string.
  Value* ptr = builder.CreateGEP(Type::getInt8Ty(C), builder.CreateBitOrPointerCast(input, Type::getInt8PtrTy(C)),
                                 ConstantInt::get(C, APInt(32, sizeof(uint32_t))));
  size = builder.CreateSub(size, ConstantInt::get(C, APInt(32, 4)));
  return {size, ptr};
}

// Create string comparation
llvm::Value* JitGetStringCompareResult(llvm::Value* L, llvm::Value* R, llvm::IRBuilder<>& builder) {
  using namespace llvm;
  auto& C = builder.getContext();
  auto lhs = JitGetStringFromPointer(L, builder);
  auto rhs = JitGetStringFromPointer(R, builder);
  // Value* sz = builder.CreateZExtOrTrunc(lhs.first, Type::getInt64Ty(C));
  // Get minimial length
  Value* sz = builder.CreateSelect(builder.CreateICmpULT(lhs.first, rhs.first), lhs.first, rhs.first);
  sz = builder.CreateZExtOrTrunc(sz, Type::getInt64Ty(C));

  auto cmp_func_type = FunctionType::get(Type::getInt32Ty(C), {Type::getInt8PtrTy(C), Type::getInt8PtrTy(C), Type::getInt64Ty(C)}, false);
  auto cmp_func = builder.GetInsertBlock()->getModule()->getOrInsertFunction("memcmp", cmp_func_type);
  // Call memcmp
  Value* c = builder.CreateCall(cmp_func, {lhs.second, rhs.second, sz});
  // Comparate two lengths.
  // I believe LLVM can optimize them out...
  Value* lt = builder.CreateICmpULT(lhs.first, rhs.first);
  Value* gt = builder.CreateICmpUGT(lhs.first, rhs.first);
  Value* c0 = builder.CreateICmpEQ(c, ConstantInt::get(C, APInt(32, 0)));
  // Results
  Value* n1 = ConstantInt::get(C, APInt(32, -1));
  Value* p1 = ConstantInt::get(C, APInt(32, 1));
  Value* zero = ConstantInt::get(C, APInt(32, 0));
  // c0 ? (lt ? -1 : (gt ? 1 : 0)) : c
  return builder.CreateSelect(c0, builder.CreateSelect(lt, n1, builder.CreateSelect(gt, p1, zero)), c);
}

// Generate output llvm::Value from input llvm::Value. 
// We don't support aggregate functions now.
llvm::Value* JitGenerateExpr(const Expr* expr, const OutputSchema& input_schema, const std::vector<llvm::Value*> input_value,
                             llvm::IRBuilder<>& builder) {
  using namespace llvm;
  auto& C = builder.getContext();
  if (expr->type_ == ExprType::LITERAL_INTEGER) {
    auto this_expr = static_cast<const LiteralIntegerExpr*>(expr);
    return ConstantInt::get(C, APInt(64, this_expr->literal_value_));
  } else if (expr->type_ == ExprType::LITERAL_FLOAT) {
    auto this_expr = static_cast<const LiteralFloatExpr*>(expr);
    return ConstantFP::get(C, APFloat(this_expr->literal_value_));
  } else if (expr->type_ == ExprType::LITERAL_STRING) {
    auto this_expr = static_cast<const LiteralStringExpr*>(expr);
    auto x = std::unique_ptr<StaticStringField>(StaticStringField::Generate(this_expr->literal_value_));
    return builder.CreateGlobalStringPtr(std::string_view(reinterpret_cast<const char*>(x.get()), x->size_));
  } else if (expr->type_ == ExprType::BINOP) {
    auto this_expr = static_cast<const BinaryExpr*>(expr);
    auto lhs = JitGenerateExpr(this_expr->ch0_.get(), input_schema, input_value, builder);
    auto rhs = JitGenerateExpr(this_expr->ch1_.get(), input_schema, input_value, builder);
#define GEN_FUNC(optype, builder_func)           \
  if (this_expr->op_ == OpType::optype) {        \
    Value* ret = builder.builder_func(lhs, rhs); \
    return ret;                                  \
  }
    if (this_expr->ret_type_ == RetType::INT) {
      // Look at https://llvm.org/doxygen/classllvm_1_1IRBuilder.html
      GEN_FUNC(ADD, CreateAdd);
      GEN_FUNC(SUB, CreateSub);
      GEN_FUNC(MUL, CreateMul);
      // All integers are signed.
      GEN_FUNC(DIV, CreateSDiv);
      GEN_FUNC(MOD, CreateSRem);
      GEN_FUNC(BITAND, CreateAnd);
      GEN_FUNC(BITOR, CreateOr);
      GEN_FUNC(BITXOR, CreateXor);
      GEN_FUNC(BITLSH, CreateShl);
      GEN_FUNC(BITRSH, CreateAShr);
    } else {
      GEN_FUNC(ADD, CreateFAdd);
      GEN_FUNC(SUB, CreateFSub);
      GEN_FUNC(MUL, CreateFMul);
      GEN_FUNC(DIV, CreateFDiv);
    }
#undef GEN_FUNC
  } else if (expr->type_ == ExprType::BINCONDOP) {
    auto this_expr = static_cast<const BinaryExpr*>(expr);
    auto lhs = JitGenerateExpr(this_expr->ch0_.get(), input_schema, input_value, builder);
    auto rhs = JitGenerateExpr(this_expr->ch1_.get(), input_schema, input_value, builder);
#define GEN_FUNC2(optype, builder_func)                        \
  if (this_expr->op_ == OpType::optype) {                      \
    Value* ret = builder.builder_func(lhs, rhs);               \
    ret = builder.CreateZExtOrTrunc(ret, Type::getInt64Ty(C)); \
    return ret;                                                \
  }
    if (this_expr->ch0_->ret_type_ == RetType::INT) {
      // All integers are signed.
      GEN_FUNC2(LT, CreateICmpSLT);
      GEN_FUNC2(GT, CreateICmpSGT);
      GEN_FUNC2(LEQ, CreateICmpSLE);
      GEN_FUNC2(GEQ, CreateICmpSGE);
      GEN_FUNC2(EQ, CreateICmpEQ);
      GEN_FUNC2(NEQ, CreateICmpNE);
      // LLVM11 doesn't have CreateLogicalAnd/CreateLogicalOr.
      // So we use CreateSelect.
      if (this_expr->op_ == OpType::AND) {
        lhs = builder.CreateICmpNE(lhs, ConstantInt::get(C, APInt(64, 0)));
        return builder.CreateSelect(lhs, rhs, ConstantInt::get(C, APInt(64, 0)));
      } else if (this_expr->op_ == OpType::OR) {
        lhs = builder.CreateICmpNE(lhs, ConstantInt::get(C, APInt(64, 0)));
        return builder.CreateSelect(lhs, ConstantInt::get(C, APInt(64, 1)), rhs);
      }
    } else if (this_expr->ch0_->ret_type_ == RetType::FLOAT) {
      // All floats are ordered (i.e. not NaNs.)
      GEN_FUNC2(LT, CreateFCmpOLT);
      GEN_FUNC2(GT, CreateFCmpOGT);
      GEN_FUNC2(LEQ, CreateFCmpOLE);
      GEN_FUNC2(GEQ, CreateFCmpOGE);
      GEN_FUNC2(EQ, CreateFCmpOEQ);
      GEN_FUNC2(NEQ, CreateFCmpONE);
#undef GEN_FUNC2
    } else if (this_expr->ch0_->ret_type_ == RetType::STRING) {
#define GEN_FUNC3(optype, builder_func)                                                                                 \
  if (this_expr->op_ == OpType::optype) {                                                                               \
    Value* ret = builder.builder_func(JitGetStringCompareResult(lhs, rhs, builder), ConstantInt::get(C, APInt(32, 0))); \
    ret = builder.CreateZExtOrTrunc(ret, Type::getInt64Ty(C));                                                          \
    return ret;                                                                                                         \
  }
      GEN_FUNC3(LT, CreateICmpSLT);
      GEN_FUNC3(GT, CreateICmpSGT);
      GEN_FUNC3(LEQ, CreateICmpSLE);
      GEN_FUNC3(GEQ, CreateICmpSGE);
      GEN_FUNC3(EQ, CreateICmpEQ);
      GEN_FUNC3(NEQ, CreateICmpNE);
#undef GEN_FUNC3
    }
  } else if (expr->type_ == ExprType::CAST) {
    auto this_expr = static_cast<const CastExpr*>(expr);
    if (expr->ret_type_ == RetType::FLOAT && this_expr->ch0_->ret_type_ == RetType::INT) {
      auto lhs = JitGenerateExpr(this_expr->ch0_.get(), input_schema, input_value, builder);
      Value* ret = builder.CreateSIToFP(lhs, Type::getDoubleTy(C));
      return ret;
    } else if (expr->ret_type_ == RetType::INT && this_expr->ch0_->ret_type_ == RetType::FLOAT) {
      auto lhs = JitGenerateExpr(this_expr->ch0_.get(), input_schema, input_value, builder);
      // Integer is always signed.
      Value* ret = builder.CreateFPToSI(lhs, Type::getInt64Ty(C));
      return ret;
    } else {
      DB_ERR("Internal Error: Invalid CastExpr.");
    }
  } else if (expr->type_ == ExprType::UNARYOP) {
    auto this_expr = static_cast<const UnaryExpr*>(expr);
    auto lhs = JitGenerateExpr(this_expr->ch0_.get(), input_schema, input_value, builder);
    if (expr->ret_type_ == RetType::FLOAT) {
      Value* ret = builder.CreateFNeg(lhs);
      return ret;
    } else if (expr->ret_type_ == RetType::INT) {
      Value* ret = builder.CreateNeg(lhs);
      return ret;
    }
  } else if (expr->type_ == ExprType::UNARYCONDOP) {
    auto this_expr = static_cast<const UnaryConditionExpr*>(expr);
    auto lhs = JitGenerateExpr(this_expr->ch0_.get(), input_schema, input_value, builder);
    Value* ret = builder.CreateNot(lhs);
    return ret;
  } else if (expr->type_ == ExprType::COLUMN) {
    auto this_expr = static_cast<const ColumnExpr*>(expr);
    uint32_t id = ~0u;
    for (uint32_t index = 0; auto& col : input_schema.GetCols()) {
      if (col.id_ == this_expr->id_in_column_name_table_) {
        id = index;
        break;
      }
      index += 1;
    }
    if (id == (~0u)) {
      DB_ERR("Internal Error: Expression contains invalid parameters.");
    }
    return input_value[id];
  }

  DB_ERR("Internal Error: Invalid Expr.");
}  // namespace wing

}  // namespace wing

#endif