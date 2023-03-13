#ifndef SAKURA_JIT_SEQSCAN_H__
#define SAKURA_JIT_SEQSCAN_H__

#include "jit/jitexpr.hpp"
#include "jit/jitmemory.hpp"
#include "jit/llvmheaders.hpp"
#include "plan/plan.hpp"
#include "type/tuple.hpp"

namespace wing {

// Generate llvm::Values from a memory pointer pointing to raw data.
std::vector<llvm::Value*> JitGenerateValuesFromTuple(llvm::Value* input, const OutputSchema& input_schema, llvm::IRBuilder<>& builder) {
  using namespace llvm;
  auto& C = builder.getContext();
  std::vector<llvm::Value*> ret;
  size_t offset = 0, id_in_str = 0;

  for (auto& a : input_schema.GetCols()) {
    auto pos = builder.CreateInBoundsGEP(Type::getInt8Ty(C), input, ConstantInt::get(C, APInt(32, Tuple::GetOffsetOfStaticField(offset))));
    if (a.type_ == FieldType::INT32) {
      auto i32_ptr = builder.CreateBitCast(pos, Type::getInt32PtrTy(C));
      ret.push_back(builder.CreateSExtOrTrunc(builder.CreateLoad(Type::getInt32Ty(C), i32_ptr), Type::getInt64Ty(C)));
      offset += a.size_;
    } else if (a.type_ == FieldType::INT64) {
      auto i64_ptr = builder.CreateBitCast(pos, Type::getInt64PtrTy(C));
      ret.push_back(builder.CreateLoad(Type::getInt64Ty(C), i64_ptr));
      offset += a.size_;
    } else if (a.type_ == FieldType::FLOAT64) {
      auto f64_ptr = builder.CreateBitCast(pos, Type::getDoublePtrTy(C));
      ret.push_back(builder.CreateLoad(Type::getDoubleTy(C), f64_ptr));
      offset += a.size_;
    } else if (a.type_ == FieldType::CHAR || a.type_ == FieldType::VARCHAR) {
      auto table_pos =
          builder.CreateGEP(Type::getInt8Ty(C), input, ConstantInt::get(C, APInt(32, Tuple::GetOffsetsOfStrings(offset, id_in_str))));
      auto i32_ptr = builder.CreateBitCast(table_pos, Type::getInt32PtrTy(C));
      Value* string_offset = builder.CreateLoad(Type::getInt32Ty(C), i32_ptr);
      ret.push_back(builder.CreateGEP(Type::getInt8Ty(C), input, builder.CreateZExtOrTrunc(string_offset, Type::getInt32Ty(C))));
      id_in_str += 1;
    }
  }
  return ret;
}

/**
 * 
 * I think the following 4 interfaces can cover all needs.
 * entry_: The begin point. Do some initialization.
 * next_: For loops (seqscan, join, aggregate and so on), fetch the next tuple from tuplestore.
 * success_: For filter, point out to some basic blocks that print out the tuple.
 *            For loops, continue the loop.
 * failure_: For filter, go ahead. For loops, jump out of the loop.
 * 
 * But I never implemented Join, Aggregate... So you can modify them.
 * 
 * For example, the final result is something like:  
 * for (auto a : A)
 *  for (auto b : B)
 *   if (f(a, b))
 *    AB.append(a, b);
 * for (auto c : C)
 *  for (auto ab : AB)
 *    if (g(ab, c))
 *      print(ab, c)
 * 
 * The for loops can be separated into 2 parts. For the last parts, we can draw the graph:
 * 
 *  +------------+
 *  |    c = 0   | （c_entry)
 *  +------------+
 *        |
 *        +------------+
 *        |    ab = 0  | (ab_entry)
 *        +------------+
 *               |
 *               +---------------+
 *               |  calculate g  | (g_entry)
 *               +---------------+
 *                   |        |
 *                   |        +----------------------------+
 *                   |        |   <g_success_> (point out) |
 *                   |        +----------------------------+
 *                   |
 *               +----------------------------------+
 *               | <g_failure_> (connect to ab_next)|
 *               +----------------------------------+
 *               |
*         +--------------------------------------------------------+ 
 *         |  ab = ab + 1                                          | (ab_next)
 *         |  ab is end ? goto <ab_failure> (connect to c_next)    |
 *         |            : goto <ab_success_> (connect to g_entry)  |
 *         +-------------------------------------------------------+
 *         |
 *  +---------------------------------------------------------+
 *  |  c = c + 1                                              | (c_next)
 *  |  c is end ? goto to <c_failure_>                        |
 *  |           : goto to <c_success_> (connect to ab_entry)  |
 *  +---------------------------------------------------------+
*/
class JitData {
 public:
  std::vector<llvm::Value*> values_;
  llvm::BasicBlock* entry_;
  llvm::BasicBlock* next_;
  llvm::BasicBlock* success_;
  llvm::BasicBlock* failure_;
};

JitData JitGenerateSeqScan(llvm::LLVMContext& C, llvm::Function* F, llvm::Value* input, JitMemory& memory, const OutputSchema& input_schema,
                        std::unique_ptr<Iterator<const uint8_t*>> scan_iter, const std::unique_ptr<Expr>& predicate) {
  using namespace llvm;
  auto scan_iter_addr = reinterpret_cast<size_t>(scan_iter.get());
  memory.AddIterator(std::move(scan_iter));
  auto iter_next_func_type = FunctionType::get(Type::getInt8PtrTy(C), {Type::getInt8PtrTy(C)}, false);
  auto iter_next_func = F->getParent()->getOrInsertFunction("_wing_iter_next", iter_next_func_type);
  BasicBlock* entry = BasicBlock::Create(C, "scan_entry", F);
  // If iterator doesn't return null.
  BasicBlock* if_not_null = BasicBlock::Create(C, "scan_if_not_null", F);
  // If iterator doesn't return null and pass the predicate.
  BasicBlock* if_pass_predicate = BasicBlock::Create(C, "scan_if_pass_predicate", F);
  // If iterator returns null.
  BasicBlock* if_null = BasicBlock::Create(C, "scan_if_null", F);
  // Values from raw data.
  std::vector<Value*> values;
  Value* tuple;

  {
    IRBuilder<> builder(entry);
    Value* ptr = builder.CreateBitOrPointerCast(ConstantInt::get(C, APInt(64, scan_iter_addr)), Type::getInt8PtrTy(C), "iter");
    tuple = builder.CreateCall(iter_next_func, {ptr}, "tuple");
    builder.CreateCondBr(builder.CreateICmpEQ(tuple, ConstantPointerNull::get(Type::getInt8PtrTy(C))), if_null, if_not_null);
  }

  {
    IRBuilder<> builder(if_not_null);
    values = JitGenerateValuesFromTuple(tuple, input_schema, builder);
    if (predicate) {
      // Get predicate value
      Value* expr_value = JitGenerateExpr(predicate.get(), input_schema, values, builder);
      expr_value->setName("predicate_value");
      Value* flag = builder.CreateICmpEQ(expr_value, ConstantInt::get(C, APInt(64, 0)), "flag");
      builder.CreateCondBr(flag, entry, if_pass_predicate);  
    } else {
      builder.CreateBr(if_pass_predicate);
    }
  }
  {
    IRBuilder<> builder(if_null);
  }
  return JitData{values, entry, entry, if_pass_predicate, if_null};
}

JitData JitGenerateFilter(llvm::LLVMContext& C, llvm::Function* F, const std::vector<llvm::Value*>& values, const OutputSchema& input_schema,
                          const std::unique_ptr<Expr>& expr) {
  using namespace llvm;
  BasicBlock* entry = BasicBlock::Create(C, "filter_entry", F);
  BasicBlock* if_true = BasicBlock::Create(C, "filter_if_true", F);
  BasicBlock* if_false = BasicBlock::Create(C, "filter_if_false", F);
  {
    IRBuilder<> builder(entry);
    Value* expr_value = JitGenerateExpr(expr.get(), input_schema, values, builder);
    expr_value->setName("expr_value");
    Value* flag = builder.CreateICmpEQ(expr_value, ConstantInt::get(C, APInt(64, 0)), "flag");
    builder.CreateCondBr(flag, if_false, if_true);
  }
  return JitData{values, entry, nullptr, if_true, if_false};
}

JitData JitGenerateProject(llvm::LLVMContext& C, llvm::Function* F, const std::vector<llvm::Value*>& values, const OutputSchema& input_schema,
                           const std::vector<std::unique_ptr<Expr>>& exprs) {
  using namespace llvm;
  BasicBlock* entry = BasicBlock::Create(C, "project_entry", F);
  std::vector<Value*> new_values;
  {
    IRBuilder<> builder(entry);
    for (uint32_t i = 0; auto& a : exprs) {
      Value* expr_value = JitGenerateExpr(a.get(), input_schema, values, builder);
      new_values.push_back(expr_value);
      expr_value->setName(fmt::format("output_{}", i++));
    }
  }
  return JitData{new_values, entry, nullptr, nullptr, nullptr};
}

}  // namespace wing

#endif