#include "jit/jitexecutor.hpp"

#include "jit/jitexpr.hpp"
#include "jit/jitmemory.hpp"
#include "jit/jitscan.hpp"
#include "jit/llvmheaders.hpp"
#include "type/tuple.hpp"

namespace wing {

class JitExecutor : public Executor {
 public:
  using GenerateAllFuncType = uint8_t* (*)(uint8_t* memory);
  using InitFuncType = void (*)(uint8_t* memory);
  JitExecutor(std::unique_ptr<llvm::orc::LLJIT>&& lljit, GenerateAllFuncType generate_all_func, std::unique_ptr<uint8_t>&& memory,
              std::unique_ptr<TupleStore>&& tuple_store, std::vector<std::unique_ptr<Iterator<const uint8_t*>>>&& iters)
      : lljit_(std::move(lljit)),
        generate_all_func_(generate_all_func),
        memory_(std::move(memory)),
        tuple_store_(std::move(tuple_store)),
        iters_(std::move(iters)) {}
  void Init() override {
    for (auto& a : iters_) a->Init();
  }
  InputTuplePtr Next() override { return generate_all_func_(memory_.get()); }

 private:
  std::unique_ptr<llvm::orc::LLJIT> lljit_;
  GenerateAllFuncType generate_all_func_;
  std::unique_ptr<uint8_t> memory_;
  std::unique_ptr<TupleStore> tuple_store_;
  std::vector<std::unique_ptr<Iterator<const uint8_t*>>> iters_;
};

// Generate blocks for each plan node
// And connect them.
JitData JitCodeGenerate(llvm::LLVMContext& C, llvm::Function* F, llvm::Value* input, JitMemory& memory, const PlanNode* plan, DB& db, size_t txn_id) {
  using namespace llvm;
  if (plan == nullptr) {
    throw DBException("Invalid PlanNode.");
  }

  if (plan->type_ == PlanType::Project) {
    auto project_plan = static_cast<const ProjectPlanNode*>(plan);
    auto ch = JitCodeGenerate(C, F, input, memory, project_plan->ch_.get(), db, txn_id);
    auto ret = JitGenerateProject(C, F, ch.values_, project_plan->ch_->output_schema_, project_plan->output_exprs_);
    {
      IRBuilder<> builder(ch.success_);
      builder.CreateBr(ret.entry_);
    }
    return {ret.values_, ch.entry_, ch.next_, ret.entry_, ch.failure_};
  }

  else if (plan->type_ == PlanType::Filter) {
    auto filter_plan = static_cast<const FilterPlanNode*>(plan);
    auto ch = JitCodeGenerate(C, F, input, memory, filter_plan->ch_.get(), db, txn_id);
    auto filter = JitGenerateFilter(C, F, ch.values_, filter_plan->ch_->output_schema_, filter_plan->predicate_.GenExpr());
    {
      IRBuilder<> builder(ch.success_);
      builder.CreateBr(filter.entry_);
    }
    {
      IRBuilder<> builder(filter.failure_);
      builder.CreateBr(ch.next_);
    }
    return {ch.values_, ch.entry_, ch.next_, filter.success_, ch.failure_};
  }

  else if (plan->type_ == PlanType::SeqScan) {
    auto seqscan_plan = static_cast<const SeqScanPlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(seqscan_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", seqscan_plan->table_name_);
    }
    return JitGenerateSeqScan(C, F, input, memory, seqscan_plan->output_schema_, db.GetIterator(txn_id, seqscan_plan->table_name_), seqscan_plan->predicate_.GenExpr());
  }

  throw DBException("Unsupported plan node.");
}

// Create a module and output the results.
llvm::orc::ThreadSafeModule CreateMyModule(JitMemory& memory, const PlanNode* plan, DB& db, size_t txn_id) {
  using namespace llvm;
  auto pC = std::make_unique<LLVMContext>();
  auto& C = *pC;
  auto M = std::make_unique<Module>("jit-executor", C);
  // create next func
  Function* next_func =
      Function::Create(FunctionType::get(Type::getInt8PtrTy(C), {Type::getInt8PtrTy(C)}, false), Function::ExternalLinkage, "next", M.get());
  BasicBlock* entry = BasicBlock::Create(C, "entry_block", next_func);
  BasicBlock* ret = BasicBlock::Create(C, "return_block", next_func);
  BasicBlock* print = BasicBlock::Create(C, "print_block", next_func);
  auto tuple_store = std::make_unique<TupleStore>(plan->output_schema_);
  auto tuple_store_addr = reinterpret_cast<size_t>(tuple_store.get());
  memory.AddTupleStore(std::move(tuple_store));
  Value* tuple_store_ptr;
  Value* input = &*next_func->arg_begin();
  auto jit_data = JitCodeGenerate(C, next_func, input, memory, plan, db, txn_id);

  // Allocate a temporary memory region for output data.
  // We can also allocate on stack.
  // This is an array of StaticFieldRef.
  auto result_addr = memory.Allocate(jit_data.values_.size() * 8);

  {
    IRBuilder<> builder(entry);
    // Get TupleStore pointer.
    tuple_store_ptr = builder.CreateBitOrPointerCast(ConstantInt::get(C, APInt(64, tuple_store_addr)), Type::getInt8PtrTy(C), "tuple_store");
    builder.CreateBr(jit_data.entry_);
  }
  {
    IRBuilder<> builder(jit_data.failure_);
    builder.CreateBr(ret);
  }
  {
    IRBuilder<> builder(jit_data.success_);
    builder.CreateBr(print);
  }
  {
    IRBuilder<> builder(print);
    auto insert_func_type = FunctionType::get(Type::getVoidTy(C), {Type::getInt8PtrTy(C), Type::getInt8PtrTy(C)}, false);
    auto insert_func = M->getOrInsertFunction("_wing_insert_into_tuple_store", insert_func_type);
    // Store the outputs to temporary memory region.
    for (uint32_t i = 0; auto& v : jit_data.values_) {
      Value* ptr = builder.CreateGEP(Type::getInt8Ty(C), input, ConstantInt::get(C, APInt(64, result_addr + i * 8)));
      // Integer values or pointer values are all int64_t.
      if (plan->output_schema_[i].type_ != FieldType::FLOAT64) {
        ptr = builder.CreateBitOrPointerCast(ptr, Type::getInt64PtrTy(C));
        builder.CreateStore(builder.CreateBitOrPointerCast(v, Type::getInt64Ty(C)), ptr);
      } else {
        ptr = builder.CreateBitCast(ptr, Type::getDoublePtrTy(C));
        builder.CreateStore(v, ptr);
      }
      i += 1;
    }
    Value* ptr = builder.CreateGEP(Type::getInt8Ty(C), input, ConstantInt::get(C, APInt(64, result_addr)));
    builder.CreateCall(insert_func, {tuple_store_ptr, ptr});
    builder.CreateBr(jit_data.entry_);
  }
  {
    IRBuilder<> builder(ret);
    builder.CreateRet(tuple_store_ptr);
  }
  // Output the IR to stderr.
  // M->print(llvm::errs(), nullptr);

  // https://llvm.org/docs/tutorial/MyFirstLanguageFrontend/LangImpl04.html
  // Optimization Passes
  {
    // Create a new pass manager attached to it.
    auto TheFPM = std::make_unique<legacy::FunctionPassManager>(M.get());
    // Do simple "peephole" optimizations and bit-twiddling optzns.
    TheFPM->add(createInstructionCombiningPass());
    // Reassociate expressions.
    TheFPM->add(createReassociatePass());
    // Eliminate Common SubExpressions.
    TheFPM->add(createGVNPass());
    // Simplify the control flow graph (deleting unreachable blocks, etc).
    TheFPM->add(createCFGSimplificationPass());
    TheFPM->add(createGVNPass());
    TheFPM->add(createInstructionCombiningPass());
    TheFPM->doInitialization();
    TheFPM->run(*next_func);
  }

  // After optimization
  // M->print(llvm::errs(), nullptr);

  verifyModule(*M);

  return llvm::orc::ThreadSafeModule(std::move(M), std::move(pC));
}

// Functions used by LLVM
// Since C++ function names are compiler specified, we use C function names.
// In LLVM there's no void* type, so we all use uint8_t*(i.e. i8*).
// TODO: add hash table functions.
extern "C" {
static void _wing_insert_into_tuple_store(uint8_t* a, uint8_t* data) { reinterpret_cast<TupleStore*>(a)->Append(data); }
static uint8_t* _wing_iter_next(uint8_t* iter) { return const_cast<uint8_t*>(reinterpret_cast<Iterator<const uint8_t*>*>(iter)->Next()); }
}

std::unique_ptr<Executor> JitExecutorGenerator::Generate(const PlanNode* plan, DB& db, size_t txn_id) {
  using namespace llvm;
  using namespace llvm::orc;
  // Initialize
  InitializeNativeTarget();
  InitializeNativeTargetAsmPrinter();
  auto J = LLJITBuilder().create();
  if (!J) throw DBException("LLJit create failed.");
  auto lljit = std::move(J.get());
  // Store global variables in memory.
  JitMemory memory;
  auto M = CreateMyModule(memory, plan, db, txn_id);
  auto err = lljit->addIRModule(std::move(M));
  if (err) {
    llvm::errs() << err;
    throw DBException("Add IR Module failed.");
  }
  // https://stackoverflow.com/questions/57612173/llvm-jit-symbols-not-found
  // Add external functions
  {
    auto& dl = lljit->getDataLayout();
    MangleAndInterner Mangle(lljit->getExecutionSession(), dl);
    auto& jd = lljit->getMainJITDylib();
    auto def_func = [&](auto&& func_name, auto&& func_pointer) {
      auto ret =
          jd.define(absoluteSymbols({{Mangle(func_name), JITEvaluatedSymbol(pointerToJITTargetAddress(func_pointer), JITSymbolFlags::Exported)}}));
      if (err) {
        llvm::errs() << err;
        throw DBException("Add export function \'{}\' failed.", func_name);
      }
    };
    def_func("_wing_insert_into_tuple_store", &_wing_insert_into_tuple_store);
    def_func("_wing_iter_next", &_wing_iter_next);
    def_func("memcmp", &memcmp);
  }

  auto handle = lljit->lookup("next");
  if (!handle) throw DBException("Cannot find JIT function.");
  auto next_func = reinterpret_cast<JitExecutor::GenerateAllFuncType>(handle.get().getAddress());

  return std::make_unique<JitExecutor>(std::move(lljit), next_func, memory.GetMemory(), std::move(memory.GetTupleStore()),
                                       std::move(memory.GetIterators()));
}
}  // namespace wing