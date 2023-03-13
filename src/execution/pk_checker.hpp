#ifndef SAKURA_PK_CHECKER_H__
#define SAKURA_PK_CHECKER_H__

#include "catalog/db.hpp"
#include "execution/exprdata.hpp"
#include "storage/storage.hpp"

namespace wing {
/**
 * (P)rimary(K)eyChecker. Used to check whether the refcounts is > 0.
 */
class PKChecker {
 public:
  PKChecker(std::string_view table_name, bool pk_hide_flag, size_t txn_id, DB& db) {
    is_pk_unreferred_ = pk_hide_flag;
    if (is_pk_unreferred_) return;
    pk_refcounts_ = db.GetSearchHandle(txn_id, DB::GenRefTableName(table_name));
  }

  void DeleteCheck(std::string_view pk_view) {
    if (is_pk_unreferred_) return;
    auto ret = pk_refcounts_->Search(pk_view);
    if (ret && InputTuplePtr(ret).Read<int64_t>(0) > 0) {
      throw DBException("Some primary keys are referred in other tables.");
    }
  }

 private:
  std::unique_ptr<SearchHandle> pk_refcounts_;
  bool is_pk_unreferred_{false};
};
}  // namespace wing

#endif