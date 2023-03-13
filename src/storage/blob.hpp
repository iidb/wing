#ifndef BLOB_H_
#define BLOB_H_

#include "storage/page-manager.hpp"

namespace wing {
/* Root: | Size : size_t | Data | Next page : pgid_t |
 * Other: | Data | Next page : pgid_t |
 */
class Blob {
 public:
  static Blob Create(PageManager& pgm) {
    Blob blob(pgm, pgm.Allocate());
    blob.Init();
    return blob;
  }
  static Blob Open(PageManager& pgm, pgid_t meta_pgid) {
    return Blob(pgm, meta_pgid);
  }
  inline void Destroy() { Free(head_); }
  inline pgid_t MetaPageID() const { return head_; }
  void Rewrite(std::string_view value);
  std::string Read();
 private:
  Blob(PageManager& pgm, pgid_t meta_pgid)
    : pgm_(pgm), head_(meta_pgid) {}
  void Init();
  inline PlainPage GetHeadPage() {
    return pgm_.GetPlainPage(head_);
  }
  inline size_t Size() {
    size_t size;
    GetHeadPage().Read(&size, 0, sizeof(size));
    return size;
  }
  inline void UpdateSize(size_t size) {
    GetHeadPage().Write(0,
      std::string_view(reinterpret_cast<const char *>(&size), sizeof(size)));
  }
  inline pgid_t NextPageID(const PlainPage& page) {
    pgid_t next;
    page.Read(&next, Page::SIZE - sizeof(pgid_t), sizeof(next));
    return next;
  }
  inline PlainPage NextPage(const PlainPage& page) {
    return pgm_.GetPlainPage(NextPageID(page));
  }
  inline void UpdateNext(PlainPage& page, pgid_t next) {
    page.Write(Page::SIZE - sizeof(pgid_t),
      std::string_view(reinterpret_cast<const char *>(&next), sizeof(next)));
  }
  void Free(pgid_t cur);
  PageManager& pgm_;
  pgid_t head_;
};

} // namespace wing

#endif // BLOB_H_
