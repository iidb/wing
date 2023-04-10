#include "storage/blob.hpp"

namespace wing {
void Blob::Init() {
  PlainPage root = pgm_.GetPlainPage(head_);
  UpdateSize(0);
  UpdateNext(root, 0);
}
void Blob::Rewrite(std::string_view value) {
  PlainPage cur = pgm_.GetPlainPage(head_);
  UpdateSize(value.size());
  const char *src = value.data();
  size_t size = value.size();
  size_t max_to_copy = Page::SIZE - sizeof(size_t) - sizeof(pgid_t);
  if (size <= max_to_copy) {
    cur.Write(sizeof(size_t), std::string_view(src, size));
    Free(NextPageID(cur));
    UpdateNext(cur, 0);
    return;
  }
  cur.Write(sizeof(size_t), std::string_view(src, max_to_copy));
  size -= max_to_copy;
  src += max_to_copy;
  max_to_copy = Page::SIZE - sizeof(pgid_t);
  for (;;) {
    assert(size > 0);
    pgid_t next = NextPageID(cur);
    if (next == 0) {
      next = pgm_.Allocate();
      UpdateNext(cur, next);
      cur = pgm_.GetPlainPage(next);
      UpdateNext(cur, 0);
    } else {
      cur = pgm_.GetPlainPage(next);
    }
    if (size <= max_to_copy) {
      cur.Write(0, std::string_view(src, size));
      break;
    }
    cur.Write(0, std::string_view(src, max_to_copy));
    size -= max_to_copy;
    src += max_to_copy;
  }
  Free(NextPageID(cur));
  UpdateNext(cur, 0);
}
std::string Blob::Read() {
  PlainPage cur = pgm_.GetPlainPage(head_);
  size_t size = Size();
  auto src = cur.as_ptr() + sizeof(size_t);
  size_t max_to_copy = Page::SIZE - sizeof(size_t) - sizeof(pgid_t);
  std::string ret;
  if (size <= max_to_copy) {
    ret.append(src, size);
    return ret;
  }
  ret.append(src, max_to_copy);
  size -= max_to_copy;
  max_to_copy = Page::SIZE - sizeof(pgid_t);
  for (;;) {
    assert(size > 0);
    cur = NextPage(cur);
    if (size <= max_to_copy) {
      ret.append(cur.as_ptr(), size);
      break;
    }
    ret.append(cur.as_ptr(), max_to_copy);
    size -= max_to_copy;
  }
  return ret;
}
void Blob::Free(pgid_t cur) {
  while (cur) {
    pgid_t next = NextPageID(pgm_.GetPlainPage(cur));
    pgm_.Free(cur);
    cur = next;
  }
}
}  // namespace wing
