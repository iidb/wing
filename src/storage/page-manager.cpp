#include "page-manager.hpp"
#include "common/logging.hpp"
#include <memory>

namespace wing {

PageManager::~PageManager() {
  // Flush free list standby buffer
  if (free_list_buf_standby_full_) {
    if (free_list_buf_used_ != 0) {
      free_list_buf_used_ -= 1;
      pgid_t pgid = free_list_buf_[free_list_buf_used_];
      FlushFreeListStandby(pgid);
    } else {
      std::swap(free_list_buf_, free_list_buf_standby_);
      free_list_buf_used_ = PGID_PER_PAGE;
      free_list_buf_standby_full_ = false;
    }
  }
  // Flush free list buffer
  if (free_list_buf_used_ != 0) {
    free_list_buf_used_ -= 1;
    pgid_t pgid = free_list_buf_[free_list_buf_used_];
    file_.seekp(pgid * Page::SIZE);
    file_.write(reinterpret_cast<const char *>(free_list_buf_),
      free_list_buf_used_ * sizeof(pgid_t));
    pgid_t head = FreeListHead();
    file_.seekp((pgid + 1) * Page::SIZE - sizeof(pgid_t));
    file_.write(reinterpret_cast<const char *>(&head), sizeof(head));
    FreeListHead() = pgid;
    FreePagesInHead() = free_list_buf_used_;
    free_list_buf_used_ = 0;
  }
  // Flush dirty pages
  std::vector<pgid_t> pages;
  pages.reserve(buf_.size());
  for (const auto& buf : buf_)
    pages.push_back(buf.first);
  std::sort(pages.begin(), pages.end());
  assert(pages[0] == 0);
  assert(buf_[0].refcount == 1);
  buf_[0].refcount = 0;
  for (pgid_t i : pages) {
    auto it = buf_.find(i);
    assert(it != buf_.end());
    assert(it->second.refcount == 0);
    if (it->second.dirty) {
      file_.seekp(i * Page::SIZE);
      file_.write(it->second.addr(), Page::SIZE);
    }
  }
}

auto PageManager::Create(
  std::filesystem::path path, size_t max_buf_pages
) -> std::unique_ptr<PageManager> {
  std::ofstream f(path); // Used to create the file
  auto pgm = std::unique_ptr<PageManager>(
    new PageManager(path, std::fstream(path), max_buf_pages));
  pgm->Init();
  return pgm;
}

auto PageManager::Open(
  std::filesystem::path path, size_t max_buf_pages
) -> Result<std::unique_ptr<PageManager>, io::Error> {
  std::fstream file(path);
  // TODO: Detect more detailed reason
  if (!file.good())
    return io::Error::New(io::ErrorKind::Other,
      "Fail to open file " + path.string());
  auto pgm = std::unique_ptr<PageManager>(
    new PageManager(path, std::move(file), max_buf_pages));
  auto ret = pgm->Load();
  if (ret.has_value())
    return std::move(ret.value());
  return pgm;
}

pgid_t PageManager::__Allocate() {
  if (free_list_buf_used_ == 0) {
    if (free_list_buf_standby_full_) {
      std::swap(free_list_buf_, free_list_buf_standby_);
      free_list_buf_standby_full_ = false;
      free_list_buf_used_ = PGID_PER_PAGE - 1;
      return free_list_buf_[free_list_buf_used_];
    }
    pgid_t pgid = FreeListHead();
    if (pgid != 0) {
      file_.seekg(pgid * Page::SIZE);
      free_list_buf_used_ = PGID_PER_PAGE;
      file_.read(reinterpret_cast<char *>(free_list_buf_),
        free_list_buf_used_ * sizeof(pgid_t));
      pgid_t head;
      file_.read(reinterpret_cast<char *>(&head), sizeof(pgid_t));
      FreeListHead() = head;
      return free_list_buf_[--free_list_buf_used_];
    }
    pgid_t ret = PageNum();
    PageNum() += 1;
    std::filesystem::resize_file(path_, PageNum() * Page::SIZE);
    return ret;
  } else {
    return free_list_buf_[--free_list_buf_used_];
  }
}
pgid_t PageManager::Allocate() {
  pgid_t ret = __Allocate();
  assert(ret <= is_free_.size());
  if (ret == is_free_.size()) {
    is_free_.push_back(false);
  } else {
    assert(is_free_[ret] == true);
    is_free_[ret] = false;
  }
  return ret;
}

void PageManager::Free(pgid_t pgid) {
  if (is_free_[pgid])
    DB_ERR("Internal error: Double free of page {}\n", pgid);
  is_free_[pgid] = true;
  eviction_policy_.Remove(pgid);
  auto it = buf_.find(pgid);
  if (it != buf_.end()) {
    assert(it->second.refcount == 0);
    buf_.erase(it);
  }
  if (free_list_buf_used_ == PGID_PER_PAGE) {
    if (free_list_buf_standby_full_) {
      FlushFreeListStandby(pgid);
      free_list_buf_standby_full_ = false;
      return;
    }
    std::swap(free_list_buf_, free_list_buf_standby_);
    free_list_buf_standby_full_ = true;
    free_list_buf_used_ = 0;
  }
  free_list_buf_[free_list_buf_used_] = pgid;
  free_list_buf_used_ += 1;
}

void PageManager::ShrinkToFit() {
  std::vector<pgid_t> free_pages;
  while (free_list_buf_used_) {
    free_list_buf_used_ -= 1;
    free_pages.push_back(free_list_buf_[free_list_buf_used_]);
  }
  if (free_list_buf_standby_full_) {
    for (size_t i = 0; i < PGID_PER_PAGE; ++i)
      free_pages.push_back(free_list_buf_standby_[i]);
  }
  pgid_t pgid = FreeListHead();
  while (pgid != 0) {
    free_pages.push_back(pgid);
    file_.seekg(pgid * Page::SIZE);
    file_.read(reinterpret_cast<char *>(free_list_buf_),
      PGID_PER_PAGE * sizeof(pgid_t));
    file_.read(reinterpret_cast<char *>(&pgid), sizeof(pgid));
    for (size_t i = 0; i < PGID_PER_PAGE; ++i)
      free_pages.push_back(free_list_buf_[i]);
  }
  std::sort(free_pages.begin(), free_pages.end());

  assert(PageNum() > 0);
  pgid_t last_page = PageNum() - 1;
  while (last_page) {
    if (free_pages.empty() || free_pages.back() != last_page)
      break;
    free_pages.pop_back();
    last_page -= 1;
  }
  PageNum() = last_page + 1;

  FreeListHead() = 0;
  size_t i = 0;
  while (free_pages.size() - i > PGID_PER_PAGE) {
    pgid = free_pages[i++];
    file_.seekp(pgid * Page::SIZE);
    file_.write(reinterpret_cast<const char *>(free_pages.data() + i),
      PGID_PER_PAGE * Page::SIZE);
    i += PGID_PER_PAGE;
    pgid_t head = FreeListHead();
    file_.write(reinterpret_cast<const char *>(&head), sizeof(head));
    FreeListHead() = pgid;
  }
  free_list_buf_used_ = free_pages.size() - i;
  memcpy(free_list_buf_, free_pages.data() + i,
    free_list_buf_used_ * sizeof(pgid_t));
  free_list_buf_standby_full_ = false;

  assert(is_free_.size() >= PageNum());
  is_free_.resize(PageNum());
}

void PageManager::AllocMeta() {
  auto buf = std::unique_ptr<char[]>(new char[Page::SIZE]);
  // Mark dirty to force the meta page to be flushed when closing,
  // so that we don't need to mark it dirty anymore when running.
  auto ret = buf_.emplace(0, PageBufInfo{std::move(buf), 1, true});
  (void)ret;
  assert(ret.second);
  assert(buf_.size() < max_buf_pages_);
}
void PageManager::Init() {
  AllocMeta();
  memset(buf_[0].addr_mut(), 0, Page::SIZE);
  FreeListHead() = 0;
  FreePagesInHead() = 0;
  PageNum() = 2;
  std::filesystem::resize_file(path_, Page::SIZE);
  is_free_.resize(PageNum(), false);
}

std::optional<io::Error> PageManager::Load() {
  AllocMeta();
  file_.read(buf_[0].addr_mut(), Page::SIZE);
  if (!file_.good())
    return io::Error::New(io::ErrorKind::Other,
      "Error occurred when reading file " + path_.string());
  is_free_.resize(PageNum(), false);
  pgid_t head = FreeListHead();
  if (head == 0)
    return std::nullopt;
  file_.seekg(head * Page::SIZE);
  free_list_buf_used_ = FreePagesInHead();
  file_.read(reinterpret_cast<char *>(free_list_buf_),
     free_list_buf_used_ * sizeof(pgid_t));
  pgid_t pgid;
  file_.seekg((head + 1) * Page::SIZE - sizeof(pgid_t));
  file_.read(reinterpret_cast<char *>(&pgid), sizeof(pgid));
  FreeListHead() = pgid;

  for (size_t i = 0; i < free_list_buf_used_; ++i)
    is_free_[free_list_buf_[i]] = true;
  while (pgid) {
    file_.seekg(pgid * Page::SIZE);
    assert(!free_list_buf_standby_full_);
    // Borrow free_list_buf_standby_ here
    file_.read(reinterpret_cast<char *>(free_list_buf_standby_),
      PGID_PER_PAGE * sizeof(pgid_t));
    for (size_t i = 0; i < PGID_PER_PAGE; ++i)
      is_free_[free_list_buf_standby_[i]] = true;
    file_.read(reinterpret_cast<char *>(&pgid), sizeof(pgid));
  }

  // Postpone the free here to make sure that free_list_buf_standby_ is empty.
  Free(head);

  if (!file_.good())
    return io::Error::New(io::ErrorKind::Other,
      "Error occurred when reading file " + path_.string());
  return std::nullopt;
}

Page PageManager::GetPage(pgid_t pgid) {
  if (pgid >= PageNum()) {
    DB_ERR("Internal Error: " + std::to_string(pgid) + " >= " +
        std::to_string(PageNum()));
  }
  if (is_free_[pgid])
    DB_ERR("Internal error: Accessing free page {}", pgid);
  char *addr;
  auto it = buf_.find(pgid);
  if (it != buf_.end()) {
    addr = it->second.addr_mut();
    if (it->second.refcount == 0)
      eviction_policy_.Pin(pgid);
    it->second.refcount += 1;
  } else {
    assert(buf_.size() <= max_buf_pages_);
    std::unique_ptr<char[]> buf;
    if (buf_.size() == max_buf_pages_) {
      pgid_t pgid_to_evict = eviction_policy_.Evict();
      auto it = buf_.find(pgid_to_evict);
      assert(it->second.refcount == 0);
      if (it->second.dirty) {
        file_.seekp(pgid_to_evict * Page::SIZE);
        file_.write(it->second.addr(), Page::SIZE);
      }
      buf = std::move(it->second.buf);
      buf_.erase(it);
    } else {
      buf = std::unique_ptr<char[]>(new char[Page::SIZE]);
    }
    PageBufInfo buf_info{std::move(buf), 1, false};
    addr = buf_info.addr_mut();
    file_.seekg(pgid * Page::SIZE);
    file_.read(addr, Page::SIZE);
    auto ret = buf_.emplace(pgid, std::move(buf_info));
    (void)ret;
    assert(ret.second);
  }
  return Page(pgid, addr, *this, false);
}
void PageManager::DropPage(pgid_t pgid, bool dirty) {
  assert(pgid != 0);
  auto it = buf_.find(pgid);
  assert(it != buf_.end());
  it->second.dirty |= dirty;
  assert(it->second.refcount > 0);
  it->second.refcount -= 1;
  if (it->second.refcount == 0)
    eviction_policy_.Unpin(pgid);
}
void PageManager::FlushFreeListStandby(pgid_t pgid) {
  file_.seekp(pgid * Page::SIZE);
  file_.write(reinterpret_cast<const char *>(free_list_buf_standby_),
    PGID_PER_PAGE * sizeof(pgid_t));
  pgid_t head = FreeListHead();
  file_.write(reinterpret_cast<const char *>(&head), sizeof(head));
  FreeListHead() = pgid;
  free_list_buf_standby_full_ = false;
}

}
