#include "storage/lsm/sst.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

namespace wing {

namespace lsm {

SSTable::SSTable(SSTInfo sst_info, size_t block_size, bool use_direct_io)
  : sst_info_(std::move(sst_info)), block_size_(block_size) {
  DB_ERR("Not implemented!");
}

SSTable::~SSTable() {
  if (remove_tag_) {
    file_.reset();
    std::filesystem::remove(sst_info_.filename_);
  }
}

GetResult SSTable::Get(Slice key, uint64_t seq, std::string* value) {
  DB_ERR("Not implemented!");
}

SSTableIterator SSTable::Seek(Slice key, uint64_t seq) {
  DB_ERR("Not implemented!");
}

SSTableIterator SSTable::Begin() { DB_ERR("Not implemented!"); }

void SSTableIterator::Seek(Slice key, uint64_t seq) {
  DB_ERR("Not implemented!");
}

void SSTableIterator::SeekToFirst() { DB_ERR("Not implemented!"); }

bool SSTableIterator::Valid() { DB_ERR("Not implemented!"); }

Slice SSTableIterator::key() { DB_ERR("Not implemented!"); }

Slice SSTableIterator::value() { DB_ERR("Not implemented!"); }

void SSTableIterator::Next() { DB_ERR("Not implemented!"); }

void SSTableBuilder::Append(ParsedKey key, Slice value) {
  DB_ERR("Not implemented!");
}

void SSTableBuilder::Finish() { DB_ERR("Not implemented!"); }

}  // namespace lsm

}  // namespace wing
