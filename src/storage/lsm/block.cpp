#include "storage/lsm/block.hpp"

namespace wing {

namespace lsm {

bool BlockBuilder::Append(ParsedKey key, Slice value) {
  DB_ERR("Not implemented!");
}

void BlockBuilder::Finish() { DB_ERR("Not implemented!"); }

void BlockIterator::Seek(Slice user_key, seq_t seq) {
  DB_ERR("Not implemented!");
}

Slice BlockIterator::key() { DB_ERR("Not implemented!"); }

Slice BlockIterator::value() { DB_ERR("Not implemented!"); }

void BlockIterator::Next() { DB_ERR("Not implemented!"); }

bool BlockIterator::Valid() { DB_ERR("Not implemented!"); }

}  // namespace lsm

}  // namespace wing
