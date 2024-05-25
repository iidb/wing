#include "execution/predicate_transfer/pt_vcreator.hpp"

#include "common/bloomfilter.hpp"

namespace wing {

void PtVecCreator::Execute() {
  std::vector<uint64_t> key_hash;
  while (true) {
    auto batch = input_->Next();
    if (batch.size() == 0) {
      break;
    }
    for (auto tuple : batch) {
      if (tuple.GetElemType(0) == LogicalType::STRING) {
        key_hash.push_back(
            utils::BloomFilter::BloomHash(tuple[0].ReadStringView()));
      } else {
        size_t data = tuple[0].ReadInt();
        key_hash.push_back(utils::BloomFilter::BloomHash(std::string_view(
            reinterpret_cast<const char*>(&data), sizeof(size_t))));
      }
    }
  }
  utils::BloomFilter::Create(key_hash.size(), bloom_bit_per_key_n_, result_);
  for (auto hash : key_hash) {
    utils::BloomFilter::Add(hash, result_);
  }
}

}  // namespace wing
