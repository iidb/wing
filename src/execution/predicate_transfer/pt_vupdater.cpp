#include "execution/predicate_transfer/pt_vupdater.hpp"

#include "common/bloomfilter.hpp"

namespace wing {

void PtVecUpdater::Execute(
    const std::string& bloom_filter, BitVector& valid_bits) {
  input_->Init();
  size_t index = 0;
  while (true) {
    auto batch = input_->Next();
    if (batch.size() == 0) {
      break;
    }

    for (auto tuple : batch) {
      // Find the current valid bit.
      while (true) {
        if (valid_bits.size() <= index) {
          auto new_size = std::max<size_t>(valid_bits.size() * 2, 10);
          auto old_size = valid_bits.size();
          valid_bits.Resize(new_size);
          for (uint32_t i = old_size; i < new_size; i++) {
            valid_bits[i] = 1;
          }
        }
        if (!valid_bits[index]) {
          index += 1;
          continue;
        } else {
          break;
        }
      }
      // Get bloom hash
      size_t hash = 0;
      if (tuple.GetElemType(0) == LogicalType::STRING) {
        hash = utils::BloomFilter::BloomHash(tuple[0].ReadStringView());
      } else {
        size_t data = tuple[0].ReadInt();
        hash = utils::BloomFilter::BloomHash(std::string_view(
            reinterpret_cast<const char*>(&data), sizeof(size_t)));
      }
      // Set to invalid if it is not found
      if (!utils::BloomFilter::Find(hash, bloom_filter)) {
        valid_bits[index] = 0;
      }
      index += 1;
    }
  }
}

}  // namespace wing
