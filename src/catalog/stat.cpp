#include "catalog/stat.hpp"
#include "common/murmurhash.hpp"

namespace wing {

void CountMinSketch::AddCount(std::string_view key, double value) {
  DB_ERR("Not implemented!");
}

double CountMinSketch::GetFreqCount(std::string_view key) const {
  DB_ERR("Not implemented!");
}

void HyperLL::Add(std::string_view key) {
  DB_ERR("Not implemented!");
}

double HyperLL::GetDistinctCounts() const {
  DB_ERR("Not implemented!");
}

}  // namespace wing