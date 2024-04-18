#include "storage/lsm/compaction_pick.hpp"

namespace wing {

namespace lsm {

std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

std::unique_ptr<Compaction> TieredCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

std::unique_ptr<Compaction> FluidCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

std::unique_ptr<Compaction> LazyLevelingCompactionPicker::Get(
    Version* version) {
  DB_ERR("Not implemented!");
}

}  // namespace lsm

}  // namespace wing
