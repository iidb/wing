#pragma once

#include "storage/lsm/format.hpp"

namespace wing {

namespace lsm {

class Iterator {
 public:
  virtual ~Iterator() = default;

  /* Return true if the iterator is positioned at a valid entry. */
  virtual bool Valid() = 0;

  /* Return the key for the current entry. */
  virtual Slice key() const = 0;

  /* Return the value for the current entry.  */
  virtual Slice value() const = 0;

  /* Move it to the next entry. It must be valid iterator. */
  virtual void Next() = 0;
};

}  // namespace lsm

}  // namespace wing
