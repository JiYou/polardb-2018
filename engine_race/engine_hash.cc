#include "util/util.h"
#include "engine_race/engine_hash.h"

#include <mutex>

namespace polar_race {
// begin of namespace polar_race

// because the key is 8 bytes, so just taken it
// as uint64_t, then put it into hash map.
// For the value, just need to record the
// place info:
// HashTree<key, value_info>

// The last level of hash tree, would 
// hash tree implementation.
class HashTree : public Hash {
    WITH_NO_COPY_CLASS(HashTree);
  public:
    virtual RetCode Put(uint64_t key, const ValueInfo &v) override {
      return kSucc;
    }
    virtual RetCode Get(uint64_t key, ValueInfo *v) override {
      return kSucc;
    }

    static Hash *GetInstnace() {
        static Hash *_hashtree_;
        static std::once_flag initialized;
        std::call_once (initialized, [] { _hashtree_ = new HashTree();});
        return _hashtree_;
    }

  private:
    HashTree() {
        // alloc all the space for hash tree.
        // 64 threads and 100W Read/Write. <uint8_t value>
        // 64 x 1M keys.
        // so, the total memory is: 64M * 8 byte = 512MB
        // for a hash tree, the item would be.
        // if we started in large value.
        // For the prime numbers:
        // 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31,
        // 37, 41, 43, 47, 53, 59, 61, 67, 71, 73,
        // 79, 83, 89, 97, 101, 103,
        // from [2 ~ 23] would cost 512MB
        // from [89 ~ 103] cost 89M items.
        // for less memory, here chose
        // [19 ~ 37] ~ 120MB
        // so, the first node is 19.
        // just the last node would point to another structure.
        // the last level, is built on skiplist/immu
        // 
    }

    ~HashTree() {

    }
};

Hash *GetHashTree() {
  return HashTree::GetInstnace();
}

} // end of namespace polar_race