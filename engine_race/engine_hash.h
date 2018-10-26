/**
 * Note: The returned array must be malloced, assume caller calls free().
 */
// ERROR: not call initNextPrime
// ERROR: assert(node->son[i]->modValue, NextPrime[n]) must put in for loop.
// ERROR: MaxPrimeNumber = 31, is too big.
// ERROR: memory alignment: fix negtive number mod should use absValue.
// 1. change list_node_size -> sizeof(struct _ListNode)
#pragma once

#include "include/engine.h"
#include "util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <stddef.h>

namespace polar_race {
// begin namespace polar_race

class ValueInfo {
};

// abstraction of hash class.
class Hash {
  public:
    Hash() { }
    virtual ~Hash() {}
    virtual RetCode Put(uint64_t key, const ValueInfo &val) = 0;
    virtual RetCode Get(uint64_t key, ValueInfo *val) = 0;
};


// just one HashTree for a process.
Hash *GetHashTree();


} // end namespace polar_race
