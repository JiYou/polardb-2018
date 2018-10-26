
#include "util/etest.h"
#include "engine_race/engine_hash.h"

class TestHashTree {};

TEST(TestHashTree, testGetInstance) {
    auto hash = polar_race::GetHashTree();
}