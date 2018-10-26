#include "util/etest.h"
#include <vector>
#include <iostream>

namespace polar_race {
// begin namespace polar_race
namespace test {

class Test_eTest {};

TEST(Test_eTest, Test_Case_1) {
    std::cout << "working" << std::endl;
    std::vector<int> a {-1, 3, 2, -5, 4};
}


} // end of test
} // end of namespace polar_race