#include <iostream>
#include <vector>
#include <numeric>
#include <chrono>

volatile int sink;
int main()
{
    for (auto size = 1ull; size < 1000000000ull; size *= 100) {
        // record start time
        auto start = std::chrono::system_clock::now();
        // do some work
        std::vector<int> v(size, 42);
        sink = std::accumulate(v.begin(), v.end(), 0u); // make sure it's a side effect
        // record end time
        auto end = std::chrono::system_clock::now();
        //std::chrono::microseconds diff = end-start;
        auto mis = std::chrono::duration_cast<std::chrono::microseconds>(end-start);
        std::cout << "Time to fill and iterate a vector of "
                  << size << " ints : " << (mis).count() << " microseconds\n";
    }
}
