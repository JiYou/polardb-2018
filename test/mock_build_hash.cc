#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <vector>

void BuildHashTable() {
  std::vector<int> disk_value {0, 1, 2, 3, 4, 5, 6};
  std::vector<int> buffers;

  // begin to create threads.
  std::atomic<bool> read_over{false};
  std::mutex lock;
  std::condition_variable cond;

  // read all the disk.
  auto disk_read = [&]() {
    int idx = 0;
    while (!read_over) {
      if (idx >= disk_value.size()) {
        read_over = true;
        break;
      }
      // read out the value from disk.
      auto v = disk_value[idx++];

      // try to put the disk into bufers;
      std::unique_lock<std::mutex> l(lock);
      buffers.push_back(v);
    }
  };

  auto insert_hash = [&]() {
    while (!read_over) {
      int x;
      lock.lock();
      // deal with all the buffer.
      // get the last buffer.
      if (buffers.size() > 0) {
        x = buffers.back();
        buffers.pop_back();
      }
      lock.unlock();

      // deal with the buffer.
      std::cout << x << std::endl;
    }

    // read over. deal all the left buffers.
    for (auto &x: buffers) {
      std::cout << x << std::endl;
    }
  };

  std::thread thd_disk_read(disk_read);
  std::thread thd_insert_hash(insert_hash);

  thd_disk_read.join();
  thd_insert_hash.join();
}

int main(void) {
  BuildHashTable();
  return 0;
}
