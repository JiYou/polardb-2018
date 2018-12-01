#pragma once

#include <stdio.h>  // printf
#include <stdlib.h> // exit
#include <fcntl.h>
#include <unistd.h> // pipe
#include <string.h> // strlen
#include <pthread.h> // pthread_create

#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>

#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <stdlib.h>

#include <atomic>
#include <cassert>
#include <stdexcept>
#include <type_traits>

namespace polar_race {
class chan {
 public:
  void write() {
    if (-1 == fd[1]) {
      return;
    }
    if (::write(fd[1], &c, sizeof(c)) != sizeof(c)) {
      ; // add error msg ?
    }
  }
  void read() {
    if (-1 == fd[0]) {
      return;
    }
    if (::read(fd[0], &c, sizeof(c)) != sizeof(c)) {
      ; // add error msg?
    }
  }
  chan() { }
  void init() {
    if (pipe(fd) < 0) {
      std::cout << "create pipe meet error\n";
    }
  }
  ~chan() {
    if (fd[0] > 0) {
      close(fd[0]);
    }
    if (fd[1] > 0) {
      close(fd[1]);
    }
    fd[0] = fd[1] = -1;
  }
 private:
  int c = 'A';
  int fd[2] = {-1};
};

template<typename T>
class spsc_queue {
public:
  explicit spsc_queue(const size_t capacity=4)
      : capacity_(capacity),
        slots_(capacity_ < 2 ? nullptr
                             : static_cast<T *>(operator new[](
                                   sizeof(T) * (capacity_ + 2 * kPadding)))),
        head_(0), tail_(0) {
    if (capacity_ < 2) {
      throw std::invalid_argument("size < 2");
    }
    assert(alignof(spsc_queue<T>) >= kCacheLineSize);
    assert(reinterpret_cast<char *>(&tail_) -
               reinterpret_cast<char *>(&head_) >=
           static_cast<ssize_t>(kCacheLineSize));
  }

  ~spsc_queue() {
    while (front()) {
      pop();
    }
    operator delete[](slots_);
  }

  // non-copyable and non-movable
  spsc_queue(const spsc_queue &) = delete;
  spsc_queue &operator=(const spsc_queue &) = delete;

  template <typename... Args>
  void emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible<T, Args &&...>::value) {
    static_assert(std::is_constructible<T, Args &&...>::value,
                  "T must be constructible with Args&&...");
    auto const head = head_.load(std::memory_order_relaxed);
    auto nextHead = head + 1;
    if (nextHead == capacity_) {
      nextHead = 0;
    }
    while (nextHead == tail_.load(std::memory_order_acquire))
      ;
    new (&slots_[head + kPadding]) T(std::forward<Args>(args)...);
    head_.store(nextHead, std::memory_order_release);
  }

  template <typename... Args>
  bool try_emplace(Args &&... args) noexcept(
      std::is_nothrow_constructible<T, Args &&...>::value) {
    static_assert(std::is_constructible<T, Args &&...>::value,
                  "T must be constructible with Args&&...");
    auto const head = head_.load(std::memory_order_relaxed);
    auto nextHead = head + 1;
    if (nextHead == capacity_) {
      nextHead = 0;
    }
    if (nextHead == tail_.load(std::memory_order_acquire)) {
      return false;
    }
    new (&slots_[head + kPadding]) T(std::forward<Args>(args)...);
    head_.store(nextHead, std::memory_order_release);
    return true;
  }

  void push(const T &v) noexcept(std::is_nothrow_copy_constructible<T>::value) {
    static_assert(std::is_copy_constructible<T>::value,
                  "T must be copy constructible");
    emplace(v);
  }

  template <typename P, typename = typename std::enable_if<
                            std::is_constructible<T, P &&>::value>::type>
  void push(P &&v) noexcept(std::is_nothrow_constructible<T, P &&>::value) {
    emplace(std::forward<P>(v));
  }

  bool
  try_push(const T &v) noexcept(std::is_nothrow_copy_constructible<T>::value) {
    static_assert(std::is_copy_constructible<T>::value,
                  "T must be copy constructible");
    return try_emplace(v);
  }

  template <typename P, typename = typename std::enable_if<
                            std::is_constructible<T, P &&>::value>::type>
  bool try_push(P &&v) noexcept(std::is_nothrow_constructible<T, P &&>::value) {
    return try_emplace(std::forward<P>(v));
  }

  T *front() noexcept {
    auto const tail = tail_.load(std::memory_order_relaxed);
    if (head_.load(std::memory_order_acquire) == tail) {
      return nullptr;
    }
    return &slots_[tail + kPadding];
  }

  void pop() noexcept {
    static_assert(std::is_nothrow_destructible<T>::value,
                  "T must be nothrow destructible");
    auto const tail = tail_.load(std::memory_order_relaxed);
    assert(head_.load(std::memory_order_acquire) != tail);
    slots_[tail + kPadding].~T();
    auto nextTail = tail + 1;
    if (nextTail == capacity_) {
      nextTail = 0;
    }
    tail_.store(nextTail, std::memory_order_release);
  }

  size_t size() const noexcept {
    ssize_t diff = head_.load(std::memory_order_acquire) -
                   tail_.load(std::memory_order_acquire);
    if (diff < 0) {
      diff += capacity_;
    }
    return diff;
  }

  bool empty() const noexcept { return size() == 0; }

  size_t capacity() const noexcept { return capacity_; }

private:
  static constexpr size_t kCacheLineSize = 128;

  // Padding to avoid false sharing between slots_ and adjacent allocations
  static constexpr size_t kPadding = (kCacheLineSize - 1) / sizeof(T) + 1;

private:
  const size_t capacity_;
  T *const slots_;

  // Align to avoid false sharing between head_ and tail_
  alignas(kCacheLineSize) std::atomic<size_t> head_;
  alignas(kCacheLineSize) std::atomic<size_t> tail_;

  // Padding to avoid adjacent allocations to share cache line with tail_
  char padding_[kCacheLineSize - sizeof(tail_)];
};

} // namespace polar_race
