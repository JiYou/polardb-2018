#pragma once

#include <atomic>

namespace polar_race {
// begin of namespace polar_race

class spinlock;

inline void spin_lock(std::atomic_flag& lock);
inline void spin_unlock(std::atomic_flag& lock);
inline void spin_lock(polar_race::spinlock& lock);
inline void spin_unlock(polar_race::spinlock& lock);

/* A pre-packaged spinlock type modelling BasicLockable: */
class spinlock final {
  std::atomic_flag af = ATOMIC_FLAG_INIT;

  public:
  void lock() {
    polar_race::spin_lock(af);
  }

  void unlock() noexcept {
    polar_race::spin_unlock(af);
  }
};

// Free functions:
inline void spin_lock(std::atomic_flag& lock) {
 while(lock.test_and_set(std::memory_order_acquire))
  ;
}

inline void spin_unlock(std::atomic_flag& lock) {
 lock.clear(std::memory_order_release);
}

inline void spin_lock(std::atomic_flag *lock) {
 spin_lock(*lock);
}

inline void spin_unlock(std::atomic_flag *lock) {
 spin_unlock(*lock);
}

inline void spin_lock(polar_race::spinlock& lock) {
 lock.lock();
}

inline void spin_unlock(polar_race::spinlock& lock) {
 lock.unlock();
}

inline void spin_lock(polar_race::spinlock *lock) {
 spin_lock(*lock);
}

inline void spin_unlock(polar_race::spinlock *lock) {
 spin_unlock(*lock);
}

} // namespace polar_race
