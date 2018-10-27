#pragma once

#include "engine_race/util.h"
#include "include/engine.h"
#include "include/polar_string.h"
#include <mutex>
#include <list>
#include <unordered_map>
#include <vector>

//inline bool operator==(const Item &l, const Item& r) {
//  return l.name == r.name;
//}

namespace std {
  template<>
  struct hash<polar_race::PolarString> {
    uint32_t operator()(const polar_race::PolarString &r) const {
      return polar_race::StrHash(r.data(), r.size());
    }
  };
} // namespace std

namespace polar_race {
// begin of polar_race

// item in the cache.
template<typename Value>
struct CacheNode {
    typedef typename std::list<CacheNode<Value>*>::iterator list_iter_;
    typedef std::pair<list_iter_, bool/*second_list?*/> value_type_;
    typename std::unordered_map<PolarString, value_type_>::iterator pos_;
    Value val_;
};


// 2 Queue LRU
template<typename Value>
class LRUCache {
  public:
    LRUCache(int cap) {
        cap_ = cap;
    }
    ~LRUCache() {
        DEBUG << "begin to free space list_size: " << first_list_.size() << ", " << second_list_.size() << std::endl;
        for (auto iter = first_list_.begin(); iter != first_list_.end(); iter++) {
            delete *iter;
        }
        for (auto iter = second_list_.begin(); iter != second_list_.end(); iter++) {
            delete *iter;
        }
        DEBUG << "Free space over" << std::endl;
    }

    RetCode FindThenUpdate(const PolarString &key, const Value &val) {
      // if not foud, skip
      std::unique_lock<std::mutex> l(lock_);
      auto pos = hash_.find(key);
      if (pos == hash_.end()) {
        return kNotFound;
      }

      auto iter = pos->second.first;
      // get the right ptr.
      CacheNode<Value> *ptr = *iter;
      ptr->val_ = val;
      return kSucc;
    }

    RetCode Get(const PolarString &key, Value *val) {
        std::unique_lock<std::mutex> l(lock_);
        // try to find it in the hash.
        auto pos = hash_.find(key);
        if (pos == hash_.end()) {
            return kNotFound;
        }
        // if found. remove from current list.
        // append to the tail of second_list_ tail.
        // NO need to which list it locate, just erase
        // it from current list.
        auto iter = pos->second.first;
        bool is_second_list = pos->second.second;
        // get the right ptr.
        CacheNode<Value> *ptr = *iter;

        if (is_second_list) {
            second_list_.erase(iter);
        } else {
            first_list_.erase(iter);
        }

        // put the ptr into the second list.
        second_list_.emplace_back(ptr);
        // update the hash_map.
        // first emplace.
        //auto ret = hash_.emplace(std::piecewise_construct,
        //                         std::forward_as_tuple(key),
        //                         std::forward_as_tuple(second_list_.back(), true));
        value_type_ v(std::prev(second_list_.end()), true);
        auto ret = hash_.emplace(key, v);
        ptr->pos_ = ret.first;

        // the assign the value.
        *val = ptr->val_;

        return kSucc;
    }

    RetCode Put(const PolarString &key, const Value &val) {
        std::unique_lock<std::mutex> l(lock_);

        auto pos = hash_.find(key);
        if (pos != hash_.end()) {
            // find it, check the value.
            auto iter = pos->second.first;
            CacheNode<Value> *ptr = *iter;
            // if find it, put to second list.
            ptr->val_ = val;
        } else {
            CacheNode<Value> *ptr = new CacheNode<Value>;
            ptr->val_ = val;
            first_list_.emplace_back(ptr);
            // update the hash_map;
            //auto ret = hash_.emplace(std::piecewise_construct,
            //                     std::forward_as_tuple(key),
            //                     std::forward_as_tuple(first_list_.back(), true));
            value_type_ v(std::prev(first_list_.end()), false);
            auto ret = hash_.emplace(key, v);
            ptr->pos_ = ret.first;
        }

        auto resize_list = [&](std::list<CacheNode<Value>*> &l) {
            while (l.size() > static_cast<size_t>(cap_)) {
                polar_race::CacheNode<Value> *ptr = l.front();
                l.erase(l.begin());
                hash_.erase(ptr->pos_);
                delete ptr;
            }
        };

        resize_list(first_list_);
        resize_list(second_list_);

        return kSucc;
    }
  private:
    // item maxium number.
    int cap_;

    std::mutex lock_;
    // just record the pointers.
    std::list<CacheNode<Value>*> first_list_;
    std::list<CacheNode<Value>*> second_list_;

    typedef typename std::list<CacheNode<Value>*>::iterator list_iter_;
    typedef std::pair<list_iter_ /*list_position*/, bool/*second_list?*/> value_type_;
    std::unordered_map<PolarString, value_type_> hash_;
};


} // end of namespace polar_string;
