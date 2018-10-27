#pragma once

#include "include/engine.h"
#include "include/polar_string.h"
#include <mutex>
#include <list>
#include <unordered_map>
#include <vector>

namespace polar_race {
// begin of polar_race

// item in the cache.
template<typename T>
struct CacheNode {
    std::unordered_map<PolarString,
        std::pair<
            std::list<CacheNode<T>*>::iterator, // position in the list.
            bool>  // belong to the second list?
    >::iterator pos_; // record the position in hash.
    T val_;
};


// 2 Queue LRU
template<typename T>
class LRUCache {
  public:
    LRUCache(int cap) {
        cap_ = cap;
    }
    ~LRUCache() {
        std::unique_lock<std::mutex> l(lock_);
        for (auto iter = first_list_.begin(); iter != first_list_.end(); iter++) {
            delete *iter;
        }
        for (auto iter = second_list_.begin(); iter != first_list_.end(); iter++) {
            delete *iter;
        }
    }
    RetCode Get(const PolarString &key, T *val) {
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
        CacheNode<T> *ptr = *iter;

        if (is_second_list) {
            second_list_.erase(iter);
        } else {
            first_list_.erase(iter);
        }

        // put the ptr into the second list.
        second_list_.emplace_back(ptr);
        // update the hash_map.
        // first emplace.
        auto ret = hash_.emplace({key, ptr});
        ptr->pos_ = ret.second;

        // the assign the value.
        *val = ptr->val_;
    }

    RetCode Put(const PolarString &key, const T &val) {
        std::unique_lock<std::mutex> l(lock_);

        auto pos = hash_.find(key);
        // if find it, then update the value, and put it into the tail of second list.
        if (pos != hash_.end()) {
            // find it, check the value.
            auto iter = pos->second.first;
            CacheNode<T> *ptr = *iter;
            // if find it, put to second list.
            ptr->val_ = val;

            bool is_selcond_list = pos->second.second;
            if (is_selcond_list) {
                second_list_.erase(iter);
            } else {
                first_list_.erase(iter);
            }
            second_list_.emplace_back(ptr);

            // update the hash_map;
            auto ret = hash_.emplace({key, ptr});
            ptr->pos_ = ret.second;
        }
        // if not found. then need to put the item into first list.
        else {
            CacheNode<T> *ptr = new CacheNode<T>;
            ptr->val_ = val;
            first_list_.emplace_back(ptr);
            // update the hash_map;
            auto ret = hash_.emplace({key, ptr});
            ptr->pos_ = ret.second;
        }

        auto resize_list = [&](std::list<CacheNode<T>*> &l) {
            while (l.size() > cap_) {
                CacheNode<T> *ptr = **(l.front());
                l.erase(l.front());
                hash_.erase(ptr->pos_);
                delete ptr;
            }
        };

        resize_list(first_list_);
        resize_list(second_list_);
    }
  private:
    // item maxium number.
    int cap_;

    std::mutex lock_;
    // just record the pointers.
    std::list<CacheNode<T>*> first_list_;
    std::list<CacheNode<T>*> second_list_;
    std::unordered_map<PolarString,
        std::pair<
            std::list<CacheNode<T>*>::iterator, // position in the list.
            bool>  // belong to the second list?
    > hash_;
};

} // end of namespace polar_string;