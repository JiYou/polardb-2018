// Copyright [2018] Alibaba Cloud All rights reserved
#pragma once

#include <string.h>
#include <string>
#include <unordered_map>

namespace polar_race {

class PolarString {
 public:
  PolarString() : data_(""), size_(0) { }

  PolarString(const char* d, size_t n) : data_(d), size_(n) { }

  PolarString(const std::string& s) : data_(s.data()), size_(s.size()) { }

  PolarString(const char* s) : data_(s), size_(strlen(s)) { }

  ~PolarString() {
    clear();
  }

  const char* data() const { return data_; }

  size_t size() const { return size_; }

  bool empty() const { return size_ == 0; }

  char operator[](size_t n) const {
    return data_[n];
  }

  void clear() {
    data_ = "";
    size_ = 0;
    if (is_change_) {
      char **c_s_addr = (char**)(&str_);
      uint64_t *c_s_len = (uint64_t*)(&str_) + 1;
      *c_s_addr = old_str_addr_;
      *c_s_len = old_str_len_;
    }
  }


  const std::string &ToString() const;

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const PolarString& b) const;

  bool starts_with(const PolarString& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_, x.data_, x.size_) == 0));
  }

  bool ends_with(const PolarString& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0));
  }

  void set_change() {
    set_change_ = true;
  }

 private:
  const char* data_;
  size_t size_;
  mutable std::string str_;
  bool set_change_ = false;
  mutable bool is_change_ = false;
  mutable char *old_str_addr_ = nullptr;
  mutable uint64_t old_str_len_ = 0;
  mutable std::string result;
  // Intentionally copyable
};

inline bool operator==(const PolarString& x, const PolarString& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const PolarString& x, const PolarString& y) {
  return !(x == y);
}

inline const std::string &PolarString::ToString() const {
  if (!set_change_) {
    result.assign(data_, size_);
     return result;
  }
  // record old addr and size
  char **c_s_addr = (char**)(&str_);
  uint64_t *c_s_len = (uint64_t*)(&str_) + 1;
  old_str_len_ = *c_s_len;
  old_str_addr_ = *c_s_addr;
  //std::string result;
  //result.assign(data_, size_);
  //return result;
  is_change_ = true;
  *c_s_addr = const_cast<char*>(data_);
  *c_s_len = size_;
  return str_;
}

inline int PolarString::compare(const PolarString& b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}


}  // namespace polar_race

