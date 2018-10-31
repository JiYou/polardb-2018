#include "engine_race/bitcask.h"

#include <unistd.h>

#include <assert.h>
#include <algorithm>

namespace polar_race {
namespace bitcask {

Bitcask::Bitcask()
  : db_lock_(nullptr),
    active_id_(0),
    hint_id_(0),
    env_(nullptr) { }

Bitcask::~Bitcask() {
  this->Close();
  delete env_;
}

Status Bitcask::Open(const Options& options, const std::string& dbname) {
  options_ = options;
  dbname_ = dbname;
  return this->Init();
}

Status Bitcask::Close() {
  for (auto &item: fd_cache_) {
    close(item.second.fd);
  }
  Status s;
  if (active_file_ > 0) {
    close(active_file_);
  }
  if (hint_file_ > 0) {
    close(hint_file_);
  }
  if (options_.read_write) {
    s = env_->UnlockFile(db_lock_);
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

Status Bitcask::Init() {
  Status s;
  if (!env_->FileExists(dbname_)) {
    env_->CreateDir(dbname_);
  }

  if (options_.read_write) {
    s = env_->LockFile(dbname_ + LOCK, &db_lock_);
    if (!s.ok()) {
      return s;
    }
  }

  std::vector<std::string> index_files;
  if (env_->FileExists(dbname_ + IndexDirectory)) {
    s = env_->GetChildren(dbname_ + IndexDirectory, index_files);
    if (!s.ok()) {
      return s;
    }
  } else {
    env_->CreateDir(dbname_ + IndexDirectory);
  }

  // Find the maximum id of hint file
  hint_id_ = env_->FindMaximumId(index_files);
  std::sort(index_files.begin(), index_files.end(),
    [](const std::string &a, const std::string &b) {
      const int va = atoi(a.c_str() + 4);
      const int vb = atoi(b.c_str() + 4);
      return va < vb;
    }
  );
  // If have index file, load it to memory
  for (auto file : index_files) {
    this->LoadIndex(file);
  }

  // Find the maximum id of active file
  if (env_->FileExists(dbname_ + DataDirectory)) {
    std::vector<std::string> db_files;
    s = env_->GetChildren(dbname_ + DataDirectory, db_files);
    if (!s.ok()) {
      return s;
    } else {
      active_id_ = env_->FindMaximumId(db_files);
    }
  } else {
    env_->CreateDir(dbname_ + DataDirectory);
  }

  s = this->NewFileWriter(active_id_, active_file_size_, DataDirectory, DataFileName, &active_file_);
  if (!s.ok()) {
    return s;
  }
  s = this->NewFileWriter(hint_id_, hint_file_size_, IndexDirectory, HintFileName, &hint_file_);
  if (!s.ok()) {
    return s;
  }
  return s;
}

Status Bitcask::NewFileWriter(const int32_t& id,
                              int64_t& file_size,
                              const std::string& directory,
                              const std::string& filename,
                              int *fd) {
  const std::string file_name = dbname_ + directory + "/" + filename + std::to_string(id);
  Status s;
  if (options_.read_write) { // Writable and readable
    *fd = open(file_name.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
    if ((*fd) < 0) {
      return s.IOError(file_name + " open error!");
    }
    file_size = lseek(*fd, 0, SEEK_CUR);
  } else {  // Only readable
    *fd = open(file_name.c_str(), O_RDONLY, 0644);
    if ((*fd) < 0) {
      return s.IOError(file_name + " open error!");
    }
  }
  return s;
}

Status Bitcask::Put(const std::string& key, const std::string& value) {
  Status s;
  BitcaskData data;
  memcpy(data.value, value.c_str(), value.size());

  BitcaskIndex index;
  index.value_len = static_cast<int32_t>(value.size());
  index.key_len = static_cast<int8_t>(key.size());
  for (int i = 0; i < std::min(kMaxKeyLength, static_cast<int>(key.size())); i++) {
    index.key[i] = key[i];
  }

  // write to file
  int64_t pos = this->SyncData(data);

  if (pos < 0) {
    return s.IOError("Write data failed.");
  }

  index.file_id = active_id_;
  index.data_pos = pos;
  index.vaild = true;

  // Update index

  s = this->SyncIndex(index);
  if (!s.ok()) {
    return s;
  }
  // if success. then update it into hash.
  index_[key] = index;
  return s;
}

int64_t Bitcask::SyncData(const BitcaskData& data) {
  int64_t pos = active_file_size_;
  if (active_file_size_ < static_cast<int64_t>(options_.max_file_size)) {
    if (write(active_file_, &data, sizeof(data)) != sizeof(data)) {
      return  -1;
    }
    active_file_size_ += sizeof(data);
    fsync(active_file_);
    return pos;
  } else {
    close(active_file_);

    // Open new data file
    active_id_++;
    Status s;
    s = this->NewFileWriter(active_id_, active_file_size_, DataDirectory, DataFileName, &active_file_);
    if (!s.ok()) {
      return -1;
    }
    return this->SyncData(data);
  }
}

Status Bitcask::SyncIndex(const BitcaskIndex& index) {
  Status st;
  if (hint_file_size_ < static_cast<int64_t>(options_.max_index_size)) {
    if (write(hint_file_, &index, sizeof(index)) != sizeof(index)) {
      return st.IOError("hint_file_ write index failed!\n");
    }
    hint_file_size_ += sizeof(index);
    fsync(hint_file_);
    return st;
  } else {
    close(hint_file_);

    // Open new index file
    hint_id_++;
    st = this->NewFileWriter(hint_id_, hint_file_size_, IndexDirectory, HintFileName, &hint_file_);
    if (!st.ok()) {
      return st;
    }
    return this->SyncIndex(index);
  }
}

Status Bitcask::Get(const std::string& key, std::string* value) {
  Status s;
  auto find = index_.find(key);
  if (find != index_.end() && find->second.vaild) {
    const auto& idx = find->second;
    s = this->Retrieve(idx, value);
    return s;
  } else {
    return s.NotFound("Not Found");
  }
}

Status Bitcask::Delete(const std::string& key) {
  Status s;
  auto find = index_.find(key);
  if (find != index_.end()) {
    find->second.vaild = false;
    index_[find->first] = find->second;
    s = this->SyncIndex(find->second);
    return s;
  }
  return s;
}

Status Bitcask::Retrieve(const BitcaskIndex& index, std::string* value) {
  BitcaskData data;
  Status s;

  static const std::string data_prefix = dbname_ + DataDirectory + "/" + DataFileName;
  /*old
  int fd = open((data_prefix + std::to_string(index.file_id)).c_str(),
                O_RDONLY, 0644);
  if (fd < 0) {
    return s.IOError(data_prefix + std::to_string(index.file_id));
  }

  lseek(fd, index.data_pos, SEEK_SET);
  auto ret = read(fd, &data, sizeof(data));
  if (ret < 0) {
    return s.IOError(data_prefix + std::to_string(index.file_id) + "pos:" + std::to_string(index.data_pos));
  }
  close(fd);
  */
  auto iter = fd_cache_.find(index.file_id);

  // if not found, try to add it to cache.
  if (iter == fd_cache_.end()) {
    std::string file_name = data_prefix + std::to_string(index.file_id);
    int fd = open(file_name.c_str(), O_RDONLY, 0644);
    if (fd < 0) {
      std::cout << "fail open file = " << file_name << std::endl;
      return s.IOError(data_prefix + std::to_string(index.file_id));
    }

    lseek(fd, index.data_pos, SEEK_SET);
    auto ret = read(fd, &data, sizeof(data));
    if (ret < 0) {
      std::cout << "read data fail pos = " << index.data_pos << std::endl;
      return s.IOError(file_name + "pos:" + std::to_string(index.data_pos));
    }
    fd_cache_.emplace(std::piecewise_construct,
                      std::forward_as_tuple(index.file_id),
                      std::forward_as_tuple(fd, index.data_pos + sizeof(data)));
  } else {
    // if find the file in cache.
    // compare the pos.
    auto old_pos = iter->second.pos;
    auto fd = iter->second.fd;
    // if not equal, then need to seek.
    if (index.data_pos != old_pos) {
      lseek(fd, index.data_pos - old_pos, SEEK_CUR);
    }
    auto ret = read(fd, &data, sizeof(data));
    if (ret < 0) {
      std::string file_name = data_prefix + std::to_string(index.file_id);
      std::cout << "read data fail pos = " << index.data_pos << std::endl;
      return s.IOError(file_name + "pos:" + std::to_string(index.data_pos));
    }

    // update the position in cache.
    auto cur_pos = index.data_pos + sizeof(data);
    iter->second.pos = cur_pos;
  }
  value->clear();
  value->append(data.value, static_cast<unsigned long>(index.value_len));

  return s;
}

Status Bitcask::LoadIndex(const std::string &file) {
  Status s;
  static const std::string index_prefix = dbname_ + IndexDirectory + "/";
  int fd = open((index_prefix + file).c_str(), O_RDONLY, 0644);
  if (fd < 0) {
    return s.IOError((index_prefix + file).c_str());
  }

  BitcaskIndex index;
  while (read(fd, &index, sizeof(index)) == sizeof(index)) {
    index_[index.key] = index;
  }
  close(fd);
  return s;
}

Status Env::CreateDir(const std::string& name) {
  Status s;
  if (mkdir(name.c_str(), 0755) != 0) {
    return s.IOError("Create directory failed.");
  }
  return s;
}

Status Env::LockFile(const std::string& name, FileLock** lock) {
  *lock = nullptr;
  Status s;
  int fd = ::open(name.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return s.IOError("Lock file failed." + std::to_string(errno));
  } else if (LockOrUnlock(fd, true) == -1) {
    ::close(fd);
    return s.IOError("Lock file failed." + std::to_string(errno));
  } else {
    FileLock* my_lock = new FileLock;
    my_lock->fd_ = fd;
    my_lock->name_ = name;
    *lock = my_lock;
  }
  return s;
}

Status Env::UnlockFile(FileLock* lock) {
  Status s;
  if (LockOrUnlock(lock->fd_, false) == -1) {
    return s.IOError("Unlock file failed.");
  }
  ::close(lock->fd_);
  delete lock;
  return s;
}

int Env::LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;
  return fcntl(fd, F_SETLK, &f);
}

Status Env::GetChildren(const std::string& name, std::vector<std::string>& files) {
  Status s;
  files.clear();
  DIR* d = opendir(name.c_str());
  if (d == nullptr) {
    return s.IOError("Get directory children failed.");
  }
  struct dirent* entry;
  while ((entry = readdir(d)) != nullptr) {
    if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
      continue;
    }
    files.push_back(entry->d_name);
  }
  closedir(d);
  return s;
}

int32_t Env::FindMaximumId(const std::vector<std::string> &files) {
  int32_t max_no = 0;
  int32_t file_no;
  for (auto file : files) {
    file_no = atoi(file.c_str()+4);
    max_no = std::max(max_no, file_no);
  }
  return max_no;
}

bool Env::FileExists(const std::string& name) {
  return access(name.c_str(), F_OK) == 0;
}

} // namespace bitcask
} // namespace polar_race

