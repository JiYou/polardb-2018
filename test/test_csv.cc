#include <dirent.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <bits/stdc++.h>

using namespace std;

enum {
  kReadOp = 1,
  kWriteOp = 2,
  kBlockSize = 512,
};

static void exit_with_help() {
  printf("Usage: test_csv [option] dir_path\n"
         "Options:\n"
         "-r: read test\n"
         "-w: write test\n"
         "-l size: write size(bytes) in every write\n"
         "         the size must be %d aligned\n"
         "-v size: overlapped size(bytes) with previous write/read\n"
         "         the size must be %d bytes aligned\n"
         "-d disk_Size: filter the tested disk size (GByte)\n"
         "example:\n"
         "    ./test_csv -r -l 4096 -v %d ./test-512G\n"
         "    ./test_csv -w -l 4096 -v 4096 ./test-102G\n",
         kBlockSize, kBlockSize, kBlockSize);

  exit(-1);
}

// filter arguments.
// -1 stands for any value.
string dir_path;
int op_type = -1;
int op_size = -1;
int overlap_size = -1;
int disk_size = -1;
double percentile = -1;

struct file_info {
  int op_type;
  int op_size;
  int overlap_size;
  int disk_size;
  double percentile;
};

static vector<string> to_words(string &s) {
  const int N = s.length();
  vector<string> ans;

  int front = -1;
  for (int i = 0; i <= N; i++) {
    if (i == N || s[i] == '-') {
      if (front + 1 < i && s[front + 1] != '-') {
        // [front+1, ..., i) is a word
        string word = s.substr(front + 1, i - front - 1);
        ans.push_back(word);
      }
    }
  }

  return ans;
}

static struct file_info filter_file(string file_name) {
  auto words = to_words(s);

  struct file_info ans;

  auto disk_size = words[0];
  disk_size.pop_back();
  ans.disk_size = stoi(disk_size.c_str());
}

static void printdir(const char *dir) {

  DIR *dp;
  struct dirent *entry;

  if ((dp = opendir(dir)) == NULL) {
    printf("open %s error\n", dir);
    return;
  }

  while ((entry = readdir(dp)) != NULL) {
    struct stat statbuf;
    memset(&statbuf, 0, sizeof(statbuf));

    lstat(entry->d_name, &statbuf);

    if (S_ISDIR(statbuf.st_mode)) {
      // not deal with dir here.
      continue;
    } else {
      printf("%s\n", entry->d_name);
    }
  }

  closedir(dp);
}

int main(int argc, char **argv) {

  // {512, ....,, 16384};
  unordered_set<int> disk_size_set;
  for (int i = 9; i <= 14; i++) {
    disk_size_set.insert(1 << i);
  }

  // parse options
  for (int i = 1; i < argc; i++) {

    // this is dir to scan
    if (argv[i][0] != '-') {
      dir_path = argv[i];
      continue;
    }

    switch (argv[i][1]) {
    // is read op!
    case 'r':
      op_type = kReadOp;
      break;

    // is write op!
    case 'w':
      op_type = kWriteOp;
      break;

    // write len
    case 'l':
      i++;
      op_size = stoi(argv[i]);
      if (op_size & (kBlockSize - 1) || op_size == 0) {
        fprintf(stderr, "write size must aligned with %d\n", kBlockSize);
        exit_with_help();
      }
      break;

    // overlap size
    case 'v':
      i++;
      overlap_size = stoi(argv[i]);
      if (overlap_size & (kBlockSize - 1)) {
        fprintf(stderr, "overlap_size must aligned with %d\n", kBlockSize);
        exit_with_help();
      }
      break;

    // disk size in GB
    case 'd':
      i++;
      disk_size = stoi(argv[i]);
      if (!disk_size_set.count(disk_size)) {
        fprintf(stderr, "overlap_size must aligned with %d\n", kBlockSize);
        exit_with_help();
      }
      break;

    default:
      exit_with_help();
      break;
    }
  }

  if (dir_path.length() == 0) {
    exit_with_help();
  }

  printdir(dir_path.c_str());

  return 0;
}
