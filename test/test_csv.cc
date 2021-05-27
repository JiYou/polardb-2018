#include <dirent.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <bits/stdc++.h>

using namespace std;

enum
{
  kReadOp = 1,
  kWriteOp = 2,
  kBlockSize = 512,
};

static void
exit_with_help()
{
  printf("Usage: ultra_disk_test [option] disk_full_path\n"
         "Options:\n"
         "-r: read test\n"
         "-w: write test\n"
         "-l size: write size(bytes) in every write\n"
         "         the size must be %d aligned\n"
         "-v size: overlapped size(bytes) with previous write/read\n"
         "         the size must be %d bytes aligned\n"
         "-t time: run how many seconds\n"
         "example:\n"
         "    ./ultra_disk_test -r -l 4096 -v %d /dev/sdd\n"
         "    ./ultra_disk_test -w -l 4096 -v 4096 /dev/sdd\n",
         kBlockSize,
         kBlockSize,
         kBlockSize);
  exit(-1);
}

string dir_path;
int op_type = -1;
int op_size = -1;
int overlap_size = 0;

void
printdir(const char* dir)
{

  DIR* dp;
  struct dirent* entry;

  if ((dp = opendir(dir)) == NULL) {
    printf("open %s error\n", dir);
    return;
  }

  while ((entry = readdir(dp)) != NULL) {

    struct stat statbuf;
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

int
main(int argc, char** argv)
{

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

      default:
        exit_with_help();
        break;
    }
  }

  printdir(dir_path.c_str());

  return 0;
}
