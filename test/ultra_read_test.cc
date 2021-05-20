
#include "engine_race/engine_race.h"

#include <bits/stdc++.h>

using namespace std;
using namespace polar_race;

enum {
  kReadOp = 1,
  kWriteOp = 2,
};

static void exit_with_help() {
    printf("Usage: ultra_disk_test [option] disk_full_path\n"
           "Options:\n"
           "-r: read test\n"
           "-w: write test\n"
           "-l size: write size(bytes) in every write\n"
           "         the size must be 512 aligned\n"
           "-v size: overlapped size(bytes) with previous write/read\n"
           "         the size must be 512 bytes aligned\n"
           "example:\n"
           "    ./ultra_disk_test -r -l 4096 -v 512 /dev/sdd\n"
           "    ./ultra_disk_test -w -l 4096 -v 4096 /dev/sdd\n"
    );
    exit(-1);
}

int main(int argc, char **argv) {

    const char *disk_path = nullptr;
    int op_type = -1;
    int op_size = -1;
    const int min_block_size = 512;
    int overlap_size = 0;

    // parse options
    for(int i = 1; i < argc; i++) {

        // this is disk path
        if(argv[i][0] != '-') {
            disk_path = argv[i];
            continue;
        }

        switch(argv[i][1]) {
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
                if (op_size & (min_block_size-1) || op_size == 0) {
                    fprintf(stderr, "write size must aligned with 512\n");
                    exit_with_help();
                }
                break;
            
            // overlap size
            case 'v':
                i++;
                overlap_size = stoi(argv[i]);
                if (overlap_size & (min_block_size - 1)) {
                  fprintf(stderr, "overlap_size must aligned with 512\n");
                  exit_with_help();
                }
                break;
            default:
                exit_with_help();
                break;
        }
    }
    

    if (-1 == op_size) {
        fprintf(stderr, "-l op_size is mandatory\n");
        exit_with_help();
    }

    if (-1 == op_type) {
        fprintf(stderr, "-r/-w is mandatory\n");
        exit_with_help();
    }

    if (nullptr == disk_path) {
        fprintf(stderr, "disk_path is mandatory\n");
        exit_with_help();
    }

    int fd = open(disk_path, O_RDWR | O_NOATIME | O_DIRECT | O_SYNC, 0644);
    if (fd == -1) {
      fprintf(stderr, "open file %s failed\n", disk_path);
      return -1;
    }

    bool is_read = op_type == kReadOp;
    struct aio_env_single aio(fd, is_read, false/*no_alloc*/);

    // setup buffer
    char *buf = polar_race::GetAlignedBuffer(op_size);
    memset(buf, 0, op_size);

    // read once
    aio.Prepare(0, buf, op_size);
    aio.Submit();

    auto ret = aio.WaitOver();
    printf("read size = %d\n", ret);

    for (int i = 0; i < op_size; i++) {
      printf("%c", buf[i]);
    }
    printf("\n");

    close(fd);

    return 0;
}