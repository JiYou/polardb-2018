#include "engine_race/libaio.h"
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>



int main(void) {
    int input_fd;
    const char *file_name = "DATA_0";
    io_context_t ctx;
    struct iocb io,*p=&io;
    struct io_event e;
    struct timespec timeout;
    memset(&ctx,0,sizeof(ctx));

    if(io_setup(10, &ctx)!=0) {
        printf("io_setup error\n");
        return -1;
    }
    if((input_fd=open(file_name,O_RDONLY|O_DIRECT,0644))<0) {
        perror("open error");
        io_destroy(ctx);
        return -1;
    }

    char *buf = nullptr;
    constexpr size_t kPageSize = 4096;
    if (posix_memalign(reinterpret_cast<void**>(&buf), kPageSize, kPageSize)) {
      perror("posix_memalign failed!\n");
      io_destroy(ctx);
      return -1;
    }

    io_prep_pread(&io, input_fd, buf, kPageSize , 0);
    io.data = buf;
    if(io_submit(ctx,1,&p)!=1) {
        io_destroy(ctx);
        printf("io_submit error\n");
        return -1;
    }
    while(1) {
        timeout.tv_sec=0;
        timeout.tv_nsec=500000000;//0.5s
        if(io_getevents(ctx,0,1,&e,&timeout)==1) {
            close(input_fd);
            break;
        }
        printf("haven't done\n");
        sleep(1);
    }
    printf(" is done!\n");
    io_destroy(ctx);
    return 0;
}
