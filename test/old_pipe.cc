
#include <stdio.h>  // printf
#include <stdlib.h> // exit
#include <unistd.h> // pipe
#include <string.h> // strlen
#include <pthread.h> // pthread_create
#include <iostream>
 
using namespace std;
 
void *func(void * fd)
{
        printf("write fd = %d\n", *(int*)fd);
        char str[] = "hello everyone!";
        write( *(int*)fd, str, strlen(str) );
}
 
int main()
{
        int fd[2];
        char readbuf[1024];
 
        if(pipe(fd) < 0)
        {
                printf("pipe error!\n");
        }

        std::cout << "fd[0] = " << fd[0] << std::endl;
        std::cout << "fd[1] = " << fd[1] << std::endl; 
        // create a new thread
        pthread_t tid = 0;
        pthread_create(&tid, NULL, func, &fd[1]);
        pthread_join(tid, NULL);
 
 
        // read buf from child thread
        read( fd[0], readbuf, sizeof(readbuf) );
        printf("read buf = %s\n", readbuf);
 
        return 0;
}
