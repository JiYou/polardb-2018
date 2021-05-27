#include<stdio.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<dirent.h>
 
void printdir(char *dir,int depth){///depth 控制缩进
    DIR *dp;
    struct dirent *entry;
    struct stat statbuf;///文件类型
    if((dp=opendir(dir))==NULL){
        printf("open %s error\n",dir);
        return ;
    }
    chdir(dir);///切换到当前目录
    while((entry=readdir(dp))!=NULL){
        lstat(entry->d_name,&statbuf);
        if(S_ISDIR(statbuf.st_mode)){///如果是目录，递归遍历输出
            printf("%*s name:%s/  type:%d  inode:%d\n",depth,"",entry->d_name,
                   statbuf.st_mode,statbuf.st_size,entry->d_ino);
            ///忽略.和..目录
            if(strcmp(".",entry->d_name)==0 || strcmp("..",entry->d_name)==0)
                continue;
            printdir(entry->d_name,depth+4);
        }
        else{///如果是文件，直接输出
            printf("%*s name:%s/  type:%d  inode:%d\n",depth,"",entry->d_name,
                   statbuf.st_mode,statbuf.st_size,entry->d_ino);
        }
    }
    chdir("..");///切换到上层目录
    closedir(dp);
 
}
 
int main(){
    char dir[30];
    printf("input dir\n");
    scanf("%s",dir);
 
    printdir(dir,0);
    return 0;
}
