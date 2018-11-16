

phxpaxosseastar12@

![image-20181025195932927](/Users/youji/Library/Application Support/typora-user-images/image-20181025195932927.png)



CPU：Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz （64核）

磁盘：Intel Corporation Device 2701（345G、裸盘IOPS 580K、EXT4 IOPS 400K）

OS：Linux version 3.10.0-327.ali2010.alios7.x86_64

文件系统：EXT4



**预热赛（10月25日- 11月5日） 正式赛（11月6日-11月19日）** 
1、第一赛季选手在本地进行实现并编译调试；
2、预热赛：10月24日12:00后开放作品提交入口，每个自然日提供5次代码运行机会，并提供运行日志，不提供排名，11月5日12:00关闭提交入口；
3、正式赛：11月5日12:00开放作品提交入口， 每个自然日提供5次代码运行机会，并提供运行日志。11月6日10点起分别在10:00、14:00、20:00、22:00、24:00 更新一次排行榜，按照得分（截止当天的历史最优成绩）从高到低排序；
4、第一赛季截止时（11月19日8:00截止作品提交，11月19日10:00产生最后排行），最好成绩排名前200名的队伍进入第二赛季。



TEAM ID: 素雪覆千里



- 只是一个hash kv
- 利用hash树实现一个多线程并发的hash，只会在最底层加上一个锁
- Wal log采用类似腾讯的作法。大概4MB ~ 8MB一个bin log文件
- Hash tree的最底层利用leveldb的做法，skillet/immu
  - Imm 刷写到磁盘上的时候，

# Enhance



- 自己实现hash. [DONE]。基于hash tree + hash array
- 加队列 [DONE] 写队列
- hash shard [DONE]
- Read file_no->fd hash_map. do not open for read every time. [DONE]

# 性能的估算

1秒 = 1000毫秒 = 1000000 微秒 = 1000000000纳秒

磁盘的IOPS 580K/s. -> 1000 000 000 〜= 推算出: 一个IO需要1724纳秒。假设读写都一样。

CPU references: https://www.7-cpu.com/cpu/Broadwell.html

假设MIPS是4K,即 4000,00,0000 ~ 4G/s 的指令,那么每秒可执行的指令是： 一条有效指令大概是0.25纳秒。

那么也就是说，一个IO进行的时候，可以处理6896条指令。那么可以进行的排序是多少？假设是:

​  100个元素，如果使用插入排序，也就只需要处理100下。时间应该是足够的。

```Cpp
L1 Data cache = 32 KB, 64 B/line, 8-WAY.
L1 Instruction cache = 32 KB, 64 B/line, 8-WAY.
L2 cache = 256 KB, 64 B/line, 8-WAY
L3 cache = 20 MB, 64 B/line, 20-WAY (i7-6900K)
L3 cache = 55 MB, 64 B/line, 20-WAY (E5-2699 v4)
L1 Data Cache Latency = 4 cycles for simple access via pointer
L1 Data Cache Latency = 5 cycles for access with complex address calculation (size_t n, *p; n = p[n]).
L2 Cache Latency = 12 cycles
L3 Cache Latency = 59 cycles (i7-6900K, 4.0 GHz)
L3 Cache Latency = 38 cycles (E5-2630 v4, 2.2 GHz) (core 0)
L3 Cache Latency = 46 cycles (E5-2630 v4, 3.1 GHz) (core 0)
L3 Cache Latency = 65 cycles (E5-2699 v4, 3.6 GHz) (60 - 69 cycles on different cores)
RAM Latency = 59 cycles + 46 ns (i7-6900K, 4.0 GHz)
RAM Latency = 38 cycles + 58 ns (E5-2630 v4, 1S, 2.2 GHz, DDR4 2166, CL15)
RAM Latency = 46 cycles + 58 ns (E5-2630 v4, 1S, 3.1 GHz, DDR4 2166, CL15)
RAM Latency = 65 cycles + 75 ns (E5-2699 v4, 3.6 GHz)
```

Intel E5-2699 v4 (Bradwell), 3.6 GHz (Turbo Boost), 14 nm. RAM: 256 GB, PC4-2133.

# TODO

- **Hash Shard锁**
  - 每个Hash shard分别加锁 spin_lock
  - 写的队列可以考虑去重和合并。

- **LibAIO**
  - aio_read/aio_write example **DONE**
    - 并发读同一个文件，看看是否可以较好地工作 **DONE**
  - read
    - DB锁改成spin_lock **DONE**
      - 每个shard会有一把锁， 注意内存大小 + 6M  **DONE**
      - 在读的时候，是否是每个文件都需要加一把锁?否则aio会不会读出错。！应该是的。**DONE**
    - 先改成不要有队列的 ~ libaio + 异步调用 **DONE**
      - Perfmance: 17m18.232s : 2K IOPS 8.36MB/s
      - 一开始的时候把文件扫描出来。然后利用一个数组建成Reader: **file_no -> fd hash**表 **DONE**
        - 如果不在这个hash表中，那么就利用临时变量fd来打开，用完之后关闭。 **DONE**
        - 直接利用固定数组实现无锁设计. **DONE**
        - Performance: 17m17.305s seems the same.
      - 每个单独的libaio读这些变量全部弄成thread_local. 减少系统调用的次数。
- **Read Queue**: 实现一个读调度器，就是在64个请求中，找到离文件offset最近的.尽量形成顺序读。[working]
  - Step 0. 针对读的场景，直接看有没有写的人，如果没有，那么全部高并发地读。不需要限制64线程的情况，以及先后的顺序，注意在队列中拿到请求以后，注意合并。
  - Step 1. 加上一个读Queue，那么所有的请求都会放到这个队列中 ~
  - Step 2. 一次收到64个请求，然后利用并发O_DIRECT + Libaio直接将这个64个请求并发地进行。
  - Step 3. 收到请求之后，返回请求线程。readrandom: 49154 ops/second 线程数并不高.
  - Step 4: 由于并发性，对于多线程情况下的读来说，由于可以异步进行，尽管hash查找可能会比较慢
    - 但是那只是针对于第一个请求来说。操作顺序可以是这样的：
      - Req.1 查hash
        - Req 1. 读盘. --> 并发地查其他请求的计算信息，比如位于哪个hash shard, 位于shard上的什么位置。
        - Req 1. Done -> 返回
        - Req other. 读盘 
        - 由于读请求只是会锁住hash表，所以，当Req other在进行读盘的时候，就应该把hash表释放出来。给后面的线程并发进行查询。
- 写入的处理 & 与有序性
  - 在写value 的4KB的时候，使用异步io
    - 在这段时间里面，可以用来在hash shard里面寻址，找到自己的位置，进行插入排序
    - Hash直接切片，切片时，每个片大概100个。那么总共的shard就是64M/100  = 640K
      - 二分查找 ~= lg(640K) = 最多10次
      - 排序复杂度 100 * lg(100) ~= 200
      - 写的时候是否可以根据进程分文件，这样写入的时候.
- 异步-非阻塞写文件并行写
  - Step 1. 写时加上libaio，可能只有data文件那边才需要，index文件碎片写。
    - 如果用libaio读index文件，那么尽量也把<key,pos>压缩一下，这样写入的数据量就会小很多。
      - index如果写入量很小，那么在读取时，就可以利用libaio+O_DIRECT读很大部分数据上来？
        - 这个可以后面再搞
      - value是否考虑去重？
      - 如果进来的同一批value 64个里面有一样的，可以考虑去重!
    - dumpe2fs /dev/vda1 查看块的大小，这里是4kb
  - Step 2. 计算与存储并行
- - 
- 
- 每个请求上锁是否显得太重？有没有什么优化手段
- 输出每个vector长度情况? 看一下是否有那种很长的
- 队列最好是用环形数组来实现，用dequeue分不停地申请与释放内存
- 读写分别是两个线程，是否需要pin在某个CPU上?
- 整数的压缩64位

# Test Results

Write test

```
fio -filename=/tmp/write.dat -direct=1 -iodepth 64 -thread -rw=randwrite -ioengine=libaio -bs=4k -size=10G -numjobs=2 -runtime=1200 -group_reporting -name=volume-test >log.volume-iowritetest 2>&1 &
```

Write IOPS: **68867.40/s** Speed: **316.09MB/s**

```Cpp
 fio -filename=/tmp/write.dat -direct=1 -iodepth 64 -thread -rw=randread -ioengine=libaio -bs=4k -size=10G -numjobs=2 -runtime=1200 -group_reporting -name=volume-test >log.volume-iowritetest 2>&1 &
```



Read IOPS: io=20,480MB, bw=550,911KB/s, iops=137,727, runt= 38,067msec. ~= 137K



Read Opt

Change to use `value->resize()`

```Cpp
Read test must drop cache:
echo 3 > /proc/sys/vm/drop_caches

After opt:
7401.60 IOPS    33.96 MB
```

-  不要用unsigned value相减。

Update the code to use hash<int64_t,uint32_t>

```Cpp
real  1m30.212s
user  0m10.164s
sys  0m41.892s
```

Update code to use hash_tree + vector

```Cpp
real  1m29.494s
user  0m10.704s
sys  0m35.308s
    
Read:
cnt = 34000
[WARN]: TEST_NOT_FOUND [PASS] can not find the item. ret = 1

real  3m41.149s
user  0m5.500s
sys  0m34.172s
```

Change Q sleep for smaller size = 64:

```Cpp
20nano seconds.
real  1m29.181s
user  0m12.356s
sys  0m38.040s

sleep for 5 nanoseconds.
real  1m28.227s
user  0m12.588s
sys  0m36.200s
    
sleep for 3 nano seconds.
    real  1m33.680s
user  0m14.308s
sys  0m37.232s
```

# References

- 直接编译到库里面

内存对齐与块大小。

```Cpp
# dumpe2fs /dev/vda1
Block size:               4096
Fragment size:            4096
```

内存对齐的操作。方法1

```Cpp
void * buf = NULL;
posix_memalign(&buf, 4096, BUF_SIZE); // 以内存大小地址对齐
```

方法2

```Cpp
#include <stdint.h>

#define CHUNK_ALIGNMENT       4096    // align to 512-byte boundary
#define CHUNK_ALIGNMENT_MASK  (~(CHUNK_ALIGNMENT - 1))

// 这里是ROUND_UP(4096)
static inline size_t align_size (size_t unaligned) {
    return((unaligned + CHUNK_ALIGNMENT - 1 ) &
            CHUNK_ALIGNMENT_MASK);
}
// 给定一个地址，得到这个地址往上取整4096时的地址,也是ROUND_UP(4096)
static inline void *align_buf (void *unaligned) {
    return((void *)((intptr_t)(unaligned + CHUNK_ALIGNMENT - 1) &
         CHUNK_ALIGNMENT_MASK));
}

// 判断是否align
static inline bool buf_aligned (void *ptr) {
    return(((intptr_t)(ptr) & (~CHUNK_ALIGNMENT_MASK)) == 0);
}
```

对齐的说明

```Cpp
在之前的 open 使用标记里面, 我提到了 O_DIRECT 标志的使用, 使用 DMA 的方式,
数据不经过内核空间, 直接在用户空间和设备之间传输. 在文章的测试例子里面我放了一个
小错误, 在使用这个标志时读写文件时没有对相关的参数对齐.

实际上, 使用 O_DIRECT 打开的文件要求读写的 buffer 和 buffer_size 和读写偏移
都要做 I/O 对齐, 对齐的单位为 logical_block_size, 是存储设备能寻址的最小存储
单元, 可以用过下列指令查看该值:

# cat /sys/block/sda/queue/logical_block_size 
512

```

Example

`buffer_size` 和偏移的对其都比较好处理, 但是 buffer 地址的对其不太方便, 不过
glibc 提供了 `posix_memalign()` 函数, 可以返回一个对齐后的 buffer.

下面是使用的小例子:

```Cpp
unsigned char buf[512] = "1234567890";
void *align_buf = NULL;
 
/* 假设 /sys/block/sda/queue/logical_block_size 为 512B */
if (posix_memalign(&align_buf, 512, sizeof(buf)) != 0) {
    perror("memalign failed");
    return;
}
int len = pwrite(fd, align_buf, sizeof(buf), offset);
/* ... ... */
```

# Test Result

把读改成串行化，只有在Hash表的部分需要加锁，但是在读和打开文件的时候，还是用的原来带缓存的方式。

```CPP
------------------------------------------------
readrandom
DB path: [test_directory]
/home/haiqing.shq/polardbrace/benchmark/engine/engine_race/engine_race.cc/home/haiqing.shq/polardbrace/benchmark/engine/engine_race/engine_race.cc::9861:WriteEntry()msg=:ReadEntry()msg=Successdb::ReadEntry()
Successdb::WriteEntry()
readrandom   :       2.666 micros/op 375076 ops/sec; 1422.1 MB/s (1000000 of 1000000 found)

Microseconds per read:
Count: 64000000 Average: 164.9907  StdDev: 60.58
Min: 0  Median: 145.6892  Max: 14531
Percentiles: P50: 145.69 P75: 174.16 P99: 569.89 P99.9: 1665.09 P99.99: 1880.31
------------------------------------------------------
[       0,       1 ]  1994413   3.116%   3.116% #
(       1,       2 ]     1396   0.002%   3.118%
(       2,       3 ]      391   0.001%   3.119%
(       3,       4 ]     2214   0.003%   3.123%
(       4,       6 ]      985   0.002%   3.124%
(       6,      10 ]      981   0.002%   3.126%
(      10,      15 ]      174   0.000%   3.126%
(      15,      22 ]    23196   0.036%   3.162%
(      22,      34 ]   509979   0.797%   3.959%
(      34,      51 ]   383966   0.600%   4.559%
(      51,      76 ]  2789953   4.359%   8.918% #
(      76,     110 ]  3836942   5.995%  14.913% #
(     110,     170 ] 37751642  58.987%  73.900% ############
(     170,     250 ] 13539912  21.156%  95.056% ####
(     250,     380 ]  2345709   3.665%  98.722% #
(     380,     580 ]   187634   0.293%  99.015%
(     580,     870 ]   135762   0.212%  99.227%
(     870,    1300 ]   333038   0.520%  99.747%
(    1300,    1900 ]   160583   0.251%  99.998%
(    1900,    2900 ]      921   0.001% 100.000%
(    2900,    4400 ]       47   0.000% 100.000%
(    4400,    6600 ]       31   0.000% 100.000%
(    6600,    9900 ]       83   0.000% 100.000%
(    9900,   14000 ]       47   0.000% 100.000%
(   14000,   22000 ]        1   0.000% 100.000%

(     110,     170 ] 37 751 642  58.987%  73.900% ############
说明大部分的请求都是在110 ~ 170 / s. 也就是 37.7M item都是落在这个区间. 占了58%
------------------------------------------------
!!!Competion Report!!!
         readrandom: 375076 ops/second
disk usage: 251220 MB
============================================================================
= TEST Result
=================Hash Shard With Spin Lock==================================
=-- Buffered IO. no libaio
DoorPlate::Init()
DoorPlace::Init() mkdir test_directory/index success
Date:       Sat Nov  3 07:15:30 2018
CPU:        64 * Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz
CPUCache:   40960 KB
Keys:       8 bytes each
Values:     4096 bytes each
Entries:    1000000
RawSize:    3913.9 MB (estimated)
FileSize:   1960.8 MB (estimated)
------------------------------------------------
fillrandom
DB path: [test_directory]
/home/haiqing.shq/polardbrace/benchmark/engine/engine_race/engine_race.cc/home/haiqing.shq/polardbrace/benchmark/engine/engine_race/engine_race.cc::61:WriteEntry()msg=Successdb::WriteEntry()98:ReadEntry()msg=
Successdb::ReadEntry()
fillrandom   :      14.420 micros/op 69347 ops/sec;  271.4 MB/s
Microseconds per write:
Count: 64000000 Average: 922.8593  StdDev: 55.85
Min: 159  Median: 1074.4045  Max: 90015
Percentiles: P50: 1074.40 P75: 1187.22 P99: 1295.52 P99.9: 1299.58 P99.99: 1299.99
------------------------------------------------------
(     110,     170 ]       62   0.000%   0.000%
(     170,     250 ]      226   0.000%   0.000%
(     250,     380 ]      415   0.001%   0.001%
(     380,     580 ]      653   0.001%   0.002%
(     580,     870 ]  3008967   4.702%   4.704% #
(     870,    1300 ] 60984781  95.289%  99.992% ###################
(    1300,    1900 ]     4764   0.007% 100.000%
(    1900,    2900 ]       65   0.000% 100.000%
(    2900,    4400 ]        3   0.000% 100.000%
(   50000,   75000 ]       20   0.000% 100.000%
(   75000,  110000 ]       44   0.000% 100.000%


------------------------------------------------
!!!Competion Report!!!
         fillrandom: 69347 ops/second
disk usage: 251220 MB
------------------------------------------------
DoorPlate::Init()
DoorPlace::Init() mkdir test_directory/index success
Date:       Sat Nov  3 07:30:59 2018
CPU:        64 * Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz
CPUCache:   40960 KB
Keys:       8 bytes each
Values:     4096 bytes each
Entries:    1000000
RawSize:    3913.9 MB (estimated)
FileSize:   1960.8 MB (estimated)
------------------------------------------------
readrandom
DB path: [test_directory]
/home/haiqing.shq/polardbrace/benchmark/engine/engine_race/engine_race.cc/home/haiqing.shq/polardbrace/benchmark/engine/engine_race/engine_race.cc::9861:WriteEntry()msg=:ReadEntry()msg=Successdb::ReadEntry()
Successdb::WriteEntry()
readrandom   :       2.666 micros/op 375076 ops/sec; 1422.1 MB/s (1000000 of 1000000 found)

Microseconds per read:  微秒
Count: 64000000 Average: 164.9907  StdDev: 60.58
Min: 0  Median: 145.6892  Max: 14531
Percentiles: P50: 145.69 P75: 174.16 P99: 569.89 P99.9: 1665.09 P99.99: 1880.31
------------------------------------------------------
[       0,       1 ]  1994413   3.116%   3.116% #
(       1,       2 ]     1396   0.002%   3.118%
(       2,       3 ]      391   0.001%   3.119%
(       3,       4 ]     2214   0.003%   3.123%
(       4,       6 ]      985   0.002%   3.124%
(       6,      10 ]      981   0.002%   3.126%
(      10,      15 ]      174   0.000%   3.126%
(      15,      22 ]    23196   0.036%   3.162%
(      22,      34 ]   509979   0.797%   3.959%
(      34,      51 ]   383966   0.600%   4.559%
(      51,      76 ]  2789953   4.359%   8.918% #
(      76,     110 ]  3836942   5.995%  14.913% #
(     110,     170 ] 37751642  58.987%  73.900% ############
(     170,     250 ] 13539912  21.156%  95.056% ####
(     250,     380 ]  2345709   3.665%  98.722% #
(     380,     580 ]   187634   0.293%  99.015%
(     580,     870 ]   135762   0.212%  99.227%
(     870,    1300 ]   333038   0.520%  99.747%
(    1300,    1900 ]   160583   0.251%  99.998%
(    1900,    2900 ]      921   0.001% 100.000%
(    2900,    4400 ]       47   0.000% 100.000%
(    4400,    6600 ]       31   0.000% 100.000%
(    6600,    9900 ]       83   0.000% 100.000%
(    9900,   14000 ]       47   0.000% 100.000%
(   14000,   22000 ]        1   0.000% 100.000%


------------------------------------------------
!!!Competion Report!!!
         readrandom: 375076 ops/second
disk usage: 251220 MB
------------------------------------------------
```

# PreparRead/PrepareWrite

```Cpp
static inline void io_set_callback(struct iocb *iocb, io_callback_t cb)
{
  iocb->data = (void *)cb;
}

static inline void io_prep_pread(struct iocb *iocb, int fd, void *buf, size_t count, long long offset)
{
  memset(iocb, 0, sizeof(*iocb));
  iocb->aio_fildes = fd;
  iocb->aio_lio_opcode = IO_CMD_PREAD;
  iocb->aio_reqprio = 0;
  iocb->u.c.buf = buf;
  iocb->u.c.nbytes = count;
  iocb->u.c.offset = offset;
}

static inline void io_prep_pwrite(struct iocb *iocb, int fd, void *buf, size_t count, long long offset)
{
  memset(iocb, 0, sizeof(*iocb));
  iocb->aio_fildes = fd;
  iocb->aio_lio_opcode = IO_CMD_PWRITE;
  iocb->aio_reqprio = 0;
  iocb->u.c.buf = buf;
  iocb->u.c.nbytes = count;
  iocb->u.c.offset = offset;
}
```

# 多线程写入

- 如何让多个线程之间并发协作共同写入
  - class.flag atomic变量
    初值为-1
    // 有64个槽位
    // 每个线程只处理自己的槽位
    every thread::Write(){
      int idx = flag++;
      // set key,value in [idx]
      if flag == 63 {  // 63 can be change to class.thread_id++;
        I;m the write thrda.
        use fd to write.
        after write over,
        flag = -1;
      } else {
        while(flag != -1) -> just busy wait.
      }
    }

- parallel to generate the hash
  - t1: deal 0, 64, 128, ....
    t2:      1, 65, 129, .....
    t64:     63, .....

- cache all the key/value, then write into single index big file.
