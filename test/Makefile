ALL: write read truncate_test prod_test aio_read aio_example
write: write.cc
	g++ -O3 -std=c++11 -o write -g -I.. write.cc -L../lib -lengine -lpthread -lrt
read: read.cc
	echo 3 > /proc/sys/vm/drop_caches
	g++ -O3 -std=c++11 -o read -g -I.. read.cc -L../lib -lengine -lpthread -lrt
truncate_test: file_truncate.c
	echo 3 > /proc/sys/vm/drop_caches
	g++ -O3 -std=c++11 -o truncate_test -g -I.. file_truncate.c -L../lib -lengine -lpthread -lrt
prod_test: prod_test.cc
	g++ -O3 -std=c++11 -o prod_test -g -I.. prod_test.cc -L../lib -lengine -lpthread -lrt
aio_read: aio_read.cc
	g++ -O3 -std=c++11 -o aio_read -g -I.. aio_read.cc -L../lib -lengine -lpthread -lrt
aio_example: aio_example.cc
	g++ -O3 -std=c++11 -o aio_example -g -I.. aio_example.cc -L../lib -lengine -lpthread -lrt


.PHONY: clean
clean:
	rm -rf read write *.o test truncate *_test
