ALL: write read truncate_test
write: write.cc
	g++ -std=c++11 -o write -g -I.. write.cc -L../lib -lengine -lpthread -lrt
read: read.cc
	g++ -std=c++11 -o read -g -I.. read.cc -L../lib -lengine -lpthread -lrt

truncate_test: file_truncate.c
	g++ -std=c++11 -o truncate_test -g -I.. file_truncate.c -L../lib -lengine -lpthread -lrt

.PHONY: clean
clean:
	rm -rf read write *.o test truncate *_test
