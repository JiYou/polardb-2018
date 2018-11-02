SRCS = $(wildcard *.cc)

PROGS = $(patsubst %.cc,%,$(SRCS))

all: $(PROGS)

# g++ -O3 -std=c++11 -o prod_test -g -I.. prod_test.cc -L../lib -lengine -lpthread -lrt
%: %.cc
	echo 3 > /proc/sys/vm/drop_caches
	g++ -O3 -std=c++11 -o $@ -g -I.. $< -L../lib -lengine -lpthread -lrt

clean:
	$(RM) $(PROGS)
