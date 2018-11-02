SRCS = $(wildcard *.cc)

PROGS = $(patsubst %.cc,%,$(SRCS))

all: $(PROGS)

# g++ -O3 -std=c++11 -o prod_test -g -I.. prod_test.cc -L../lib -lengine -lpthread -lrt
%: %.cc
	g++ -O3 -std=c++11 -o $@ -g -I.. $< -L../lib -lengine -lpthread -lrt

clean:
	$(RM) $(PROGS)
