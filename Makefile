CXX ?= g++
CXXFLAGS ?= -std=c++11 -O2 -Wall -Wextra -Wpedantic
LDLIBS ?= -libverbs

.PHONY: all clean

all: test

test: allreduce.cpp main.cpp allreduce.h
	$(CXX) $(CXXFLAGS) allreduce.cpp main.cpp -o $@ $(LDLIBS)

clean:
	$(RM) test
