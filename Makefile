CXX ?= g++
CXXFLAGS ?= -std=c++11 -O2 -Wall -Wextra -Wpedantic
LDLIBS ?= -libverbs

.PHONY: all clean

all: test

test: ex3n.cpp main.cpp allreduce.h
	$(CXX) $(CXXFLAGS) ex3n.cpp main.cpp -o $@ $(LDLIBS)

clean:
	$(RM) test
