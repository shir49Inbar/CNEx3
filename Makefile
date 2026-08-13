CC ?= gcc
CFLAGS ?= -O2 -g
CFLAGS += -std=gnu11 -Wall -Wextra -Wpedantic
LDLIBS += -libverbs -lm

.PHONY: all clean

all: ex3 test

ex3: Ex3.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

test: ex3
	ln -sf ex3 test

clean:
	$(RM) ex3 test
