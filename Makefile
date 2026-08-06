CC ?= gcc
CFLAGS ?= -O2 -g
CFLAGS += -std=gnu11 -Wall -Wextra -Wpedantic
LDLIBS += -libverbs -lm

.PHONY: all clean

all: ex3

ex3: Ex3.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

clean:
	$(RM) ex3
