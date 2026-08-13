# Exercise 3: Ring collectives with RDMA Verbs

This project implements single-threaded ring reduce-scatter, all-gather, and
all-reduce collectives with libibverbs.

See [`CODE_GUIDE.md`](CODE_GUIDE.md) for the full conceptual explanation,
running instructions, and line-by-line source walkthrough.

Two transfer protocols are available:

- **Eager:** `IBV_WR_SEND_WITH_IMM`; the receiver posts a destination buffer.
- **Rendezvous:** `IBV_WR_RDMA_WRITE_WITH_IMM`; reduce-scatter writes into a
  registered scratch block and all-gather writes directly into the final
  receive buffer.

Messages are divided into configurable blocks. During reduce-scatter, the CPU
reduces each received block while the corresponding outgoing transfer can
remain in flight.

## Build

On every RDMA-capable Linux host:

```sh
sudo apt install build-essential libibverbs-dev
make
```

## Run

The ordered host list defines the ring. Start one process per rank using the
same host list. TCP port `18515 + rank` is used for bootstrap and control.
The exercise syntax uses one-based `-myindex` values.

Two ranks:

```sh
# host rdma0
./test -myindex 01 -list rdma0 rdma1 --count 1048576 --protocol rendezvous

# host rdma1
./test -myindex 02 -list rdma0 rdma1 --count 1048576 --protocol rendezvous
```

Four ranks:

```sh
./test -myindex 01 -list rdma0 rdma1 rdma2 rdma3 --protocol eager
./test -myindex 02 -list rdma0 rdma1 rdma2 rdma3 --protocol eager
./test -myindex 03 -list rdma0 rdma1 rdma2 rdma3 --protocol eager
./test -myindex 04 -list rdma0 rdma1 rdma2 rdma3 --protocol eager
```

Use identical `--count`, `--datatype`, `--op`, `--protocol`, and
`--iterations` values on every rank.

## Configuration

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `PG_BASE_PORT` | `20000 + UID % 40000` | TCP bootstrap base port |
| `PG_DEVICE` | first device | libibverbs device name |
| `PG_IB_PORT` | `1` | RDMA device port |
| `PG_GID_INDEX` | `-1` | GID index; set for RoCE |
| `PG_PROTOCOL` | `auto` | `eager`, `rendezvous`, or threshold-based `auto` |
| `PG_EAGER_THRESHOLD` | `8192` | Auto-mode eager limit in bytes |
| `PG_PIPELINE_BYTES` | `65536` | Pipeline block size, aligned to 8 bytes |

For InfiniBand, the default LID-based addressing normally works. For RoCE,
set `PG_GID_INDEX` to the correct local GID table index on every host.

## Public API

The API, implementation, and benchmark program are all in `Ex3.c`.
The exercise CLI converts its one-based index and space-separated host list
into the `PG_RANK` and comma-separated list used by
`connect_process_group()`.

`pg_reduce_scatter()` and `pg_all_gather()` use equal per-rank counts.
`pg_all_reduce()` accepts any total count, including counts not divisible by
the number of ranks.
