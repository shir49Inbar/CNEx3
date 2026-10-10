# Submission and Interview Guide

This guide summarizes the official exercise requirements, what should be
included in the submission, what should still be validated, and the main
implementation details to remember for the interview.

## 1. Official Exercise Requirements

The exercise PDF explicitly asks for:

1. A single-threaded C or C++ application.
2. Communication through the InfiniBand Verbs API.
3. At least two processes connected in a ring.
4. Testing with both two and four processes.
5. Reduce Scatter.
6. All Gather.
7. All Reduce implemented from Reduce Scatter and All Gather.
8. The following public API:

```cpp
int connect_process_group(char *servername, void **pg_handle);

int pg_all_reduce(void *sendbuf,
                  void *recvbuf,
                  int count,
                  DATATYPE datatype,
                  OPERATION op,
                  void *pg_handle);

int pg_close(void *pg_handle);
```

9. A custom `pg_handle` structure created by the implementation and used by
   every API call.
10. A comparison between a trivial Eager protocol and Rendezvous.
11. Pipelining that overlaps communication and computation.
12. RDMA Read or RDMA Write for large-message zero-copy All Gather.

RDMA Write with Immediate is allowed for completion notification but is not
mandatory.

The PDF gives a launch format similar to:

```text
test -myindex 01 -list host1 host2 host3 host4
```

The PDF does not state the exact submission portal, archive name, required
report format, or whether benchmark output must be submitted. Confirm those
administrative details on the course website or with the teaching staff.

## 2. Requirement Coverage

| Requirement | Current implementation |
|---|---|
| C/C++ single-threaded application | C++11, one application thread |
| Verbs API | libibverbs device, PD, CQ, MR, QP, SEND/RECV, RDMA Write |
| Ring topology | One QP to PREV and one QP to NEXT |
| Two and four processes | Tested successfully with both |
| Reduce Scatter | `reduce_scatter()` |
| All Gather | `all_gather()` |
| All Reduce | `pg_all_reduce()` calls both phases |
| Custom handle | Private `process_group` returned as `pg_handle` |
| Eager | `IBV_WR_SEND` and posted receives |
| Rendezvous | REQUEST/READY followed by RDMA Write with Immediate |
| Pipelining | 64 KiB segments and two staging slots |
| Communication/computation overlap | Next segment posted before current reduction |
| Zero-copy All Gather | RDMA Write directly into the final `recv_buf` chunk |
| Cleanup | `pg_close()` destroys and releases RDMA resources |

## 3. Recommended Submission Package

Unless the course instructions say otherwise, submit these source files:

```text
allreduce.h
allreduce.cpp
main.cpp
Makefile
README.md
FUNCTION_GUIDE.md
SUBMISSION_INTERVIEW_GUIDE.md
```

The essential files required to build are:

```text
allreduce.h
allreduce.cpp
main.cpp
Makefile
```

Do not submit generated files unless requested:

```text
test
*.o
```

Run this before creating an archive:

```bash
make clean
```

If a compressed submission is required:

```bash
tar -czf CNEx3.tar.gz \
    allreduce.h \
    allreduce.cpp \
    main.cpp \
    Makefile \
    README.md \
    FUNCTION_GUIDE.md \
    SUBMISSION_INTERVIEW_GUIDE.md
```

## 4. Final Checks Before Submission

### Required checks

1. Clone or copy the final version into a clean directory.
2. Build with:

```bash
make clean
make
```

3. Confirm that the build has no errors.
4. Run the complete benchmark with two processes.
5. Run the complete benchmark with four processes.
6. Confirm that every row prints `PASS`.
7. Save the two-process and four-process benchmark outputs.
8. Confirm `pg_close()` completes without an error.

### Additional checks strongly recommended

The current benchmark exercises `TYPE_INT32` with `OP_SUM`. Before submission,
also verify:

- `TYPE_INT32` with `OP_PRODUCT`.
- `TYPE_FP64` with `OP_SUM`.
- `TYPE_FP64` with `OP_PRODUCT`.
- An in-place call where `send_buf == recv_buf`.
- Rejection of a count that is not divisible by the process count.

These combinations are implemented but are not covered by the default
benchmark.

### Benchmark improvements

Five iterations are sufficient for a smoke test but not for stable
performance conclusions. For final measurements:

- Use 50 to 100 measured iterations.
- Report median or percentile latency in addition to average and minimum.
- Run when the machines are not heavily loaded.
- Keep the same host order for every process.
- Remember that the current table reports rank-zero local time, not the
  maximum time across all ranks.

### Robustness improvements worth considering

These are not required by the exercise PDF, but they improve the project:

- Make TCP bootstrap port `18515` configurable.
- Use the queried active MTU instead of hard-coded `IBV_MTU_1024`.
- Add GID-based addressing if the code must work on RoCE.
- Handle interrupted TCP `send` and `recv` calls.
- Serialize bootstrap data instead of sending native C structures directly.
- Add an explicit API for registering long-lived user buffers instead of
  relying on the documented receive-buffer lifetime.

## 5. Thirty-Second Project Explanation

Use this as a short interview introduction:

> We implemented single-threaded Ring All-Reduce with the Verbs API. Every
> process owns two reliable-connected QPs, one to its previous neighbor and
> one to its next neighbor. TCP is used only during bootstrap to exchange LID,
> QP number, and PSN. All-Reduce is implemented as P minus one Reduce Scatter
> steps followed by P minus one All Gather steps. Eager uses SEND and RECV.
> Rendezvous exchanges the destination address and rkey once per chunk and
> transfers 64 KiB segments using RDMA Write with Immediate. Reduce Scatter
> uses two staging slots so the next transfer can overlap the current CPU
> reduction. All Gather writes directly into the final receive buffer.

## 6. Ring All-Reduce Algorithm

Assume:

- `P` processes.
- `N` total elements per process.
- `N` is divisible by `P`.
- Each chunk contains `N / P` elements.

### Reduce Scatter

Every process begins with a complete input vector. During each of `P - 1`
steps:

1. Send one chunk to NEXT.
2. Receive one chunk from PREV.
3. Reduce the incoming chunk into the corresponding local chunk.

The implementation uses:

```cpp
send_chunk = (pid - step - 1 + P) % P;
recv_chunk = (pid - step - 2 + P) % P;
```

After `P - 1` steps, every process owns one fully reduced chunk.

### All Gather

The reduced chunks circulate for another `P - 1` steps:

```cpp
send_chunk = (pid - step + P) % P;
recv_chunk = (pid - step - 1 + P) % P;
```

After All Gather, every process has the complete reduced vector.

### Cost

Each phase has `P - 1` communication steps. Each process sends:

```text
(P - 1) / P * N
```

elements per phase. Across both phases:

```text
2 * (P - 1) / P * N
```

elements are sent by each process.

The ring algorithm does not require `P` to be a power of two. This
implementation requires `count` to be divisible by `P` because all chunks
have equal size.

## 7. Eager Protocol

Eager uses two-sided Verbs communication:

1. The receiver posts an `ibv_recv_wr`.
2. The sender posts `IBV_WR_SEND`.
3. The SEND consumes the posted receive WQE.
4. Both sides observe completions on the shared CQ.

Advantages:

- Very little setup for each message.
- Best for small messages.

Disadvantages:

- The receiver must have posted a receive.
- Large messages require multiple SEND/RECV operations.
- The current All Gather handles Eager segments sequentially.

## 8. Rendezvous Protocol

Rendezvous separates control from data.

### Chunk handshake

Once per ring chunk:

1. Sender sends `RENDEZVOUS_REQUEST(chunk_size)`.
2. Receiver posts the first Write-with-Immediate notification receive.
3. Receiver replies with `RENDEZVOUS_READY(base_address, rkey)`.
4. Sender stores the remote region.

### Segment transfer

For every 64 KiB segment:

1. Receiver posts a zero-SGE receive WQE for notification.
2. Sender posts `IBV_WR_RDMA_WRITE_WITH_IMM`.
3. Immediate data contains the segment number.
4. Receiver verifies the segment number.
5. Sender collects the RDMA Write completion.

The handshake is performed once per chunk, not once per segment. This
amortizes the control-message cost over the complete chunk.

### Reduce Scatter addresses

Reduce Scatter has two registered staging slots:

```text
slot 0 = staging_buffer
slot 1 = staging_buffer + SEGMENT_SIZE
```

Segment `i` is written to:

```text
remote_staging_base + (i % PIPELINE_DEPTH) * SEGMENT_SIZE
```

### All Gather addresses

All Gather advertises the final destination chunk. Segment `i` is written to:

```text
remote_chunk_base + segment_offset
```

This is zero-copy for the All Gather receive path because data arrives
directly in its final position.

## 9. Pipelining

`SEGMENT_SIZE` is 64 KiB and `PIPELINE_DEPTH` is two.

For Reduce Scatter:

```text
Wait for incoming segment i
Post notification receive for segment i + 1
Post outgoing transfer for segment i + 1
Reduce segment i on the CPU
Collect outgoing completion for segment i
```

The NIC can transfer segment `i + 1` while the CPU reduces segment `i`.

The application is still single-threaded. Overlap is possible because the
network adapter progresses posted work asynchronously.

## 10. Memory Registration

### Why registration is required

RDMA hardware cannot access arbitrary virtual memory. `ibv_reg_mr` pins and
authorizes a memory range and returns:

- `lkey`: used for local SGEs.
- `rkey`: given to a remote peer for RDMA access.

### Current lifetime

- Control buffers are allocated and registered during process-group setup.
- The two-slot staging buffer is allocated and registered once.
- The receive-buffer MR is reused while its address and registered capacity
  remain compatible.
- If the requested size grows or the pointer changes, the old receive MR is
  deregistered and a new one is created.
- All remaining MRs are deregistered by `pg_close`.

The caller must keep a registered receive buffer alive until another receive
buffer replaces it or until `pg_close`.

The benchmark satisfies this rule by allocating maximum-sized vectors once
and destroying them only after `pg_close`.

## 11. Connection Setup

### TCP bootstrap

TCP is used only to exchange:

- LID
- QP number
- PSN

Each process listens for PREV and connects to NEXT. Even and odd ranks use
different accept/connect order to break circular waiting.

### QP state transitions

Every RC QP moves through:

```text
RESET -> INIT -> RTR -> RTS
```

- `INIT`: local port, pkey, and access flags.
- `RTR`: remote QP number, remote PSN, LID, MTU, and receive parameters.
- `RTS`: local PSN, timeout, retry, and sender parameters.

The pairings are:

```text
local next_qp <-> NEXT process prev_qp
local prev_qp <-> PREV process next_qp
```

## 12. Completion Handling

Both QPs use one shared completion queue.

`wait_for_completion()` waits for a specific `wr_id`. If another successful
completion arrives first, it stores it in `pending_wc` for a later wait.

Rendezvous work-request IDs combine:

- A work-request category in the high bits.
- A segment number in the low bits.

This allows multiple pipelined operations to be distinguished.

## 13. Important Interview Questions

### Why use a ring?

It is bandwidth-efficient and distributes communication evenly. Every process
communicates only with two neighbors.

### Why are there two QPs?

One QP sends to and receives from NEXT, and the other sends to and receives
from PREV. The ring needs independent reliable connections in both
directions.

### Why use TCP if this is an RDMA exercise?

Verbs does not automatically exchange connection metadata. TCP is used only
as an out-of-band bootstrap channel. Collective data uses RDMA.

### What is the difference between Eager and Rendezvous?

Eager sends immediately and requires a posted receive. Rendezvous first
exchanges destination metadata, then transfers data with one-sided RDMA.

### Why is Eager faster for small messages?

Its control path is shorter. Rendezvous REQUEST/READY overhead dominates small
payloads.

### Why can Rendezvous become faster for large messages?

Handshake cost is amortized over more data, transfers are pipelined, and All
Gather writes directly to the final destination.

### What does Write with Immediate provide?

It performs an RDMA Write and also generates a receive completion at the
remote side. The immediate value identifies the segment.

### Why is a receive WQE needed for Write with Immediate?

The data is written using the remote address and rkey, but the immediate
notification consumes a receive WQE to create the remote completion.

### What does zero-copy mean here?

During Rendezvous All Gather, incoming data is written directly into its final
location in `recv_buf`; there is no intermediate receive buffer and copy.

### Is Reduce Scatter zero-copy?

No. Incoming Reduce Scatter data lands in the staging buffer because the CPU
must combine it with the local destination chunk.

### How does the pipeline overlap work?

The next network operation is posted before the CPU reduces the current
segment. The RNIC progresses the posted transfer while the application thread
executes `reduce`.

### Why is rank parity used?

If every process waited to receive a control request before sending one, the
ring could deadlock. Alternating sender-first and receiver-first order breaks
the cycle.

### Why must count be divisible by P?

The implementation uses equal-sized chunks. Supporting a remainder would
require uneven chunk metadata or padding.

### Does the ring size need to be a power of two?

No. Ring All-Reduce works for any `P >= 2`. Power-of-two restrictions are
associated with other collective algorithms such as recursive doubling.

### Why reuse memory registrations?

Registration is expensive. Reusing MRs removes registration overhead from
measured iterations and reduces repeated pin/unpin work.

### What happens if the caller frees a cached receive buffer too early?

The MR would refer to invalid memory. The buffer must stay allocated until it
is replaced or `pg_close` deregisters it.

### Why busy-poll the CQ?

Busy polling avoids interrupt and event-channel latency, which is appropriate
for a low-latency exercise, but it consumes a CPU core while waiting.

### Why did benchmark averages contain large spikes?

All-Reduce waits for peer progress. Operating-system scheduling, machine load,
or network contention on any participant can delay a complete iteration.
More iterations and median/percentile statistics are needed for stable
performance conclusions.

## 14. Known Limitations

Be ready to state these honestly:

- Fixed TCP port `18515`.
- First detected RDMA device and port 1 are used.
- LID addressing assumes InfiniBand rather than RoCE.
- Path MTU is fixed at 1024.
- Bootstrap structures are transferred in native layout and byte order.
- CQ polling is a busy loop.
- Benchmark output reports rank-zero timing only.
- Default benchmark covers only INT32 SUM.
- Cached receive-buffer lifetime is an additional API requirement.

These limitations do not invalidate the exercise implementation, but they are
reasonable areas for future improvement.

## 15. Interview Walkthrough Order

When asked to explain the code, use this order:

1. Show `allreduce.h` and the required API.
2. Explain `process_group`.
3. Explain TCP bootstrap and QP state transitions.
4. Explain memory registration and keys.
5. Explain Eager SEND/RECV.
6. Explain the Rendezvous chunk handshake.
7. Explain segmented RDMA Writes and immediate data.
8. Explain Reduce Scatter chunk indices.
9. Explain the two-slot pipeline.
10. Explain All Gather and zero-copy destination writes.
11. Explain MR reuse and buffer lifetime.
12. Explain cleanup in `pg_close`.
13. Finish with two-process and four-process benchmark results.

The most important distinction to remember is:

```text
Eager minimizes setup overhead.
Rendezvous amortizes setup and optimizes large transfers.
```
