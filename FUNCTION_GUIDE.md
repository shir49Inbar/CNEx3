# Function Guide

This document explains the structures and functions in `allreduce.h`,
`ex3n.cpp`, and `main.cpp`.

## Execution Flow

```mermaid
flowchart TD
    A["main"] --> B["connect_process_group"]
    B --> C["init_rdma_resources"]
    B --> D["connect_qps"]
    A --> E["run_test: eager"]
    E --> F["pg_all_reduce"]
    A --> G["run_test: rendezvous"]
    G --> F
    F --> H["prepare_data_buffers"]
    H --> I["reduce_scatter"]
    I --> J["all_gather"]
    J --> K["release_data_buffers"]
    A --> L["pg_close"]
```

## Public Types

### `DATATYPE`

Declared in `allreduce.h`.

- `TYPE_INT32` selects 32-bit signed integers.
- `TYPE_FP64` selects double-precision floating-point values.

### `OPERATION`

Declared in `allreduce.h`.

- `OP_SUM` adds corresponding elements.
- `OP_PRODUCT` multiplies corresponding elements.

### `PROTOCOL`

Private to `ex3n.cpp`.

- `PROTOCOL_EAGER` uses Verbs SEND/RECV.
- `PROTOCOL_RENDEZVOUS` uses a control-message handshake followed by RDMA
  Write with Immediate.

### `control_type`

Identifies Rendezvous control messages.

- `RENDEZVOUS_REQUEST` announces a transfer and its size.
- `RENDEZVOUS_READY` returns the destination address and remote key.
- `RENDEZVOUS_FIN` is defined but is not currently used because
  Write-with-Immediate provides completion notification.

### `wr_type`

Assigns categories to work requests. The values identify Eager sends and
receives, control traffic, RDMA writes, and Write-with-Immediate receive
notifications.

## Data Structures

### `control_message`

Carries the Rendezvous handshake:

- `type`: request or ready message.
- `size`: transfer size.
- `addr`: receiver virtual address.
- `rkey`: receiver memory-region remote key.

### `rdma_peer`

Stores information exchanged during TCP bootstrap:

- LID
- Queue-pair number
- Packet sequence number
- Optional remote address and remote key fields

### `process_group`

The private object returned through `pg_handle`. It contains:

- Process rank and ring size.
- Previous and next ranks.
- RDMA device context.
- Protection domain.
- Shared completion queue.
- Queue pairs to the previous and next processes.
- Registered receive and staging buffers.
- Registered Rendezvous control buffers.
- Previous and next peer connection information.
- A small array for completions that arrived before the completion currently
  being awaited.

## General Helpers

### `make_rendezvous_wr_id`

Combines a work-request category with a segment sequence number. The category
is stored in the high bits and the segment number in the low bits. This gives
each pipelined Rendezvous send and receive a distinct completion identifier.

### `datatype_size`

Returns the element size for `TYPE_INT32` or `TYPE_FP64`. It returns zero for
an unsupported datatype.

### `reduce_typed`

Template implementing element-by-element SUM or PRODUCT for a concrete C++
type.

### `reduce`

Dispatches to `reduce_typed<int32_t>` or `reduce_typed<double>` according to
the requested datatype.

## Completion and Receive Helpers

### `post_receive`

Builds one `ibv_sge` and one `ibv_recv_wr`, then posts the receive to the
specified queue pair with `ibv_post_recv`.

### `wait_for_completion`

Waits for a specific work-request ID.

1. Checks previously saved completions.
2. Busy-polls the completion queue with `ibv_poll_cq`.
3. Returns when the expected completion arrives.
4. Saves unrelated successful completions for later.
5. Reports failed completions or pending-array overflow.

This behavior is required because pipelined operations may complete in a
different order from the order in which the CPU waits for them.

## RDMA Resource Management

### `init_rdma_resources`

Creates resources that live for the entire process group:

1. Gets the first available RDMA device.
2. Opens its device context.
3. Allocates a protection domain.
4. Creates a shared completion queue.
5. Allocates and registers control send and receive buffers.
6. Creates one reliable-connected QP for NEXT.
7. Creates one reliable-connected QP for PREV.

### `prepare_data_buffers`

Called at the start of each All-Reduce:

- Registers the user receive buffer for local and remote writes.
- Allocates a staging area containing two 64 KiB pipeline slots.
- Registers the staging area for local and remote writes.

The implementation sends data from `recv_buf`, after the user contribution is
copied into it, so one registered data memory region is sufficient.

### `release_data_buffers`

Deregisters the per-call receive and staging memory regions, frees the staging
buffer, and clears the stored pointers.

### `pg_close`

Releases the complete process group:

1. Releases per-call data buffers if still present.
2. Destroys both queue pairs.
3. Deregisters both control memory regions.
4. Frees both control buffers.
5. Destroys the completion queue.
6. Deallocates the protection domain.
7. Closes the RDMA device.
8. Frees the `process_group`.

Passing a null handle is treated as a successful no-op.

## QP Connection Setup

### `connect_one_qp`

Moves one reliable-connected queue pair through the required states:

```text
RESET -> INIT -> RTR -> RTS
```

It configures remote read/write access, the remote QP number, LID, PSN,
retry settings, and atomic-operation limits.

### `tcp_send_all`

Repeatedly calls `send` until the complete bootstrap structure has been
transmitted or an error occurs.

### `tcp_recv_all`

Repeatedly calls `recv` until the complete bootstrap structure has been
received or an error occurs.

### `tcp_accept_exchange`

Accepts a TCP connection from PREV. The accepted client sends its local peer
description first; the server receives it and replies with its own.

### `tcp_create_listener`

Creates an IPv4 TCP listening socket on the bootstrap port. It enables address
reuse, binds to all local interfaces, and starts listening.

### `tcp_client_exchange`

Resolves the NEXT hostname and repeatedly attempts to connect. After
connecting, it sends the local peer description and receives the remote peer
description.

### `connect_qps`

Builds the two ring links:

1. Queries the local RDMA port LID.
2. Creates separate PSNs for the two local QPs.
3. Starts a TCP listener on port 18515.
4. Exchanges QP information with PREV and NEXT.
5. Alternates connect/accept order by rank parity to avoid bootstrap deadlock.
6. Connects both QPs through `connect_one_qp`.

The resulting pairings are:

```text
local next_qp <-> NEXT process prev_qp
local prev_qp <-> PREV process next_qp
```

### `parse_server_config`

Parses the exercise command format:

```text
-myindex INDEX -list HOST1 HOST2 ...
```

The command-line index is one-based and is converted to a zero-based internal
rank. At least two hosts are required.

### `connect_process_group`

Public setup API:

1. Parses rank and host list.
2. Allocates and initializes `process_group`.
3. Computes previous and next ranks with modulo arithmetic.
4. Initializes local RDMA resources.
5. Connects the two QPs.
6. Returns the group through `pg_handle`.

On failure, it calls `pg_close` to release partially created resources.

## Eager Protocol

### `send_eager`

Blocking Eager sender. It posts `IBV_WR_SEND` to `next_qp`, waits for the send
completion, and verifies the completion opcode.

This helper is currently available for simple blocking transfers; the
collective pipeline uses the nonblocking posting helpers instead.

### `post_eager_send`

Posts an Eager SEND with the standard Eager work-request ID and returns
without waiting. All Gather uses it before waiting separately for receive and
send completions.

### `post_eager_send_with_id`

Posts an Eager SEND with a caller-provided ID. Reduce Scatter uses segment
numbers in these IDs so multiple pipeline operations can be distinguished.

### `receive_eager`

Blocking Eager receiver. It posts a receive on `prev_qp` and waits for its
completion. The current collective pipeline posts and waits explicitly
instead of calling this convenience helper.

## Rendezvous Protocol

### `wait_for_control_message`

Posts a receive for a control message on either the previous or next QP,
waits for completion, verifies message size and opcode, and checks the
expected control-message type.

### `send_control_message`

Fills the registered control send buffer, posts an `IBV_WR_SEND` on the
selected QP, and waits until that small control message has been sent.

### `begin_send_rendezvous`

Starts the sender side without waiting for the data-write completion:

1. Sends `RENDEZVOUS_REQUEST`.
2. Waits for `RENDEZVOUS_READY`.
3. Reads the destination address and rkey.
4. Posts `IBV_WR_RDMA_WRITE_WITH_IMM` with the supplied work-request ID.
5. Returns while the data transfer can still be in progress.

### `finish_send_rendezvous`

Waits for the RDMA Write completion matching the supplied work-request ID and
verifies that its opcode is `IBV_WC_RDMA_WRITE`.

### `send_rendezvous`

Blocking convenience wrapper used by All Gather. It calls
`begin_send_rendezvous` and immediately calls `finish_send_rendezvous`.

### `begin_receive_rendezvous`

Starts the receiver side:

1. Waits for `RENDEZVOUS_REQUEST`.
2. Validates the requested size.
3. Posts a zero-SGE receive WQE for the Write-with-Immediate notification.
4. Sends `RENDEZVOUS_READY` containing the target address and rkey.

The data itself is written directly by the remote RDMA operation.

### `finish_receive_rendezvous`

Waits for the matching Write-with-Immediate receive completion and verifies
the `IBV_WC_RECV_RDMA_WITH_IMM` opcode.

### `receive_rendezvous`

Blocking convenience wrapper used by All Gather. It calls
`begin_receive_rendezvous` followed by `finish_receive_rendezvous`.

## Collective Operations

### `reduce_scatter`

Divides the full buffer into `P` equal chunks and performs `P - 1` ring steps.
At each step, a process:

1. Sends one chunk toward NEXT.
2. Receives one chunk from PREV into the staging buffer.
3. Reduces the received values into the corresponding local chunk.

Each chunk is divided into 64 KiB segments. Two staging slots are used in a
round-robin arrangement.

For Eager, the next SEND and receive are posted before reducing the current
segment.

For Rendezvous, each segment has unique send and receive IDs. The next
handshake and RDMA Write are started before reducing the current segment, and
the current send completion is collected after reduction. Rank parity chooses
send-first or receive-first ordering to avoid circular waits.

After `P - 1` steps, each process owns one completely reduced chunk.

### `all_gather`

Circulates the reduced chunks for another `P - 1` ring steps until every
process has the complete result.

- Eager posts a receive directly into the destination chunk, posts SEND, and
  waits for both completions.
- Rendezvous writes directly into the final destination chunk in `recv_buf`.
  This is the large-message zero-copy path.

### `pg_all_reduce`

Public collective API:

1. Validates the handle, buffers, count, datatype, and operation.
2. Chooses Eager or Rendezvous from `ALLREDUCE_PROTOCOL`.
3. Requires `count` to be divisible by the ring size.
4. Copies `send_buf` into `recv_buf` unless the call is in place.
5. Registers data and staging buffers.
6. Calls `reduce_scatter`.
7. Calls `all_gather`.
8. Releases per-call registered memory.

It returns zero on success and `-1` on failure.

## Test Program

### `run_test`

Runs one protocol test:

1. Sets `ALLREDUCE_PROTOCOL`.
2. Allocates an integer input and output buffer.
3. Fills every input element with `rank + 1`.
4. Calls `pg_all_reduce` with `TYPE_INT32` and `OP_SUM`.
5. Checks every output element against `P * (P + 1) / 2`.
6. Prints elapsed time from rank zero.

The count is `P * 65536`, producing a 256 KiB chunk per process and four
64 KiB pipeline segments.

### `main`

The executable entry point:

1. Validates `-myindex INDEX -list HOSTS...`.
2. Determines the local rank and process count.
3. Reconstructs the configuration string for `connect_process_group`.
4. Connects the RDMA ring.
5. Runs the Eager test.
6. Runs the Rendezvous test.
7. Calls `pg_close`.
8. Returns nonzero if setup, either test, or cleanup fails.
