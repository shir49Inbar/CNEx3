# Exercise 3 - Ring AllReduce with RDMA

## Overview

This project implements Ring AllReduce using the RDMA Verbs API.

The processes are connected in a ring:

    PREV <----> CURRENT <----> NEXT

Each process therefore maintains two RC Queue Pairs:
- `prev_qp` - communication with the previous process in the ring.
- `next_qp` - communication with the next process in the ring.

The final AllReduce implementation will consist of two phases:

1. Reduce Scatter
2. All Gather

The exercise also compares two communication protocols:
- Eager - for small messages.
- Rendezvous - for large messages using RDMA Write.

The implementation is single-threaded.


## Current Status

### Implemented

- RDMA resource initialization
- Creation of two RC Queue Pairs per process
- TCP bootstrap for exchanging RDMA connection information
- Ring connection setup
- QP transitions: INIT -> RTR -> RTS
- Eager send/receive
- Rendezvous control messages
- Rendezvous transfer using RDMA Write with Immediate
- Local reduction operations:
  - SUM
  - MAX
  - MIN
  - int / float / double

### TODO

- Register the AllReduce data buffers / staging buffer
- Process rank and number of processes
- Reduce Scatter
- All Gather
- All Reduce
- Pipelining
- Zero-copy All Gather for large messages
- Resource cleanup
- Testing with 2 and 4 processes


## Main Data Structures

### `rdma_peer`

Contains information about another process required for establishing
an RDMA connection:

- LID
- QP number
- PSN

It also contains fields that can be used for RDMA operations:

- remote address
- remote key


### `process_group`

The process group is used as the `pg_handle` of the exercise API.

It contains the resources required by a process to communicate with
its neighbors:

- RDMA context
- Protection Domain
- Completion Queue
- QP towards NEXT
- QP towards PREV
- receive/staging buffers
- registered memory regions
- Rendezvous control buffers
- information about the neighboring processes


## Connection Setup

### `init_rdma_resources()`

Initializes the local RDMA resources.

It:

1. Finds an RDMA device.
2. Opens the device.
3. Allocates a Protection Domain.
4. Creates a Completion Queue.
5. Allocates and registers the Rendezvous control buffers.
6. Creates two RC Queue Pairs:
   - one for NEXT
   - one for PREV


### `tcp_create_listener()`

Creates the TCP listening socket.

The previous process in the ring connects to this socket during
the bootstrap phase.


### `tcp_client_exchange()`

Connects to the NEXT process over TCP.

It exchanges the RDMA connection information required to connect
the QPs.


### `tcp_accept_exchange()`

Accepts the connection from the PREVIOUS process and exchanges
RDMA connection information with it.


### `connect_one_qp()`

Moves one RC Queue Pair through:

    RESET -> INIT -> RTR -> RTS

using the connection information received during the TCP bootstrap.


### `connect_qps()`

Creates the ring connections.

Each process:

1. Opens its TCP listener.
2. Connects to NEXT.
3. Exchanges QP information with NEXT.
4. Accepts a connection from PREV.
5. Exchanges QP information with PREV.
6. Connects both RDMA QPs.


### `connect_process_group()`

Public API function that creates the process group.

It initializes the RDMA resources, connects the QPs, and returns
the resulting `process_group` through `pg_handle`.


## Completion / Receive Helpers

### `post_receive()`

Posts an RDMA receive Work Request on a given QP and buffer.


### `wait_for_completion()`

Polls the Completion Queue until a successful Work Completion is
available.


## Eager Protocol

Eager is intended for small messages.

The receiver provides a receive buffer in advance and the sender
transfers the message using an RDMA SEND operation.

### `send_eager()`

Posts an `IBV_WR_SEND` operation to `next_qp`.

Conceptually:

    CURRENT ---- SEND ----> NEXT


### `receive_eager()`

Posts a receive on `prev_qp` and waits for its completion.

Conceptually:

    PREV ---- SEND ----> CURRENT


## Rendezvous Protocol

Rendezvous is intended for larger messages.

Instead of transferring the large message using SEND, the processes
first exchange small control messages and then transfer the actual
data using RDMA Write.

Protocol:

    Sender                         Receiver

      | -------- REQUEST --------> |
      |                            |
      | <--- READY(addr, rkey) --- |
      |                            |
      | --- RDMA WRITE + IMM ----> |


### `control_message`

The control message contains:

- message type
- message size
- remote address
- remote key

The currently defined message types are:

- `RENDEZVOUS_REQUEST`
- `RENDEZVOUS_READY`
- `RENDEZVOUS_FIN`


### `send_control_message()`

Sends a small Rendezvous control message using `IBV_WR_SEND`.


### `wait_for_control_message()`

Posts a receive for a Rendezvous control message and waits until
the expected message arrives.


### `send_rendezvous()`

Sender side of the Rendezvous protocol.

It:

1. Sends `RENDEZVOUS_REQUEST` to NEXT.
2. Waits for `RENDEZVOUS_READY`.
3. Reads the destination address and rkey from the READY message.
4. Transfers the actual data using `IBV_WR_RDMA_WRITE_WITH_IMM`.


### `receive_rendezvous()`

Receiver side of the Rendezvous protocol.

It:

1. Waits for `RENDEZVOUS_REQUEST` from PREV.
2. Reads the incoming message size.
3. Posts a receive WQE for the Write-with-Immediate notification.
4. Sends `RENDEZVOUS_READY` containing the destination address
   and rkey.
5. Waits for the RDMA Write with Immediate to complete.


## Reduction

### `datatype_size()`

Returns the size of the requested datatype:

- `TYPE_INT`
- `TYPE_FLOAT`
- `TYPE_DOUBLE`


### `reduce()`

Performs the local reduction between two buffers.

Supported operations:

- `OP_SUM`
- `OP_MAX`
- `OP_MIN`

Supported datatypes:

- int
- float
- double

The result is written directly into the destination buffer.

Example:

    dst = [1, 5, 3]
    src = [4, 2, 7]
    op  = SUM

Result:

    dst = [5, 7, 10]


## Planned Ring AllReduce

The final algorithm will use:

    AllReduce = Reduce Scatter + All Gather


### Reduce Scatter

The input buffer will be divided into chunks according to the number
of processes.

During each ring step:

1. Send a chunk to NEXT.
2. Receive a chunk from PREV.
3. Reduce the received data into the corresponding local chunk.

After `P - 1` steps, each process owns one fully reduced chunk.

A staging buffer will be used for received data before applying
`reduce()`.


### All Gather

After Reduce Scatter, each process owns one part of the final result.

The All Gather phase circulates these reduced chunks around the ring
until every process has all chunks.

After `P - 1` steps, every process contains the complete reduced
buffer.


### Zero-Copy

For large messages during All Gather, the plan is to use Rendezvous
and RDMA Write directly into the correct location in the final
receive buffer.

This avoids copying the received data through an intermediate buffer.


### Pipelining

The Reduce Scatter phase will be divided into smaller segments so
that communication and reduction can overlap.

This will be implemented after the basic Ring AllReduce is working
correctly.


## Public API

The required API is:

    int connect_process_group(char *servername, void **pg_handle);

    int pg_all_reduce(void *sendbuf,
                      void *recvbuf,
                      int count,
                      DATATYPE datatype,
                      OPERATION op,
                      void *pg_handle);

    int pg_close(void *pg_handle);


## Next Step

The next implementation step is `reduce_scatter()`.

The communication infrastructure, Eager/Rendezvous protocols, and
local reduction operation are now in place, so Reduce Scatter will
connect these components into the first phase of the Ring AllReduce
algorithm.
