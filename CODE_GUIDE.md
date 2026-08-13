# Complete Guide to `Ex3.c`

This document explains the communication concepts, the program structure, how
to build and run the exercise, and every part of `Ex3.c`. Adjacent source lines
that perform one operation are grouped together so that the walkthrough remains
readable. The ranges cover the complete file from line 1 through line 1642.

## 1. What the program implements

The program creates a process group arranged as a logical ring:

```text
rank 0 -> rank 1 -> rank 2 -> rank 3 -> rank 0
```

Every process has:

- An outgoing Reliable Connection QP, called `tx_qp`, connected to the next
  process.
- An incoming Reliable Connection QP, called `rx_qp`, connected to the
  previous process.
- A TCP connection to each neighbor for setup, barriers, and exchanging memory
  registration information.
- A registered scratch buffer used by the rendezvous reduce-scatter phase.

The program implements:

1. **Reduce-scatter:** reduce all input arrays while distributing one reduced
   chunk to each process.
2. **All-gather:** circulate those reduced chunks until every process has the
   complete array.
3. **All-reduce:** run reduce-scatter followed by all-gather.

The implementation is single-threaded. Progress is made by polling the Verbs
completion queue.

## 2. Important RDMA concepts

### 2.1 RDMA

Remote Direct Memory Access allows one machine's network adapter to transfer
data directly to or from registered memory on another machine. The CPU does not
need to copy every packet through the operating-system networking stack.

### 2.2 Verbs API

The Verbs API is the low-level interface used to control an RDMA device. This
program uses objects from `libibverbs`:

| Object | Meaning |
| --- | --- |
| `ibv_context` | Open RDMA device |
| `ibv_pd` | Protection domain that groups trusted resources |
| `ibv_mr` | Registered memory region with local and remote keys |
| `ibv_cq` | Completion queue |
| `ibv_qp` | Queue pair containing send and receive work queues |
| `ibv_sge` | Address, length, and key for one memory segment |
| `ibv_send_wr` | Send-side work request |
| `ibv_recv_wr` | Receive-side work request |
| `ibv_wc` | Work completion returned by the completion queue |

### 2.3 Memory registration

RDMA hardware cannot access arbitrary virtual memory. A buffer must first be
registered with `ibv_reg_mr()`.

Registration produces:

- `lkey`: authorizes the local QP to access the buffer.
- `rkey`: authorizes a remote QP to access the buffer.
- Virtual address: tells the remote side where the registered buffer begins.

For a remote RDMA write, the sender needs the destination's address and `rkey`.

### 2.4 Queue pairs

A Queue Pair has a send queue and a receive queue. An RC QP must pass through
these states:

```text
RESET -> INIT -> RTR -> RTS
```

- `INIT`: local port and access permissions are configured.
- `RTR`, Ready to Receive: the remote QP number, path, and remote packet
  sequence number are known.
- `RTS`, Ready to Send: retry settings and the local packet sequence number are
  configured.

One RC QP can connect to only one remote QP. Therefore a ring with more than two
processes needs two local QPs:

```text
previous rank's tx_qp -> this rank's rx_qp
this rank's tx_qp     -> next rank's rx_qp
```

### 2.5 Completion queue

Posting a work request is asynchronous. Completion is detected by polling the
CQ with `ibv_poll_cq()`.

For every transferred block, this program expects:

- One send completion from `tx_qp`.
- One receive completion from `rx_qp`.

The send and receive completions can arrive in either order.

### 2.6 Immediate data

`SEND_WITH_IMM` and `RDMA_WRITE_WITH_IMM` include a 32-bit immediate value.
The receiver gets this value in its completion. The program encodes:

```text
phase | ring step | pipeline block
```

This verifies that a completion belongs to the expected collective operation.

## 3. Eager and rendezvous protocols

### 3.1 Eager

The eager path uses:

```c
IBV_WR_SEND_WITH_IMM
```

The receiver posts a receive work request containing a destination SGE. The
incoming data is copied into that receive buffer.

Advantages:

- Simple protocol.
- Good for small messages.
- The sender does not need the remote address or `rkey`.

Disadvantages:

- The receiver must prepare an appropriate receive buffer.
- It generally involves more protocol handling and copying than a direct
  one-sided operation.

### 3.2 Rendezvous

The rendezvous path uses:

```c
IBV_WR_RDMA_WRITE_WITH_IMM
```

Before sending, the sender learns the receiver's registered address and
`rkey`. The network adapter then writes directly into that remote memory.

Advantages:

- Suitable for large messages.
- The all-gather phase writes directly into the final output location.
- Avoids an additional CPU copy during all-gather.

Disadvantages:

- Requires memory registration information exchange.
- Setup cost is less attractive for small messages.

### 3.3 Automatic selection

With `--protocol auto`, the complete collective size is compared with
`PG_EAGER_THRESHOLD`.

```text
message size <= threshold -> eager
message size > threshold  -> rendezvous
```

The protocol can also be forced with `--protocol eager` or
`--protocol rendezvous`.

## 4. Ring collective algorithms

Assume four ranks and an input divided into four chunks:

```text
[chunk 0][chunk 1][chunk 2][chunk 3]
```

### 4.1 Reduce-scatter

Each rank starts with a complete input array. During each of `P - 1` steps:

1. Send one current partial chunk to the next rank.
2. Receive one partial chunk from the previous rank.
3. Apply the selected operation to the received data and the local partial
   chunk.

The chunk indexes are:

```c
send_chunk = (rank - step - 1 + size) % size;
receive_chunk = (rank - step - 2 + 2 * size) % size;
```

After `P - 1` steps, each rank owns its conventional reduce-scatter chunk:

```c
r
```

### 4.2 All-gather

Each rank begins with one owned reduced chunk. During every step:

1. Send the chunk currently moving clockwise.
2. Receive the next missing chunk from the previous rank.
3. Place it in its final global offset.

After `P - 1` steps, every rank has every reduced chunk.

### 4.3 All-reduce

All-reduce is the composition:

```text
all-reduce = reduce-scatter + all-gather
```

This ring algorithm sends approximately `2 * (P - 1) / P` times the complete
message size per process, which is efficient for large arrays.

### 4.4 Uneven counts

`pg_all_reduce()` supports a count that is not divisible by the number of
ranks. The first `count % size` chunks receive one extra element.

For example, 10 elements across 4 ranks become:

```text
chunk sizes: 3, 3, 2, 2
offsets:     0, 3, 6, 8
```

`chunk_count()` and `chunk_offset()` calculate this layout.

## 5. Pipelining

Large chunks are split into blocks controlled by `PG_PIPELINE_BYTES`.

For every reduce-scatter block:

1. Post the receive operation.
2. Post the outgoing transfer.
3. Wait until the incoming block arrives.
4. Reduce the incoming block on the CPU.
5. If the outgoing transfer has not completed, wait for it.

Step 4 may execute while the outgoing network transfer is still active. This
overlaps communication and computation without using another thread.

## 6. TCP bootstrap and control ring

RDMA QPs cannot connect without exchanging metadata. TCP is used only as an
out-of-band control channel.

Every process listens on:

```text
PG_BASE_PORT + rank
```

It connects to the next rank and accepts a connection from the previous rank.
The processes exchange:

- QP number.
- Packet sequence number.
- LID.
- GID.
- Active MTU.
- Scratch-buffer virtual address.
- Scratch-buffer `rkey`.

Rank zero changes the handshake order to break a circular wait:

- Rank zero accepts the previous rank first.
- Other ranks complete their outgoing handshake first.

The TCP connections remain open for barriers and all-gather MR exchange.

## 7. Build requirements

The program must be built and run on Linux with RDMA hardware or a configured
software RDMA environment.

Install the dependencies:

```sh
sudo apt update
sudo apt install build-essential libibverbs-dev
```

Build:

```sh
cd CNEx3
make
```

Equivalent direct compilation:

```sh
gcc -O2 -g -std=gnu11 -Wall -Wextra -Wpedantic \
    Ex3.c -o ex3 -libverbs -lm
```

The result includes the exercise-named executable and the original name:

```text
./test
./ex3
```

## 8. Before running

On every machine:

1. Confirm that the same source and executable are installed.
2. Confirm that the RDMA interface is active.
3. Confirm that hostnames resolve between machines.
4. Allow the TCP bootstrap ports through the firewall.
5. Use the same host order and collective arguments on every process.

Useful Linux commands include:

```sh
ibv_devices
ibv_devinfo
```

For RoCE, determine the correct GID index and export it:

```sh
export PG_GID_INDEX=3
```

The correct index depends on the machine's RDMA configuration.

## 9. Running with two processes

Assume the machines are named `rdma0` and `rdma1`.

On `rdma0`:

```sh
./test -myindex 01 -list rdma0 rdma1 \
    --count 1048576 --datatype int --op sum \
    --protocol eager --iterations 20
```

On `rdma1`:

```sh
./test -myindex 02 -list rdma0 rdma1 \
    --count 1048576 --datatype int --op sum \
    --protocol eager --iterations 20
```

To measure rendezvous, repeat the experiment with:

```sh
--protocol rendezvous
```

## 10. Running with four processes

Use the identical ordered host list on all four machines.

On `rdma0`:

```sh
./test -myindex 01 -list rdma0 rdma1 rdma2 rdma3 --protocol rendezvous
```

On `rdma1`:

```sh
./test -myindex 02 -list rdma0 rdma1 rdma2 rdma3 --protocol rendezvous
```

On `rdma2`:

```sh
./test -myindex 03 -list rdma0 rdma1 rdma2 rdma3 --protocol rendezvous
```

On `rdma3`:

```sh
./test -myindex 04 -list rdma0 rdma1 rdma2 rdma3 --protocol rendezvous
```

The program uses a default count of 1,048,576 integers and 20 timed iterations
when those options are omitted.

## 11. Command-line options

| Option | Meaning |
| --- | --- |
| `-myindex N` | This process's one-based exercise index |
| `-list HOST...` | Ordered space-separated host list |
| `--rank N` | Alternative zero-based process index |
| `--hosts LIST` | Alternative comma-separated host list |
| `--count N` | Number of all-reduce elements |
| `--iterations N` | Number of timed all-reduce calls |
| `--datatype int` | Use C `int` elements |
| `--datatype float` | Use C `float` elements |
| `--datatype double` | Use C `double` elements |
| `--op sum` | Addition reduction |
| `--op prod` | Multiplication reduction |
| `--op max` | Maximum reduction |
| `--op min` | Minimum reduction |
| `--protocol eager` | Force SEND-based transfers |
| `--protocol rendezvous` | Force RDMA Write transfers |
| `--protocol auto` | Choose using the eager threshold |

## 12. Environment variables

| Variable | Default | Explanation |
| --- | ---: | --- |
| `PG_BASE_PORT` | `18515` | Rank `r` listens on this value plus `r` |
| `PG_DEVICE` | First device | Name shown by `ibv_devices` |
| `PG_IB_PORT` | `1` | Physical RDMA device port |
| `PG_GID_INDEX` | `-1` | Negative means LID addressing; set for RoCE |
| `PG_PROTOCOL` | `auto` | Protocol selected by the CLI |
| `PG_EAGER_THRESHOLD` | `8192` | Auto-mode threshold in bytes |
| `PG_PIPELINE_BYTES` | `65536` | Maximum transfer block size |

The pipeline size is aligned down to an eight-byte boundary so that `double`
elements are never divided between two blocks.

## 13. Expected output

Only rank zero prints the benchmark result:

```text
ranks=4 count=1048576 bytes=4194304 protocol=rendezvous iterations=20 average=850.123 us
```

The exact latency depends on the hardware, PCIe topology, network, MTU, message
size, and memory-registration cost.

Before printing, every rank verifies that the result is correct. The test input
at rank `r` contains `r + 1` in every element.

For four ranks and `sum`:

```text
1 + 2 + 3 + 4 = 10
```

Therefore every output element must be 10.

## 14. Complete line-by-line map of `Ex3.c`

### Lines 1-20: language mode and headers

- **Line 1:** Enables GNU/POSIX declarations such as `setenv()`,
  `strdup()`, and `MSG_NOSIGNAL`.
- **Lines 2-20:** Include networking, Verbs, numeric, memory, timing, and
  process APIs. Each later structure and function comes from one of these
  headers.

### Lines 21-32: constants

- **Line 21:** Default TCP bootstrap base port.
- **Line 22:** Default physical RDMA port.
- **Line 23:** Default GID index. `-1` selects LID-based InfiniBand addressing.
- **Line 24:** Default boundary between eager and rendezvous.
- **Line 25:** Default pipeline block size, 64 KiB.
- **Line 26:** Number of TCP connection attempts.
- **Line 27:** Maximum outstanding send and receive work requests per QP.
- **Line 28:** Number of CQ entries.
- **Line 30:** Reserves the high work-request-ID bit for send completions.
- **Lines 31-32:** Numeric identifiers for the two collective phases.

### Lines 33-46: public datatype and operation enums

- **Lines 34-38:** Define the three supported element types.
- **Lines 40-46:** Define sum, product, maximum, and minimum operations.

### Lines 47-54: public API declarations

- **Line 47:** Declares process-group creation.
- **Lines 48-49:** Declare reduce-scatter.
- **Lines 50-51:** Declare all-gather.
- **Lines 52-53:** Declare all-reduce.
- **Line 54:** Declares resource destruction.

### Lines 55-61: protocol mode

- Defines automatic, forced eager, and forced rendezvous modes.

### Lines 62-71: host-format QP endpoint

- Stores the QP number, PSN, LID, MTU, GID, scratch address, and scratch
  `rkey` in normal host byte order.

### Lines 72-81: network-format QP endpoint

- Defines the packed representation transmitted through TCP.
- Packing prevents compiler-added bytes from changing the wire layout.

### Lines 82-86: memory-region wire descriptor

- Contains the registered virtual address and `rkey` needed for an RDMA write.

### Lines 87-116: process-group handle

- **Lines 88-95:** Store rank, group size, hosts, addressing configuration,
  protocol threshold, and pipeline size.
- **Lines 97-99:** Store the TCP listener and neighbor sockets.
- **Lines 101-107:** Store the opened device, PD, CQ, two QPs, and PSNs.
- **Lines 108-109:** Store local port and GID information.
- **Lines 111-112:** Store the local rendezvous scratch buffer and MR.
- **Line 113:** Stores the next rank's incoming QP and scratch information.
- **Line 114:** Ends the handle. All public APIs receive it as `void *`.

### Lines 117-130: 64-bit byte-order conversion

- **Lines 117-125:** Convert a host 64-bit integer to big-endian format.
- **Lines 126-130:** Convert big-endian back to host format. Byte swapping is
  symmetric, so the first helper can be reused.

### Lines 131-142: error helpers

- **Lines 131-136:** Print a program error and return `-1`.
- **Lines 137-142:** Add the current `errno` explanation to a failed operation.

### Lines 143-161: environment-number parser

- Reads an environment variable.
- Returns the default when it is absent.
- Uses `strtol()` for checked conversion.
- Rejects trailing characters, overflow, and values outside the requested
  range.

### Lines 162-176: protocol parser

- Reads `PG_PROTOCOL`.
- Converts `auto`, `eager`, or `rendezvous` to the internal enum.
- Reports an unknown value and safely falls back to automatic selection.

### Lines 177-189: datatype size

- Maps each public datatype to `sizeof(int)`, `sizeof(float)`, or
  `sizeof(double)`.
- Returns zero for an invalid enum.

### Lines 190-247: host-list parser

- Counts commas to estimate the number of hosts.
- Duplicates the input because `strtok_r()` modifies its string.
- Allocates one string for each hostname.
- Requires at least two processes.
- Frees all partially allocated data on every failure path.

### Lines 248-257: host-list cleanup

- Frees every hostname and then the hostname array.
- Clears the pointer to prevent accidental reuse.

### Lines 258-276: complete TCP send

- Repeatedly calls `send()` until every byte is transmitted.
- Retries interrupted system calls.
- Uses `MSG_NOSIGNAL` to avoid process termination on a closed socket.
- Treats a zero-byte write as a closed connection.

### Lines 277-295: complete TCP receive

- Repeatedly calls `recv()` until the requested fixed-size message is complete.
- Handles interruption and early connection closure.

### Lines 296-302: disable Nagle's algorithm

- Enables `TCP_NODELAY` because the control messages are small and latency is
  more important than packet aggregation.

### Lines 303-340: create TCP listener

- Converts the numeric port to text for `getaddrinfo()`.
- Requests an IPv4 passive TCP address, matching the exercise hosts.
- Tries all returned addresses.
- Enables address reuse.
- Binds and starts listening.
- Returns the first successful listener.

### Lines 341-385: connect to the next rank

- Resolves the next hostname and port over IPv4.
- Tries every returned address.
- Retries for approximately 30 seconds because processes may start at slightly
  different times.
- Enables `TCP_NODELAY` after a connection succeeds.

### Lines 386-397: accept the previous rank

- Calls `accept()` and retries an interrupted call.
- Enables `TCP_NODELAY` on the accepted socket.

### Lines 398-409: encode endpoint

- Converts multibyte fields to network byte order.
- Copies the 16 raw GID bytes unchanged.

### Lines 410-421: decode endpoint

- Reverses the endpoint encoding after TCP reception.

### Lines 422-428: send endpoint

- Encodes a local endpoint and transmits the complete packed structure.

### Lines 429-437: receive endpoint

- Receives the packed structure and converts it to host order.

### Lines 438-453: QP RESET-to-INIT transition

- Selects the physical port and P_Key index.
- Allows remote reads and writes.
- Calls `ibv_modify_qp()` with the required INIT attribute mask.

### Lines 454-489: QP INIT-to-RTR transition

- Uses the smaller local/remote MTU.
- Installs the remote QP number and PSN.
- Configures receive-side atomic and RNR settings.
- Creates a LID path for InfiniBand.
- Adds a global routing header and remote GID when a GID index is configured.
- Moves the QP to Ready to Receive.

### Lines 490-508: QP RTR-to-RTS transition

- Configures timeout, retry, RNR retry, local PSN, and atomic limits.
- Uses retry count 7 and RNR retry 7, which provide robust retry behavior.
- Moves the QP to Ready to Send.

### Lines 509-523: build local endpoint record

- Reads the QP number from the created QP.
- Adds local PSN, LID, MTU, GID, scratch address, and scratch `rkey`.
- Returns the complete bootstrap record by value.

### Lines 524-538: outgoing QP handshake

- Sends this rank's `tx_qp` information to the next rank.
- Receives the next rank's `rx_qp` information.
- Connects `tx_qp` to that remote `rx_qp`.
- Saves the next rank's scratch address and `rkey`.

### Lines 539-552: incoming QP handshake

- Receives the previous rank's `tx_qp` information.
- Connects this rank's `rx_qp` to it.
- Sends this rank's `rx_qp` information back to the previous rank.

### Lines 553-633: create Verbs resources

- Gets the list of RDMA devices.
- Selects `PG_DEVICE` or the first available device.
- Opens the device context.
- Queries and checks the selected port.
- Queries a GID when requested.
- Allocates a protection domain.
- Creates one completion queue.
- Creates `tx_qp` and `rx_qp`, both using that CQ.
- Moves both QPs to INIT.
- Allocates a page-aligned scratch buffer.
- Registers the scratch memory for local and remote writes.

### Lines 634-641: generate packet sequence number

- Mixes monotonic time, process ID, rank, and a salt.
- Keeps the lower 24 bits because RC PSNs are 24-bit values.

### Lines 642-732: `connect_process_group`

- Validates the output pointer and allocates `pg_handle_t`.
- Initializes socket descriptors to `-1` for safe cleanup.
- Reads all environment configuration.
- Aligns pipeline blocks to eight bytes.
- Parses the host list and rank.
- Generates different PSNs for the two QPs.
- Creates the Verbs resources.
- Creates the rank-specific TCP listener.
- Connects to the next rank.
- Uses rank zero's special order to break the handshake cycle.
- Closes the listener after both neighbor connections exist.
- Returns the opaque handle through `*pg_handle`.
- Calls `pg_close()` after any partial failure.

### Lines 733-758: ring barrier

- Rank zero injects an `ENTER` token into the ring.
- Every other rank receives and forwards it.
- Rank zero then injects a `RELEASE` token.
- Every rank forwards the release before entering the collective.
- This ensures that all processes call collectives in the same order.

### Lines 759-782: exchange all-gather destination MR

- Publishes this rank's final output address and `rkey` to the previous rank.
- Receives the next rank's output descriptor.
- Supplies the information needed for zero-copy rendezvous all-gather writes.

### Lines 783-789: calculate chunk size

- Divides the total count evenly.
- Gives one extra element to the first remainder chunks.

### Lines 790-798: calculate chunk offset

- Calculates how many base elements and extra remainder elements precede a
  chunk.

### Lines 799-844: apply reduction operation

- The `REDUCE_TYPED` macro creates the same typed loop for all datatypes.
- Each element performs sum, product, maximum, or minimum.
- The datatype switch selects `int`, `float`, or `double`.
- The macro is undefined afterward to limit its scope.

### Lines 845-850: immediate-data tag

- Stores phase in the high byte.
- Stores ring step in the next byte.
- Stores pipeline block in the low 16 bits.

### Lines 851-868: poll one completion

- Busy-polls the CQ because the application is single-threaded and optimized
  for low latency.
- Rejects CQ polling errors.
- Uses `ibv_wc_status_str()` to report failed work requests.

### Lines 869-889: post receive work request

- Builds an SGE when eager data must be placed in local memory.
- Uses no SGE for rendezvous because the data arrives through RDMA Write.
- Stores the tag in `wr_id`.
- Posts the work request on `rx_qp`.

### Lines 890-920: post outgoing transfer

- Builds the local source SGE.
- Marks the work request as a send by setting `WR_SEND_BIT`.
- Uses `SEND_WITH_IMM` for eager and zero-byte control transfers.
- Uses `RDMA_WRITE_WITH_IMM` for non-empty rendezvous transfers.
- Requests a completion with `IBV_SEND_SIGNALED`.
- Installs remote address and `rkey` for RDMA Write.
- Posts the operation on `tx_qp`.

### Lines 921-945: wait for incoming completion

- Polls until the receive completion arrives.
- Records a send completion if it arrives first.
- Checks the work-request tag.
- Checks that immediate data exists and contains the expected network-order tag.

### Lines 946-957: wait for outgoing completion

- Polls one completion when the send did not complete earlier.
- Verifies that it is the expected send work request.

### Lines 958-966: protocol decision

- Forced eager always returns true.
- Forced rendezvous always returns false.
- Automatic mode compares total bytes with the eager threshold.

### Lines 967-1050: internal reduce-scatter phase

- Calculates element size, maximum chunk size, and number of pipeline blocks.
- Iterates through `size - 1` ring steps.
- Calculates the outgoing and incoming chunk indexes.
- Calculates each chunk's exact byte size and offset.
- Iterates over a globally consistent number of blocks.
- Uses zero-byte immediate messages when a short uneven chunk has no data in a
  later block.
- Posts receive before send to provide a receive work request.
- Eager receives into scratch through an SGE.
- Rendezvous writes directly into the registered scratch buffer.
- Waits for the incoming completion.
- Reduces the scratch block into the correct partial chunk.
- Waits for the outgoing completion only if it was not already observed.

### Lines 1051-1137: internal all-gather phase

- Calculates pipeline geometry.
- Exchanges output MR descriptors for rendezvous.
- Iterates through `size - 1` ring steps.
- Chooses the chunk to send and the missing chunk to receive.
- Eager places incoming data through a receive SGE at its final offset.
- Rendezvous writes directly to the next rank's final output offset.
- Drains both completions for every block.
- Performs no reduction because all chunks are already fully reduced.

### Lines 1138-1152: register collective output

- Ensures that even a zero-count call registers one valid byte.
- Registers the buffer for local and remote writes.
- Returns the MR or reports the registration failure.

### Lines 1153-1201: public `pg_all_reduce`

- Validates pointers, count, datatype, and multiplication overflow.
- Calculates total bytes and selects a protocol.
- Synchronizes all ranks.
- Copies `sendbuf` into `recvbuf`.
- Registers `recvbuf`.
- Runs reduce-scatter.
- Calculates the chunk owned after reduce-scatter.
- Runs all-gather.
- Deregisters the MR.
- Uses a final barrier before returning.

### Lines 1202-1259: public `pg_reduce_scatter`

- Uses standard equal per-rank receive counts.
- Calculates the total send count as `recv_count * size`.
- Allocates and registers an internal full-size working buffer.
- Copies the caller's send data into it.
- Runs the same ring reduce-scatter phase.
- Copies this rank's final reduced chunk into the caller's `recvbuf`.
- Deregisters and frees temporary resources.

### Lines 1260-1306: public `pg_all_gather`

- Uses a standard equal per-rank send count.
- Calculates the complete receive count.
- Copies the local input to this rank's final output slot.
- Registers the complete output buffer.
- Starts the all-gather phase with `owned_chunk == rank`.
- Deregisters the output MR and synchronizes before returning.

### Lines 1307-1338: `pg_close`

- Safely accepts a null handle.
- Closes listener and control sockets.
- Deregisters and frees the scratch buffer.
- Destroys both QPs.
- Destroys the CQ.
- Deallocates the protection domain.
- Closes the RDMA device.
- Frees the host list and handle.

The order is important: QPs and registered memory must be cleaned up before
their parent PD and device context.

### Lines 1339-1348: benchmark options structure

- Stores all command-line choices used by `main()`.

### Lines 1350-1363: usage text

- Prints the exercise syntax, the alternative syntax, and optional arguments.

### Lines 1364-1376: checked CLI integer parser

- Converts a decimal argument with `strtol()`.
- Rejects invalid characters, negative values when forbidden, and values larger
  than `INT32_MAX`.

### Lines 1377-1411: exercise host-list parser

- Collects the space-separated hosts following `-list`.
- Converts them into the comma-separated representation used by the API.
- Keeps the allocated list alive until the program exits.

### Lines 1412-1479: command-line parser

- Installs default count, iteration count, datatype, operation, and protocol.
- Walks through every argument.
- Converts one-based `-myindex` values to zero-based internal ranks.
- Supports both the exercise syntax and the original long-option syntax.
- Maps strings to enums.
- Requires rank and host list.
- Rejects unknown or incomplete options.

### Lines 1480-1487: count hosts

- Counts commas and adds one to determine the number of ranks for the benchmark.

### Lines 1488-1496: benchmark datatype size

- Returns the allocation size for the selected benchmark type.

### Lines 1497-1514: initialize test input

- Fills every local element with `rank + 1`.
- Uses a correctly typed pointer for each datatype.

### Lines 1515-1531: calculate expected test result

- Sum uses the arithmetic-series formula.
- Maximum is the number of ranks.
- Minimum is one.
- Product multiplies the values from 1 through the number of ranks.

### Lines 1532-1554: verify output

- Converts each result element to `double` for comparison.
- Uses a small relative tolerance for floating-point values.
- Reports the first incorrect element.

### Lines 1555-1561: elapsed time

- Converts two monotonic timestamps into a fractional number of seconds.

### Lines 1562-1642: `main`

- Parses arguments.
- Validates that rank is inside the host list.
- Allocates send and receive buffers.
- Initializes the test input.
- Publishes rank and protocol through environment variables used by the API.
- Connects the process group.
- Performs one untimed warm-up all-reduce.
- Times the requested number of all-reduce iterations.
- Reinitializes input before every iteration.
- Verifies the final output.
- Prints average latency from rank zero.
- Closes the process group and frees both buffers on every exit path.

## 15. Suggested experiment

Run both protocols with increasing message sizes:

```text
1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576 elements
```

For each size, record:

- Eager average latency with two ranks.
- Rendezvous average latency with two ranks.
- Eager average latency with four ranks.
- Rendezvous average latency with four ranks.

Expected general behavior:

- Eager is often better for very small messages because it avoids rendezvous
  metadata overhead.
- Rendezvous becomes better as the message grows because all-gather writes
  directly into the final output.
- Four ranks perform more ring steps than two ranks.
- Pipelining matters more for large messages.

The exact crossover point is a result of the experiment, not a universal
constant.

## 16. Common problems

### No RDMA device

```text
ex3: no RDMA device is available
```

Check `ibv_devices`, drivers, and whether the program is running on an
RDMA-capable machine.

### Port is not active

```text
ex3: selected RDMA port is not active
```

Check cable, switch, subnet manager for InfiniBand, or RoCE interface state.

### TCP connection failure

Check:

- Hostnames.
- `PG_BASE_PORT`.
- Firewall.
- Whether every rank was started.
- Whether another process already uses the port.

### QP transition failure

Likely causes include:

- Incorrect GID index.
- Incompatible addressing configuration.
- Inactive port.
- Incorrect RDMA device selection.

### Work completion failure

The error text from `ibv_wc_status_str()` identifies problems such as remote
access errors, retry exhaustion, or RNR retry exhaustion.

### Processes hang

All ranks must call collectives in the same order with identical counts,
datatypes, operations, and protocol settings. If one process exits early, the
remaining ranks can wait indefinitely for its barrier or completion.
