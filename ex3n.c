/*
TODO:
- connect_process_group():
    - creating a ring
    - creating RDMA resources/QPs
    - creating pg_handle
- Reduce Scatter:
    - Moving the chunk around the Ring + reduction
- All Gather:
    - Spreading the chunks around the Ring
- pg_all_reduce():
    - Reduce Scatter + All Gather
- Communication protocol
    - Eager
    - Rendezvous
        - large messages with RDMA Read/Write
- Optimization
    - Zero-copy in All Gather for large messages
    - Pipelining of communication + computation
- pg_close()
    - clean up/destroy QPs
*/

#include <infiniband/verbs.h>

#include <cstudio>
#include <cstdlib>
#include <cstring>
#include <cstdint>

// Types Required by the API

// Indication for send_buf type
enum DATATYPE
{
    TYPE_INT,
    TYPE_FLOAT,
    TYPE_DOUBLE
};

// which reduction action to preform
enum OPERATION
{
    OP_SUM,
    OP_MAX,
    OP_MIN
};

//
enum control_type {
    RENDEZVOUS_REQUEST,
    RENDEZVOUS_READY,
    RENDEZVOUS_FIN
};

//
struct control_message {
    int type;
    size_t size;

    uint64_t addr;
    uint32_t rkey;
};

/*
Information about another process in the ring
*/
struct rdma_peer
{
    // these are for the connection creation
    uint16_t lid; // local identifier
    uint32_t qpn; // queue pair number
    uint32_t psn; // packet sequence number

    // these are for Rendezvous/RDMA read, write
    uint64_t remote_addr; // Remote address of the other process
    uint32_t rkey;        // remote key for mr
}

/*
Process Group- pg_handle.
it contains what we need to communicate with other processes.
*/
struct process_group
{
    int pid;           // identifier within the group.
    int num_processes; // number of processes in the ring

    int next_pid; // next process in the ring
    int prev_pid; // prev process in the ring

    /* RDMA Resources */
    struct ibv_context *context; // context of RDMA device we opened
    struct ibv_pd *pd;           // protection domain
    struct ibv_cq *cq;           // completion queue

    struct ibv_qp *next_qp; // queue pair of the next process
    struct ibv_qp *prev_qp; // queue pair of the previous process

    /* Buffers */
    void *recv_buffer;    // buffer we want to save data inside it
    void *staging_buffer; // temp buffer to use in Reduce Scatter

    struct ibv_mr *recv_mr;    // memory registrations
    struct ibv_mr *staging_mr; // memory registrations

    // Info about near processes
    struct rdma_peer next;
    struct rdma_peer prev;
};

/* Helper Functions */
static size_t datatype_size(DATATYPE datatype)
{ // Returns the datatype size
    switch (datatype)
    {
    case TYPE_INT:
        return sizeof(int);
    case TYPE_FLOAT:
        return sizeof(float);
    case TYPE_DOUBLE:
        return sizeof(double);
    default:
        return 0;
    }
}

// Reduction
static void reduce(void *dst, const void *src, int count, DATATYPE datatype, OPERATION op)
{
    /*
    TODO:
    Implement SUM/MAX/MIN

    Example:
    if (datatype == TYPE_INT && op == OP_SUM) {
        int *d = (int *)dst;
        const int *s = (const int *)src;

        for (int i = 0; i < count; ++i)
            d[i] += s[i];
    }
    */
}

/* RDMA Initialization */
static int init_rdma_resources(struct process_group *pg)
{
    /*
    TODO:
    1. Find RDMA device
    2. Ibv_open_device()
    3. ibv_alloc_pd()
    4. ibv_create_cq()
    5. allocate buffers
    6. ibv_reg_mr()
    7. create QP(s)
    */

    return 0;
}

/* Connect QPs */
static int connect_qps(struct proccess_group *pg)
{
    /*
    TODO:
    Exchange information between processes:
    LID, QPN, PSN, Remote_addr, rkey.
    Then move QPs: Reset->Init->RTR->RTS
    */
    return 0;
}

/* API- connect all processes into a ring */
int connect_process_group(char *servername, void **pg_handle)
{
    struct process_group *pg = (struct process_group *)calloc(1, sizeof(struct process_group));
    if (!pg)
        return -1;

    /*TODO:
    Determine:
        pg->rank
        pg->num_processes
    And therefore:
        next_rank=(rank+1)%P
        prev_rank=(rank-1+P)%P
    */
    pg->next_pid = (pg->pid + 1) % pg->num_processes;
    pg->prev_pid = (pg->pid - 1 + pg->num_processes) % pg->num_processes;

    if (init_rdma_resources(pg))
    {
        free(pg);
        return -1;
    }

    if (connect_qps(pg))
    {
        free(pg);
        return -1;
    }

    *pg_handle = pg;
    return 0;
}

/* Eager */
static int send_eager(struct process_group *pg, void *buffer, size_t size, struct ibv_mr *mr)
{
    /* Describe the local buffer */
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)buffer;
    sge.length = size;
    sge.lkey = mr->lkey;

    /* Create SEND Work Request */
    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = 1;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.next = NULL;

    struct ibv_send_wr *bad_wr = NULL;

    /* send to the next process in the ring */
    if(ibv_post_send(pg->next_qp, &wr, &bad_wr)){
        fprintf(stderr, "Failed to post Eager SEND\n");
        return -1;
    }
    return 0;
}

/* Rendezvous */
static int send_rendezvous(struct process_group *pg, void *buffer, size_t size, struct ibv_mr *mr)
{
    /* handshake */
    struct control_message *ctrl = (struct control_message *)pg->control_buffer;
    /* Send Egaer control message */
    ctrl->type = RENDEZVOUS_REQUEST;
    ctrl->size = size;

    if (send_eager(pg,
                   ctrl,
                   sizeof(struct control_message),
                   pg->control_mr)) {
        return -1;
    }

    /* wait for Ready message from the receiver */
    if (wait_for_control_message(pg, RENDEZVOUS_READY)) {
        return -1;
    }

    uint64_t remote_addr = ctrl->addr;
    uint32_t remote_rkey = ctrl->rkey;

    /* Describe the local buffer */
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)buffer;
    sge.length = size;
    sge.lkey = mr->lkey;

    /* Create SEND Work Request */
    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = 1;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;

    /* Remote memory information received during Rendezvous handshake */
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey        = remote_rkey;

    struct ibv_send_wr *bad_wr = NULL;

    /* RDMA Write to the next process in the ring */
    if (ibv_post_send(pg->next_qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post Rendezvous RDMA Write\n");
        return -1;
    }
    return 0;
}

/* Send Chunk- decide between Eager and Rendezvous */
// static int send_chunk(struct process_group *pg, void *buffer, size_t size, uint64_t remote_addr, uint32_t rkey)
// {
//     const size_t EAGER_THRESHOLD = 8192; // TODO: change in the end- after benchmark.
//     if (size <= EAGER_THRESHOLD)
//     {
//         return send_eager(pg, buffer, size);
//     }
//     else
//     {
//         return send_rendezvous(pg, buffer, size, remote_addr, rkey)
//     }
// }

/* Reduce Scatter */
static int reduce_scatter(struct process_group *pg, void *buffer, int count, DATATYPE datatype, OPERATION op)
{
    int P = pg->num_processes;
    int pid = pg->pid;

    size_t elem_size = datatype_size(datatype);

    /* Simplification:
    Assume count is divisible by P.
    */

    int chunk_count = count / P;
    size_t chunk_bytes = chunk_count * elem_size;

    /* Ring Reduce-Scatter requires P-1 Steps */
    for (int step = 0; step < P - 1; ++step)
    {
        /* Determine which chunk I send and which chunk I receive */
        int send_chunk_index = (pid - step - 1 + P) % P;
        int recv_chunk_index = (pid - step - 2 + P) % P;

        /* ptrs to the send and recv chunks */
        char *send_ptr = (char *)buffer + send_chunk_index * chunk_bytes;
        char *recv_ptr = (char *)buffer + recv_chunk_index * chunk_bytes;

        /*
        TODO:
        Pipeline: Split this chunk into smaller segments
        */
        size_t PIPELINE_SIZE = 64 * 1024;
        for (size_t offset = 0; offset < chunk_bytes; offset += PIPELINE_SIZE)
        {
            size_t bytes = (offset + PIPELINE_SIZE <= chunk_bytes) ? PIPELINE_SIZE : chunk_bytes - offset;
            /*
            1. Send our segment to the NEXT process
            */

            /*
            TODO:
            send_chunk(pg, send_ptr + offset, butes, remote_addr, rkey);
            */

            /*
            2. Wait for a segment from PREVIOUS process.
            */
            // TODO: Pool CQ

            /*
            3. Reduce received segment into our local chunk.
            */

            /*
            reduce(recv_ptr + offset, pg->staging_buffer, bytes / elem_size, datatype, op);
            */
        }
    }
    return 0;
}

/* All Gather */
static int all_gather(struct process_group *pg, void *recv_buffer, int count, DATATYPE datatype)
{
    int P = pg->num_processes;
    int pid = pg->pid;

    size_t elem_size = datatype_size(datatype);

    int chunk_count = count / P;
    size_t chunk_bytes = chunk_count * elem_size;

    /*
    Again, Ring requires P-1 steps
    */
    for (int step = 0; step < P - 1; ++step)
    {
        int send_chunk_index = (pid - step + P) % P;
        int recv_chunk_index = (pid - step - 1 + P) % P;

        char *send_ptr = (char *)recv_buffer + send_chunk_index * chunk_bytes;
        char *recv_ptr = (char *)recv_buffer + recv_chunk_index * chunk_bytes;

        /*
        TODO:
        Small Messages: Eager
        Large Messages: Rendezvous
        For large messages: RDMA Write directly into recv_ptr
        this gives us the required zero-copy All-gather.
        */

        // send_chunk(...);
    }
    return 0;
}

/* All Reduce */
int pg_all_reduce(void *send_buf, void *recv_buf, int count, DATATYPE datatype, OPERATION op, void *pg_handle)
{
    struct process_group *pg = (struct process_group *)pg_handle;

    if (!pg || !send_buf | !recv_buf)
        return -1;

    size_t bytes = count * datatype_size(datatype);

    /* initially: recvbuf = out own contribution */
    memcpy(recv_buf, send_buf, bytes);

    /* Phase 1: Reduce Scatter */
    if (reduce_scatter(pg, recv_buf, count, datatype, op))
    {
        return -1;
    }

    /* Phase 2: All Gather */
    if (all_gather(pg, recv_buf, count, datatype))
    {
        return -1;
    }
    return 0;
}

/* Cleanup */
int pg_close(void *pg_handle)
{
    struct process_group *pg = (struct process_group *)pg_handle;

    if (!pg)
        return 0;

    /*
    TODO:
        ibv_destroy_qp()
        ibv_destroy_cq()
        ibv_dereg_mr()
        ibv_dealloc_mr()
        ibv_dealloc_pd()
        ibv_close_device()

        free buffers
    */

    free(pg);
    return 0;
}