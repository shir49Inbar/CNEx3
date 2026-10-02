#include <infiniband/verbs.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>

#define TX_DEPTH 128
#define RX_DEPTH 128

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
};

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

    /* Control message for Rendezvous */
    struct control_message *control_send_buffer;
    struct control_message *control_recv_buffer;

    struct ibv_mr *control_send_mr;    // memory registrations
    struct ibv_mr *control_recv_mr; // memory registrations

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

/*RDMA Helper functions*/
static int post_receive(struct process_group *pg,
                        struct ibv_qp *qp,
                        void *buffer,
                        size_t size,
                        struct ibv_mr *mr,
                        uint64_t wr_id)
{
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));

    sge.addr = (uintptr_t)buffer;
    sge.length = size;
    sge.lkey = mr->lkey;

    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    struct ibv_recv_wr *bad_wr = NULL;

    return ibv_post_recv(qp, &wr, &bad_wr);
}

static int wait_for_completion(struct process_group *pg,
                               struct ibv_wc *wc)
{
    while (1) {
        int n = ibv_poll_cq(pg->cq, 1, wc);

        if (n < 0)
            return -1;

        if (n == 0)
            continue;

        if (wc->status != IBV_WC_SUCCESS)
            return -1;

        return 0;
    }
}

/* RDMA Initialization */
static int init_rdma_resources(struct process_group *pg)
{
    struct ibv_device **dev_list = NULL;
    struct ibv_device *ib_dev = NULL;

    /* Find RDMA device */
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        fprintf(stderr, "Failed to get RDMA device list\n");
        return -1;
    }

    ib_dev = dev_list[0];
    if (!ib_dev) {
        fprintf(stderr, "No RDMA device found\n");
        ibv_free_device_list(dev_list);
        return -1;
    }

    /* Open RDMA device */
    pg->context = ibv_open_device(ib_dev);
    if (!pg->context) {
        fprintf(stderr, "Failed to open RDMA device\n");
        ibv_free_device_list(dev_list);
        return -1;
    }

    ibv_free_device_list(dev_list);

    /* Allocate Protection Domain */
    pg->pd = ibv_alloc_pd(pg->context);
    if (!pg->pd) {
        fprintf(stderr, "Failed to allocate PD\n");
        return -1;
    }

    /* Create Completion Queue */
    pg->cq = ibv_create_cq(pg->context, 128, NULL, NULL, 0);
    if (!pg->cq) {
        fprintf(stderr, "Failed to create CQ\n");
        return -1;
    }

    /* Allocate Rendezvous control buffers */
    pg->control_send_buffer =
        (struct control_message *)calloc(1, sizeof(struct control_message));

    pg->control_recv_buffer =
        (struct control_message *)calloc(1, sizeof(struct control_message));

    if (!pg->control_send_buffer || !pg->control_recv_buffer) {
        fprintf(stderr, "Failed to allocate control buffers\n");
        return -1;
    }

    /* Register control buffers */
    pg->control_send_mr =
        ibv_reg_mr(pg->pd,
                   pg->control_send_buffer,
                   sizeof(struct control_message),
                   IBV_ACCESS_LOCAL_WRITE);

    pg->control_recv_mr =
        ibv_reg_mr(pg->pd,
                   pg->control_recv_buffer,
                   sizeof(struct control_message),
                   IBV_ACCESS_LOCAL_WRITE);

    if (!pg->control_send_mr || !pg->control_recv_mr) {
        fprintf(stderr, "Failed to register control buffers\n");
        return -1;
    }

    /* QP configuration */
    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.send_cq = pg->cq;
    qp_attr.recv_cq = pg->cq;

    qp_attr.cap.max_send_wr  = TX_DEPTH;
    qp_attr.cap.max_recv_wr  = RX_DEPTH;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    qp_attr.qp_type = IBV_QPT_RC;

    /* QP towards the next process */
    pg->next_qp = ibv_create_qp(pg->pd, &qp_attr);
    if (!pg->next_qp) {
        fprintf(stderr, "Failed to create next QP\n");
        return -1;
    }

    /* QP towards the previous process */
    pg->prev_qp = ibv_create_qp(pg->pd, &qp_attr);
    if (!pg->prev_qp) {
        fprintf(stderr, "Failed to create previous QP\n");
        return -1;
    }

    return 0;
}

/* Move a QP through INIT -> RTR -> RTS */
static int connect_one_qp(struct ibv_qp *qp,
                          const struct rdma_peer *remote,
                          uint32_t local_psn)
{
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));

    /* RESET -> INIT */
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = 1;
    attr.qp_access_flags =
        IBV_ACCESS_REMOTE_READ |
        IBV_ACCESS_REMOTE_WRITE;

    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_PKEY_INDEX |
                      IBV_QP_PORT |
                      IBV_QP_ACCESS_FLAGS)) {
        fprintf(stderr, "Failed to modify QP to INIT\n");
        return -1;
    }

    /* INIT -> RTR */
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = remote->qpn;
    attr.rq_psn = remote->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;

    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = remote->lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = 1;

    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_AV |
                      IBV_QP_PATH_MTU |
                      IBV_QP_DEST_QPN |
                      IBV_QP_RQ_PSN |
                      IBV_QP_MAX_DEST_RD_ATOMIC |
                      IBV_QP_MIN_RNR_TIMER)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return -1;
    }

    /* RTR -> RTS */
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = local_psn;
    attr.max_rd_atomic = 1;

    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_TIMEOUT |
                      IBV_QP_RETRY_CNT |
                      IBV_QP_RNR_RETRY |
                      IBV_QP_SQ_PSN |
                      IBV_QP_MAX_QP_RD_ATOMIC)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return -1;
    }

    return 0;
}


/*
 * Send exactly len bytes over a TCP socket.
 */
static int tcp_send_all(int sockfd, const void *buffer, size_t len)
{
    const char *ptr = (const char *)buffer;

    while (len > 0) {
        ssize_t n = send(sockfd, ptr, len, 0);

        if (n <= 0)
            return -1;

        ptr += n;
        len -= n;
    }

    return 0;
}


/*
 * Receive exactly len bytes from a TCP socket.
 */
static int tcp_recv_all(int sockfd, void *buffer, size_t len)
{
    char *ptr = (char *)buffer;

    while (len > 0) {
        ssize_t n = recv(sockfd, ptr, len, 0);

        if (n <= 0)
            return -1;

        ptr += n;
        len -= n;
    }

    return 0;
}

/*
 * Exchange RDMA information with our ring neighbors
 * and connect the two RC QPs.
 */
static int connect_qps(struct process_group *pg,
                       const char *servername)
{
    struct ibv_port_attr port_attr;

    /*
     * Get the LID of our local RDMA port.
     */
    if (ibv_query_port(pg->context, 1, &port_attr)) {
        fprintf(stderr, "Failed to query RDMA port\n");
        return -1;
    }

    uint16_t local_lid = port_attr.lid;


    /*
     * Each local QP gets its own starting PSN.
     */
    uint32_t next_psn = lrand48() & 0xffffff;
    uint32_t prev_psn = lrand48() & 0xffffff;


    /*
     * Describe our two local QPs.
     */
    struct rdma_peer local_next;
    struct rdma_peer local_prev;

    memset(&local_next, 0, sizeof(local_next));
    memset(&local_prev, 0, sizeof(local_prev));

    local_next.lid = local_lid;
    local_next.qpn = pg->next_qp->qp_num;
    local_next.psn = next_psn;

    local_prev.lid = local_lid;
    local_prev.qpn = pg->prev_qp->qp_num;
    local_prev.psn = prev_psn;


    /*
     * Each process listens for its PREVIOUS neighbor
     * and connects as a client to its NEXT neighbor.
     *
     * Process r listens on BASE_PORT + r.
     */
    const int BASE_PORT = 18515;

    int my_port   = BASE_PORT + pg->pid;
    int next_port = BASE_PORT + pg->next_pid;


    /*
     * Avoid deadlock:
     *
     * even ranks first listen and then connect,
     * odd ranks first connect and then listen.
     */
    if ((pg->pid % 2) == 0) {

        /*
         * Previous process connects to our prev_qp.
         */
        if (tcp_server_exchange(my_port,
                                &local_prev,
                                &pg->prev)) {

            fprintf(stderr,
                    "Failed to exchange information with previous process\n");

            return -1;
        }

        /*
         * Connect our next_qp to the next process.
         */
        if (tcp_client_exchange(servername,
                                next_port,
                                &local_next,
                                &pg->next)) {

            fprintf(stderr,
                    "Failed to exchange information with next process\n");

            return -1;
        }

    } else {

        /*
         * Connect our next_qp to the next process.
         */
        if (tcp_client_exchange(servername,
                                next_port,
                                &local_next,
                                &pg->next)) {

            fprintf(stderr,
                    "Failed to exchange information with next process\n");

            return -1;
        }

        /*
         * Previous process connects to our prev_qp.
         */
        if (tcp_server_exchange(my_port,
                                &local_prev,
                                &pg->prev)) {

            fprintf(stderr,
                    "Failed to exchange information with previous process\n");

            return -1;
        }
    }


    /*
     * We now know the remote LID/QPN/PSN.
     *
     * Connect:
     *
     *   our next_qp <-> next process's prev_qp
     *
     *   our prev_qp <-> previous process's next_qp
     */
    if (connect_one_qp(pg->next_qp,
                       &pg->next,
                       next_psn)) {

        fprintf(stderr, "Failed to connect next QP\n");
        return -1;
    }

    if (connect_one_qp(pg->prev_qp,
                       &pg->prev,
                       prev_psn)) {

        fprintf(stderr, "Failed to connect previous QP\n");
        return -1;
    }

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

    if (connect_qps(pg, servername))
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

/*Rendezvous helper function 
    - Wait for a spesific Randezuos control message*/
static int wait_for_control_message(struct process_group *pg,
                                    control_type expected_type)
{
    /* Prepare to receive the control message */
    if (post_receive(pg,
                     pg->next_qp,
                     pg->control_recv_buffer,
                     sizeof(struct control_message),
                     pg->control_recv_mr,
                     2)) {
        return -1;
    }

    /* Wait until the message arrives */
    struct ibv_wc wc;

    if (wait_for_completion(pg, &wc)) {
        return -1;
    }

    /* Verify that we received the expected control message */
    if (pg->control_recv_buffer->type != expected_type) {
        fprintf(stderr, "Unexpected Rendezvous control message\n");
        return -1;
    }

    return 0;
}

/* Rendezvous */
static int send_rendezvous(struct process_group *pg, void *buffer, size_t size, struct ibv_mr *mr)
{
    /* handshake */
    struct control_message *ctrl = pg->control_send_buffer;
    /* Send Egaer control message */
    ctrl->type = RENDEZVOUS_REQUEST;
    ctrl->size = size;

    if (send_eager(pg,
                   ctrl,
                   sizeof(struct control_message),
                   pg->control_send_mr)) {
        return -1;
    }

    /* wait for Ready message from the receiver */
    if (wait_for_control_message(pg, RENDEZVOUS_READY)) {
        return -1;
    }

    uint64_t remote_addr = pg->control_recv_buffer->addr;
    uint32_t remote_rkey = pg->control_recv_buffer->rkey;

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

    if (!pg || !send_buf || !recv_buf)
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