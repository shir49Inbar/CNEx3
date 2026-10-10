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
#include <cerrno>
#include <climit>
#include <string>
#include <vector>
#include <sstream>

#define TX_DEPTH 128
#define RX_DEPTH 128

#define MAX_PENDING_COMPLETIONS 128

#define SEGMENT_SIZE (64 * 1024)
#define PIPELINE_DEPTH 2

#define WR_PIPELINE_RECV_BASE 1000
#define WR_PIPELINE_SEND_BASE 2000

// Types Required by the API

// Indication for send_buf type
enum DATATYPE
{
    TYPE_INT32,
    TYPE_FP64
};

// which reduction action to preform
enum OPERATION
{
    OP_SUM,
    OP_PRODUCT
};

enum PROTOCOL
{
    PROTOCOL_EAGER,
    PROTOCOL_RENDEZVOUS
};

//
enum control_type
{
    RENDEZVOUS_REQUEST,
    RENDEZVOUS_READY,
    RENDEZVOUS_FIN
};

typedef enum
{
    WR_EAGER_SEND = 1,
    WR_EAGER_RECV,
    WR_CONTROL_SEND,
    WR_CONTROL_RECV_NEXT,
    WR_CONTROL_RECV_PREV,
    WR_RDMA_WRITE,
    WR_RDMA_WRITE_RECV
} wr_type;

//
struct control_message
{
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

    struct ibv_mr *control_send_mr; // memory registrations
    struct ibv_mr *control_recv_mr; // memory registrations

    // Info about near processes
    struct rdma_peer next;
    struct rdma_peer prev;

    struct ibv_wc pending_wc[MAX_PENDING_COMPLETIONS];
    int pending_count;
};

/* Helper Functions */
static size_t datatype_size(DATATYPE datatype)
{ // Returns the datatype size
    switch (datatype)
    {
    case TYPE_INT32:
        return sizeof(int32_t);
    case TYPE_FP64:
        return sizeof(double);
    default:
        return 0;
    }
}

template<typename T> static void reduce_typed(T *dst, const T *src, int count, OPERATION op)
{
    for (int i = 0; i < count; ++i)
    {
        switch (op)
        {
        case OP_SUM:
            dst[i] += src[i];
            break;
        case OP_PRODUCT:
            dst[i] *= src[i];
            break;
        }
    }
}

// Reduction
static void reduce(void *dst, const void *src, int count, DATATYPE datatype, OPERATION op)
{
    switch (datatype)
    {
    case TYPE_INT32:
        reduce_typed((int32_t *)dst, (const int32_t *)src, count, op);
        break;
    case TYPE_FP64:
        reduce_typed((double *)dst, (const double *)src, count, op);
        break;
    }
}

/* RDMA Helper functions */
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
                               uint64_t expected_wr_id,
                               struct ibv_wc *result)
{
    for (int i = 0; i < pg->pending_count; i++)
    {
        if (pg->pending_wc[i].wr_id == expected_wr_id)
        {
            *result = pg->pending_wc[i];

            pg->pending_wc[i] = pg->pending_wc[--pg->pending_count];

            return result->status == IBV_WC_SUCCESS ? 0 : -1;
        }
    }

    while (1)
    {
        struct ibv_wc wc;
        int n = ibv_poll_cq(pg->cq, 1, &wc);

        if (n < 0)
            return -1;

        if (n == 0)
            continue;

        if (wc.status != IBV_WC_SUCCESS)
        {
            fprintf(stderr, "RDMA completion failed: wr_id=%llu, status=%s (%d)\n", (unsigned long long)wc.wr_id, ibv_wc_status_str(wc.status), wc.status);
            return -1;
        }

        if (wc.wr_id == expected_wr_id)
        {
            *result = wc;
            return 0;
        }

        if (pg->pending_count >= MAX_PENDING_COMPLETIONS)
        {
            fprintf(stderr, "Too many pending completions\n");
            return -1;
        }

        pg->pending_wc[pg->pending_count++] = wc;
    }
}

/* RDMA Initialization */
static int init_rdma_resources(struct process_group *pg)
{
    struct ibv_device **dev_list = NULL;
    struct ibv_device *ib_dev = NULL;

    /* Find RDMA device */
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list)
    {
        fprintf(stderr, "Failed to get RDMA device list\n");
        return -1;
    }

    ib_dev = dev_list[0];
    if (!ib_dev)
    {
        fprintf(stderr, "No RDMA device found\n");
        ibv_free_device_list(dev_list);
        return -1;
    }

    /* Open RDMA device */
    pg->context = ibv_open_device(ib_dev);
    if (!pg->context)
    {
        fprintf(stderr, "Failed to open RDMA device\n");
        ibv_free_device_list(dev_list);
        return -1;
    }

    ibv_free_device_list(dev_list);

    /* Allocate Protection Domain */
    pg->pd = ibv_alloc_pd(pg->context);
    if (!pg->pd)
    {
        fprintf(stderr, "Failed to allocate PD\n");
        return -1;
    }

    /* Create Completion Queue */
    pg->cq = ibv_create_cq(pg->context, 128, NULL, NULL, 0);
    if (!pg->cq)
    {
        fprintf(stderr, "Failed to create CQ\n");
        return -1;
    }

    /* Allocate Rendezvous control buffers */
    pg->control_send_buffer =
        (struct control_message *)calloc(1, sizeof(struct control_message));

    pg->control_recv_buffer =
        (struct control_message *)calloc(1, sizeof(struct control_message));

    if (!pg->control_send_buffer || !pg->control_recv_buffer)
    {
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

    if (!pg->control_send_mr || !pg->control_recv_mr)
    {
        fprintf(stderr, "Failed to register control buffers\n");
        return -1;
    }

    /* QP configuration */
    struct ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));

    qp_attr.send_cq = pg->cq;
    qp_attr.recv_cq = pg->cq;

    qp_attr.cap.max_send_wr = TX_DEPTH;
    qp_attr.cap.max_recv_wr = RX_DEPTH;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;

    qp_attr.qp_type = IBV_QPT_RC;

    /* QP towards the next process */
    pg->next_qp = ibv_create_qp(pg->pd, &qp_attr);
    if (!pg->next_qp)
    {
        fprintf(stderr, "Failed to create next QP\n");
        return -1;
    }

    /* QP towards the previous process */
    pg->prev_qp = ibv_create_qp(pg->pd, &qp_attr);
    if (!pg->prev_qp)
    {
        fprintf(stderr, "Failed to create previous QP\n");
        return -1;
    }

    return 0;
}

static int prepare_data_buffers(struct process_group *pg, void *recv_buf, size_t total_bytes)
{
    if (total_bytes == 0)
        return 0;

    // Register the user-provided receive buffer
    pg->recv_mr = ibv_reg_mr(pg->pd, recv_buf, total_bytes, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!pg->recv_mr)
    {
        fprintf(stderr, "Failed to register receive buffer\n");
        return -1;
    }
    pg->recv_buffer = recv_buf;

    // Temporary buffer for incoming Reduce Scatter data
    size_t staging_size = PIPELINE_DEPTH * SEGMENT_SIZE;

    pg->staging_buffer = malloc(staging_size);

    if (!pg->staging_buffer)
    {
        fprintf(stderr, "Failed to allocate staging buffer\n");
        ibv_dereg_mr(pg->recv_mr);
        pg->recv_mr = NULL;
        pg->recv_buffer = NULL;
        return -1;
    }

    pg->staging_mr = ibv_reg_mr(pg->pd, pg->staging_buffer, staging_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!pg->staging_mr)
    {
        fprintf(stderr, "Failed to register staging buffer\n");

        free(pg->staging_buffer);
        pg->staging_buffer = NULL;

        ibv_dereg_mr(pg->recv_mr);
        pg->recv_mr = NULL;
        pg->recv_buffer = NULL;

        return -1;
    }

    return 0;
}

static void release_data_buffers(struct process_group *pg)
{
    if (pg->recv_mr)
    {
        ibv_dereg_mr(pg->recv_mr);
        pg->recv_mr = NULL;
    }

    if (pg->staging_mr)
    {
        ibv_dereg_mr(pg->staging_mr);
        pg->staging_mr = NULL;
    }

    free(pg->staging_buffer);
    pg->staging_buffer = NULL;

    pg->recv_buffer = NULL;
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
                          IBV_QP_ACCESS_FLAGS))
    {
        fprintf(stderr, "Failed to modify QP to INIT\n");
        return -1;
    }

    /* INIT -> RTR */
    memset(&attr, 0, sizeof(attr));

    attr.qp_state = IBV_QPS_RTR;  // Ready to receive
    attr.path_mtu = IBV_MTU_1024; // max pack size of 1024
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
                          IBV_QP_MIN_RNR_TIMER))
    {
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
                          IBV_QP_MAX_QP_RD_ATOMIC))
    {
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

    while (len > 0)
    {
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

    while (len > 0)
    {
        ssize_t n = recv(sockfd, ptr, len, 0);

        if (n <= 0)
            return -1;

        ptr += n;
        len -= n;
    }

    return 0;
}

static int tcp_accept_exchange(int listen_fd, const struct rdma_peer *local, struct rdma_peer *remote)
{
    int conn_fd = accept(listen_fd, NULL, NULL);
    if (conn_fd < 0)
    {
        perror("accept");
        return -1;
    }

    // Client sends firstl server receives first.
    if (tcp_recv_all(conn_fd, remote, sizeof(*remote)) || tcp_send_all(conn_fd, local, sizeof(*local)))
    {
        close(conn_fd);
        return -1;
    }

    close(conn_fd);
    return 0;
}

static int tcp_create_listener(int port)
{
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        perror("socket");
        return -1;
    }

    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("setsockopt");
        close(listen_fd);
        return -1;
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind");
        close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, 1) < 0)
    {
        perror("listen");
        close(listen_fd);
        return -1;
    }

    return listen_fd;
}

static int tcp_client_exchange(const char *servername, int port, const struct rdma_peer *local, struct rdma_peer *remote)
{
    struct addrinfo hints = {};
    struct addrinfo *res = NULL;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    if (getaddrinfo(servername, port_str, &hints, &res) != 0)
    {
        fprintf(stderr, "Failed to resolve server %s\n", servername);
        return -1;
    }

    int sockfd = -1;

    for (int attempt = 0; attempt < 100; ++attempt)
    {
        sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);

        if (sockfd < 0)
        {
            freeaddrinfo(res);
            return -1;
        }

        if (connect(sockfd, res->ai_addr, res->ai_addrlen) == 0)
        {
            break;
        }

        close(sockfd);
        sockfd = -1;
        usleep(100000);
    }

    freeaddrinfo(res);

    if (sockfd < 0)
    {
        fprintf(stderr, "Failed to connect to %s\n", servername);
        return -1;
    }

    if (tcp_send_all(sockfd, local, sizeof(*local)) || tcp_recv_all(sockfd, remote, sizeof(*remote)))
    {
        close(sockfd);
        return -1;
    }

    close(sockfd);
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
    if (ibv_query_port(pg->context, 1, &port_attr))
    {
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
    const int PORT = 18515;

    int listen_fd = tcp_create_listener(PORT);

    if (listen_fd < 0)
    {
        fprintf(stderr, "Failed to create TCP listener\n");
        return -1;
    }

    int rc = 0;

    if (pg->pid % 2 == 0)
    {
        if (tcp_accept_exchange(listen_fd, &local_prev, &pg->prev))
        {
            fprintf(stderr, "Failed to exchange information with PREV\n");
            rc = -1;
        }
        if (rc == 0 && tcp_client_exchange(servername, PORT, &local_next, &pg->next))
        {
            fprintf(stderr, "Failed to exchange information with NEXT\n");
            rc = -1;
        }
    }
    else
    {
        if (tcp_client_exchange(servername, PORT, &local_next, &pg->next))
        {
            fprintf(stderr, "Failed to exchange information with NEXT\n");
            rc = -1;
        }

        if (rc == 0 && tcp_accept_exchange(listen_fd, &local_prev, &pg->prev))
        {
            fprintf(stderr, "Failed to exchange information with PREV\n");
            rc = -1;
        }
    }

    close(listen_fd);

    if (rc != 0)
        return -1;

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
                       next_psn))
    {

        fprintf(stderr, "Failed to connect next QP\n");
        return -1;
    }

    if (connect_one_qp(pg->prev_qp,
                       &pg->prev,
                       prev_psn))
    {

        fprintf(stderr, "Failed to connect previous QP\n");
        return -1;
    }

    return 0;
}

static int parse_server_config(const char *config, int *rank, std::vector<std::string> *servers)
{
    if (!config || !rank || !servers)
        return -1;

    std::istringstream input(config);
    std::string first;
    std::string rank_str;
    std::string flag;

    if (!(input >> first))
        return -1;

    bool one_based = false;

    if (first == "-myindex")
    {
        if (!(input >> rank_str >> flag))
            return -1;
        one_based = true;
    }
    else
    {
        rank_str = first;
        if (!(input >> flag))
            return -1;
    }

    if (flag != "-list")
        return -1;

    char *end = nullptr;
    long parsed_rank = strtol(rank_str.c_str(), &end, 10);

    if (*end != '\0' || end == rank_str.c_str())
        return -1;

    servers->clear();

    std::string host;
    while (input >> host)
        servers->push_back(host);

    if (servers->size() != 2 && servers->size() != 4)
        return -1;

    if (one_based)
        parsed_rank--;

    if (parsed_rank < 0 || parsed_rank >= (long)servers->size())
        return -1;

    *rank = (int)parsed_rank;
    return 0;
}

/* Cleanup */
int pg_close(void *pg_handle)
{
    struct process_group *pg = (struct process_group *)pg_handle;

    if (!pg)
        return 0;

    release_data_buffers(pg);

    /* Destroy QPs */
    if (pg->next_qp)
        ibv_destroy_qp(pg->next_qp);
    if (pg->prev_qp)
        ibv_destroy_qp(pg->prev_qp);

    /* De-Register memory regions */
    if (pg->control_send_mr)
        ibv_dereg_mr(pg->control_send_mr);
    if (pg->control_recv_mr)
        ibv_dereg_mr(pg->control_recv_mr);

    /* Free memory buffers */
    if (pg->control_send_buffer)
        free(pg->control_send_buffer);
    if (pg->control_recv_buffer)
        free(pg->control_recv_buffer);

    /* Destroy completion queue */
    if (pg->cq)
        ibv_destroy_cq(pg->cq);

    /* Deallocate Protection Domain */
    if (pg->pd)
        ibv_dealloc_pd(pg->pd);

    /* Close Device Context */
    if (pg->context)
        ibv_close_device(pg->context);

    free(pg);

    return 0;
}

/* API- connect all processes into a ring */
int connect_process_group(char *servername, void **pg_handle)
{
    if (!servername || !pg_handle)
        return -1;

    *pg_handle = nullptr;

    int rank;
    std::vector<std::string> servers;

    if (parse_server_config(servername, &rank, &servers))
    {
        fprintf(stderr, "Invalid Server configuration\n");
        return -1;
    }

    struct process_group *pg = (struct process_group *)calloc(1, sizeof(struct process_group));

    if (!pg)
        return -1;

    pg->pid = rank;
    pg->num_processes = (int)servers.size();

    pg->next_pid = (rank + 1) % pg->num_processes;
    pg->prev_pid = (rank - 1 + pg->num_processes) % pg->num_processes;

    pg->pending_count = 0;

    const std::string &next_server = servers[pg->next_pid];

    if (init_rdma_resources(pg))
    {
        pg_close(pg);
        return -1;
    }

    if (connect_qps(pg, next_server.c_str()))
    {
        pg_close(pg);
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

    wr.wr_id = WR_EAGER_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.next = NULL;

    struct ibv_send_wr *bad_wr = NULL;

    /* send to the next process in the ring */
    if (ibv_post_send(pg->next_qp, &wr, &bad_wr))
    {
        fprintf(stderr, "Failed to post Eager SEND\n");
        return -1;
    }

    struct ibv_wc wc;
    if (wait_for_completion(pg, WR_EAGER_SEND, &wc))
    {
        fprintf(stderr, "Failed waiting for Eager SEND\n");
        return -1;
    }

    if (wc.opcode != IBV_WC_SEND)
        return -1;

    return 0;
}

static int post_eager_send(struct process_group *pg, void *buffer, size_t size, struct ibv_mr *mr)
{
    struct ibv_sge sge = {};
    sge.addr = (uintptr_t)buffer;
    sge.length = (uint32_t)size;
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr = {};
    wr.wr_id = WR_EAGER_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad_wr = NULL;

    if (ibv_post_send(pg->next_qp, &wr, &bad_wr))
    {
        fprintf(stderr, "Failed to post Eager SEND\n");
        return -1;
    }

    return 0;
}

static int post_eager_send_with_id(struct process_group *pg, void *buffer, size_t size, struct ibv_mr *mr, uint64_t wr_id)
{
    struct ibv_sge sge = {};
    sge.addr = (uintptr_t)buffer;
    sge.length = (uint32_t)size;
    sge.lkey = mr->lkey;

    struct ibv_send_wr wr = {};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad_wr = nullptr;

    if (ibv_post_send(pg->next_qp, &wr, &bad_wr) != 0)
    {
        fprintf(stderr, "Failed to post pipeline send\n");
        return -1;
    }

    return 0;
}

static int receive_eager(struct process_group *pg,
                         void *buffer,
                         size_t size,
                         struct ibv_mr *mr)
{
    if (post_receive(pg, pg->prev_qp, buffer, size, mr, WR_EAGER_RECV))
    {
        fprintf(stderr, "Failed to post Eager receive\n");
        return -1;
    }

    struct ibv_wc wc;
    if (wait_for_completion(pg, WR_EAGER_RECV, &wc))
    {
        fprintf(stderr, "Failed waiting for Eager receive\n");
        return -1;
    }

    return 0;
}

/*Rendezvous helper function
    - Wait for a spesific Randezuos control message*/
static int wait_for_control_message(struct process_group *pg,
                                    struct ibv_qp *qp,
                                    control_type expected_type)
{
    uint64_t wr_id;

    if (qp == pg->next_qp)
        wr_id = WR_CONTROL_RECV_NEXT;
    else if (qp == pg->prev_qp)
        wr_id = WR_CONTROL_RECV_PREV;
    else
        return -1;

    /* Prepare to receive the control message */
    if (post_receive(pg,
                     qp,
                     pg->control_recv_buffer,
                     sizeof(struct control_message),
                     pg->control_recv_mr,
                     wr_id))
    {
        return -1;
    }

    /* Wait until the message arrives */
    struct ibv_wc wc;

    if (wait_for_completion(pg, wr_id, &wc))
    {
        return -1;
    }

    if (wc.opcode != IBV_WC_RECV || wc.byte_len != sizeof(struct control_message))
    {
        fprintf(stderr, "Invaild control receive completion\n");
        return -1;
    }

    /* Verify that we received the expected control message */
    if (pg->control_recv_buffer->type != expected_type)
    {
        fprintf(stderr, "Unexpected Rendezvous control message\n");
        return -1;
    }

    return 0;
}

static int send_control_message(struct process_group *pg,
                                struct ibv_qp *qp, control_type type, size_t size, uint64_t addr, uint32_t rkey)
{
    struct control_message *ctrl = pg->control_send_buffer;

    ctrl->type = type;
    ctrl->size = size;
    ctrl->addr = addr;
    ctrl->rkey = rkey;

    struct ibv_sge sge = {};
    sge.addr = (uintptr_t)ctrl;
    sge.length = sizeof(struct control_message);
    sge.lkey = pg->control_send_mr->lkey;

    struct ibv_send_wr wr = {};
    wr.wr_id = WR_CONTROL_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    struct ibv_send_wr *bad_wr = NULL;

    if (ibv_post_send(qp, &wr, &bad_wr))
    {
        fprintf(stderr, "Failed to send control message\n");
        return -1;
    }

    struct ibv_wc wc;

    if (wait_for_completion(pg, WR_CONTROL_SEND, &wc))
        return -1;

    if (wc.opcode != IBV_WC_SEND)
        return -1;

    return 0;
}

/* Rendezvous */
static int send_rendezvous(struct process_group *pg, void *buffer, size_t size, struct ibv_mr *mr)
{

    if (send_control_message(pg, pg->next_qp, RENDEZVOUS_REQUEST, size, 0, 0))
        return -1;

    if (wait_for_control_message(pg, pg->next_qp, RENDEZVOUS_READY))
        return -1;

    if (pg->control_recv_buffer->size != size)
    {
        fprintf(stderr, "Rendezvous READY size mismatch\n");
        return -1;
    }

    uint64_t remote_addr = pg->control_recv_buffer->addr;
    uint32_t remote_rkey = pg->control_recv_buffer->rkey;

    /* Describe the local buffer */
    struct ibv_sge sge = {};
    sge.addr = (uintptr_t)buffer;
    sge.length = size;
    sge.lkey = mr->lkey;

    /* Create SEND Work Request */
    struct ibv_send_wr wr = {};

    wr.wr_id = WR_RDMA_WRITE;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;

    /* Remote memory information received during Rendezvous handshake */
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = remote_rkey;
    wr.imm_data = 0;

    struct ibv_send_wr *bad_wr = NULL;

    /* RDMA Write to the next process in the ring */
    if (ibv_post_send(pg->next_qp, &wr, &bad_wr))
    {
        fprintf(stderr, "Failed to post Rendezvous RDMA Write\n");
        return -1;
    }

    struct ibv_wc wc;
    if (wait_for_completion(pg, WR_RDMA_WRITE, &wc))
        return -1;

    if (wc.opcode != IBV_WC_RDMA_WRITE)
        return -1;

    return 0;
}

static int receive_rendezvous(struct process_group *pg, void *buffer, size_t buffer_size, struct ibv_mr *mr)
{
    // Receive request from PREV
    if (wait_for_control_message(pg, pg->prev_qp, RENDEZVOUS_REQUEST))
        return -1;

    size_t incoming_size = pg->control_recv_buffer->size;
    if (incoming_size != buffer_size)
    {
        fprintf(stderr, "Rendezvous message size mismatch\n");
        return -1;
    }

    // Post a receive WQE for WRITE_WITH_IMM
    struct ibv_recv_wr recv_wr = {};
    recv_wr.wr_id = WR_RDMA_WRITE_RECV;
    recv_wr.sg_list = NULL;
    recv_wr.num_sge = 0;

    struct ibv_recv_wr *bad_recv_wr = NULL;

    if (ibv_post_recv(pg->prev_qp, &recv_wr, &bad_recv_wr))
    {
        fprintf(stderr, "Failed to post rendezvous receive\n");
        return -1;
    }

    // Tell PREV where to write
    if (send_control_message(pg, pg->prev_qp, RENDEZVOUS_READY, incoming_size, (uint64_t)buffer, mr->rkey))
        return -1;

    // Wait for the RDMA Write notification
    struct ibv_wc wc;

    if (wait_for_completion(pg, WR_RDMA_WRITE_RECV, &wc))
        return -1;

    if (wc.opcode != IBV_WC_RECV_RDMA_WITH_IMM)
    {
        fprintf(stderr, "Expected RDMA Write with Immediate\n");
        return -1;
    }

    return 0;
}

static int begin_receive_rendezvous(struct process_group *pg, void *buffer, size_t size, struct ibv_mr *mr)
{
    if (wait_for_control_message(pg, pg->prev_qp, RENDEZVOUS_REQUEST))
        return -1;
    if (pg->control_recv_buffer->size != size)
    {
        fprintf(stderr, "Rendezvous request size mismatch\n");
        return -1;
    }

    struct ibv_recv_wr wr = {};
    wr.wr_id = WR_RDMA_WRITE_RECV;
    wr.num_sge = 0;
    wr.sg_list = NULL;

    struct ibv_recv_wr *bad_wr = NULL;
    if (ibv_post_recv(pg->prev_qp, &wr, &bad_wr))
        return -1;

    return send_control_message(pg, pg->prev_qp, RENDEZVOUS_READY, size, (uint64_t)(uintptr)buffer, mr->rkey);
}

static int finish_receive_rendezvous(struct process_group *pg)
{
    struct ibv_wc wc = {};
    if (wait_for_completion(pg, WR_RDMA_WRITE_RECV, &wc))
        return -1;

    if (wc.opcode != IBV_WC_RECV_RDMA_WITH_IMM)
        return -1;

    return 0;
}

/* Reduce Scatter */
static int reduce_scatter(struct process_group *pg, void *buffer, int count, DATATYPE datatype, OPERATION op, PROTOCOL protocol)
{
    const int P = pg->num_processes;
    const int pid = pg->pid;

    const size_t elem_size = datatype_size(datatype);
    const size_t chunk_count = (size_t)count / P;
    const size_t chunk_bytes = chunk_count * elem_size;

    char *staging = (char *)pg->staging_buffer;

    for (int step = 0; step < P - 1; ++step)
    {
        const int send_chunk_index = (pid - step - 1 + P) % P;

        const int recv_chunk_index = (pid - step - 2 + P) % P;

        char *send_ptr = (char *)buffer + send_chunk_index * chunk_bytes;

        char *recv_ptr = (char *)buffer + recv_chunk_index * chunk_bytes;

        const size_t num_segments = (chunk_bytes + SEGMENT_SIZE - 1) / SEGMENT_SIZE;

        if (protocol == PROTOCOL_EAGER)
        {
            // Post the first receive and send.
            const size_t first_bytes = chunk_bytes < SEGMENT_SIZE ? chunk_bytes : SEGMENT_SIZE;

            if (post_receive(pg, pg->prev_qp, staging, first_bytes, pg->staging_mr, WR_PIPELINE_RECV_BASE))
                return -1;

            if (post_eager_send_with_id(pg, send_ptr, first_bytes, pg->recv_mr, WR_PIPELINE_SEND_BASE))
                return -1;

            for (size_t segment = 0; segment < num_segments; ++segment)
            {
                const size_t slot = segment % PIPELINE_DEPTH;
                const size_t offset = segment * SEGMENT_SIZE;

                const size_t bytes = chunk_bytes - offset < SEGMENT_SIZE ? chunk_bytes - offset : SEGMENT_SIZE;

                char *current_staging = staging + slot * SEGMENT_SIZE;

                struct ibv_wc recv_wc = {};

                if (wait_for_completion(pg, WR_PIPELINE_RECV_BASE + segment, &recv_wc))
                    return -1;

                if (recv_wc.opcode != IBV_WC_RECV || recv_wc.byte_len != bytes)
                    return -1;

                // Start transferring the next segment before
                // reducing the current segment.
                if (segment + 1 < num_segments)
                {
                    const size_t next_segment = segment + 1;
                    const size_t next_slot = next_segment % PIPELINE_DEPTH;

                    const size_t next_offset = next_segment * SEGMENT_SIZE;

                    const size_t next_bytes = chunk_bytes - next_offset < SEGMENT_SIZE ? chunk_bytes - next_offset : SEGMENT_SIZE;

                    char *next_staging = staging + next_slot * SEGMENT_SIZE;

                    if (post_receive(pg, pg->prev_qp, next_staging, next_bytes, pg->staging_mr, WR_PIPELINE_RECV_BASE + next_segment))
                        return -1;

                    if (post_eager_send_with_id(pg, send_ptr + next_offset, next_bytes, pg->recv_mr, WR_PIPELINE_SEND_BASE + next_segment))
                        return -1;
                }

                // The next transfer can progress while
                // we reduce the current segment.
                reduce(recv_ptr + offset, current_staging, (int)(bytes / elem_size), datatype, op);

                struct ibv_wc send_wc = {};

                if (wait_for_completion(pg, WR_PIPELINE_SEND_BASE + segment, &send_wc))
                    return -1;

                if (send_wc.opcode != IBV_WC_SEND)
                    return -1;
            }
        }
        else if (protocol == PROTOCOL_RENDEZVOUS)
        {
            if (num_segments == 0)
                continue;

            const size_t first_bytes = chunk_bytes < SEGMENT_SIZE ? chunk_bytes : SEGMENT_SIZE;

            if (pid % 2 == 0)
            {
                if (begin_receive_rendezvous(pg, staging, first_bytes, pg->staging_mr))
                    return -1;

                if (send_rendezvous(pg, send_ptr, first_bytes, pg->recv_mr))
                    return -1;
            }
            else
            {
                if (send_rendezvous(pg, send_ptr, first_bytes, pg->recv_mr))
                    return -1;
                if (begin_receive_rendezvous(pg, staging, first_bytes, pg->staging_mr))
                    return -1;
            }
            for (size_t segment = 0; segment < num_segments; ++segment)
            {
                const size_t offset = segment * SEGMENT_SIZE;

                const size_t bytes = chunk_bytes - offset < SEGMENT_SIZE ? chunk_bytes - offset : SEGMENT_SIZE;
                char *current_staging = staging + (segment % PIPELINE_DEPTH) * SEGMENT_SIZE;

                if (finish_receive_rendezvous(pg))
                    return -1;

                if (segment + 1 < num_segments)
                {
                    const size_t next_offset = (segment + 1) * SEGMENT_SIZE;
                    const size_t next_bytes = chunk_bytes - next_offset < SEGMENT_SIZE;

                    char *next_staging = staging + ((segment + 1) % PIPELINE_DEPTH) * SEGMENT_SIZE;

                    if (pid % 2 == 0)
                    {
                        if (begin_receive_rendezvous(pg, next_staging, next_bytes, pg->staging_mr))
                            return -1;
                        if (send_rendezvous(pg, send_ptr + next_offset, next_bytes, pg->recv_mr))
                            return -1;
                    }
                    else
                    {
                        if (send_rendezvous(pg, send_ptr + next_offset, next_bytes, pg->recv_mr))
                            return -1;
                        if (begin_receive_rendezvous(pg, next_staging, next_bytes, pg->staging_mr))
                            return -1;
                    }
                }
                reduce(recv_ptr + offset, current_staging, (int)(bytes / elem_size), datatype, op);
            }
        }
        else
        {
            fprintf(stderr, "Unsupported protocol\n");
            return -1;
        }
    }

    return 0;
}

/* All Gather */
static int all_gather(struct process_group *pg, void *recv_buffer, int count, DATATYPE datatype, PROTOCOL protocol)
{
    const int P = pg->num_processes;
    const int pid = pg->pid;

    const size_t elem_size = datatype_size(datatype);
    const int chunk_count = (size_t)count / P;
    const size_t chunk_bytes = chunk_count * elem_size;

    /*
    Again, Ring requires P-1 steps
    */
    for (int step = 0; step < P - 1; ++step)
    {
        int send_chunk_index = (pid - step + P) % P;
        int recv_chunk_index = (pid - step - 1 + P) % P;

        char *send_ptr = (char *)recv_buffer + send_chunk_index * chunk_bytes;
        char *recv_ptr = (char *)recv_buffer + recv_chunk_index * chunk_bytes;

        for (size_t offset = 0; offset < chunk_bytes; offset += SEGMENT_SIZE)
        {
            size_t bytes = (chunk_bytes - offset < SEGMENT_SIZE) ? chunk_bytes - offset : SEGMENT_SIZE;
            if (protocol == PROTOCOL_EAGER)
            {
                // Receive directly into the final buffer.
                if (post_receive(pg, pg->prev_qp, recv_ptr + offset, bytes, pg->recv_mr, WR_EAGER_RECV))
                {
                    fprintf(stderr, "All Gather: failed to post receive\n");
                    return -1;
                }

                if (post_eager_send(pg, send_ptr + offset, bytes, pg->recv_mr))
                {
                    fprintf(stderr, "All Gather: failed to post send\n");
                    return -1;
                }

                struct ibv_wc recv_wc = {};
                if (wait_for_completion(pg, WR_EAGER_RECV, &recv_wc))
                    return -1;

                if (recv_wc.opcode != IBV_WC_RECV || recv_wc.byte_len != bytes)
                    return -1;

                struct ibv_wc send_wc = {};
                if (wait_for_completion(pg, WR_EAGER_SEND, &send_wc))
                    return -1;

                if (send_wc.opcode != IBV_WC_SEND)
                    return -1;
            }
            else if (protocol == PROTOCOL_RENDEZVOUS)
            {
                /*
                Both sides must participate in a rendezvous:
                - receive from PREV
                - send to NEXT

                The existing helpers are blocking, so we alternate their order by rank
                to avoid a circular wait in an even-sized ring.
                */
                if (pid % 2 == 0)
                {
                    if (receive_rendezvous(pg, recv_ptr + offset, bytes, pg->recv_mr))
                        return -1;

                    if (send_rendezvous(pg, send_ptr + offset, bytes, pg->recv_mr))
                        return -1;
                }
                else
                {
                    if (send_rendezvous(pg, send_ptr + offset, bytes, pg->recv_mr))
                        return -1;
                    if (receive_rendezvous(pg, recv_ptr + offset, bytes, pg->recv_mr))
                        return -1;
                }
            }
            else
            {
                fprintf(stderr, "All Gather: unsupported protocol\n");
                return -1;
            }
        }
    }
    return 0;
}

/* All Reduce */
int pg_all_reduce(void *send_buf, void *recv_buf, int count, DATATYPE datatype, OPERATION op, void *pg_handle)
{
    struct process_group *pg = (struct process_group *)pg_handle;

    if (!pg || count < 0)
        return -1;

    PROTOCOL protocol = PROTOCOL_EAGER;

    const char *env = getenv("ALLREDUCE_PROTOCOL");

    if (env && strcmp(env, "rendezvous") == 0)
    {
        protocol = PROTOCOL_RENDEZVOUS;
    }
    else if (env && strcmp(env, "eager") != 0)
    {
        fprintf(stderr, "Unknown AllReduce protocol\n");
        return -1;
    }

    size_t element_size = datatype_size(datatype);

    if (element_size == 0 || (op != OP_SUM && op != OP_PRODUCT))
        return -1;

    if (count == 0)
        return 0;

    if (!send_buf || !recv_buf)
        return -1;

    if ((size_t)count > SIZE_MAX / element_size)
        return -1;

    size_t bytes = (size_t)count * element_size;

    if (count % pg->num_processes != 0)
        return -1;

    if (send_buf != recv_buf)
        memcpy(recv_buf, send_buf, bytes);

    release_data_buffers(pg);

    if (prepare_data_buffers(pg, recv_buf, bytes) != 0)
        return -1;

    int rc = 0;

    /* Phase 1: Reduce Scatter */
    if (reduce_scatter(pg, recv_buf, count, datatype, op, protocol) != 0)
    {
        rc = -1;
    }

    /* Phase 2: All Gather */

    if (rc == 0 && all_gather(pg, recv_buf, count, datatype, protocol) != 0)
    {
        rc = -1;
    }

    release_data_buffers(pg);

    return rc;
}
