#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_BASE_PORT (20000 + (int)(getuid() % 40000))
#define DEFAULT_IB_PORT 1
#define DEFAULT_GID_INDEX (-1)
#define DEFAULT_EAGER_THRESHOLD 8192U
#define DEFAULT_PIPELINE_BYTES (64U * 1024U)
#define CONNECT_RETRIES 300
#define MAX_QP_WR 128
#define CQ_CAPACITY 512

#define WR_SEND_BIT (UINT64_C(1) << 63)
#define PHASE_REDUCE_SCATTER 1U
#define PHASE_ALL_GATHER 2U

typedef enum {
    DATATYPE_INT,
    DATATYPE_FLOAT,
    DATATYPE_DOUBLE
} DATATYPE;

typedef enum {
    OP_SUM,
    OP_PROD,
    OP_MAX,
    OP_MIN
} OPERATION;

int connect_process_group(char *servername, void **pg_handle);
int pg_reduce_scatter(void *sendbuf, void *recvbuf, int recv_count,
                      DATATYPE datatype, OPERATION op, void *pg_handle);
int pg_all_gather(void *sendbuf, void *recvbuf, int send_count,
                  DATATYPE datatype, void *pg_handle);
int pg_all_reduce(void *sendbuf, void *recvbuf, int count,
                  DATATYPE datatype, OPERATION op, void *pg_handle);
int pg_close(void *pg_handle);

typedef enum {
    PROTOCOL_AUTO,
    PROTOCOL_EAGER,
    PROTOCOL_RENDEZVOUS
} protocol_mode_t;

typedef struct {
    uint32_t qpn;
    uint32_t psn;
    uint16_t lid;
    uint8_t mtu;
    uint8_t gid[16];
    uint64_t scratch_addr;
    uint32_t scratch_rkey;
} qp_endpoint_t;

typedef struct __attribute__((packed)) {
    uint32_t qpn;
    uint32_t psn;
    uint16_t lid;
    uint8_t mtu;
    uint8_t gid[16];
    uint64_t scratch_addr;
    uint32_t scratch_rkey;
} qp_endpoint_wire_t;

typedef struct __attribute__((packed)) {
    uint64_t addr;
    uint32_t rkey;
} mr_wire_t;

typedef struct {
    int rank;
    int size;
    char **hosts;
    int base_port;
    int ib_port;
    int gid_index;
    size_t eager_threshold;
    size_t pipeline_bytes;
    protocol_mode_t protocol;

    int listen_fd;
    int next_sock;
    int prev_sock;

    struct ibv_context *context;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *tx_qp;
    struct ibv_qp *rx_qp;
    uint32_t tx_psn;
    uint32_t rx_psn;
    struct ibv_port_attr port_attr;
    union ibv_gid gid;

    void *scratch;
    struct ibv_mr *scratch_mr;
    qp_endpoint_t next_rx;
} pg_handle_t;

static uint64_t host_to_be64(uint64_t value)
{
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return __builtin_bswap64(value);
#else
    return value;
#endif
}

static uint64_t be64_to_host(uint64_t value)
{
    return host_to_be64(value);
}

static int report_error(const char *message)
{
    fprintf(stderr, "ex3: %s\n", message);
    return -1;
}

static int report_errno(const char *operation)
{
    fprintf(stderr, "ex3: %s: %s\n", operation, strerror(errno));
    return -1;
}

static long env_long(const char *name, long default_value, long minimum,
                     long maximum)
{
    const char *text = getenv(name);
    char *end = NULL;
    long value;

    if (!text || !*text)
        return default_value;

    errno = 0;
    value = strtol(text, &end, 10);
    if (errno || !end || *end || value < minimum || value > maximum) {
        fprintf(stderr, "ex3: invalid %s value '%s'\n", name, text);
        return default_value;
    }
    return value;
}

static protocol_mode_t protocol_from_env(void)
{
    const char *value = getenv("PG_PROTOCOL");

    if (!value || !strcmp(value, "auto"))
        return PROTOCOL_AUTO;
    if (!strcmp(value, "eager"))
        return PROTOCOL_EAGER;
    if (!strcmp(value, "rendezvous"))
        return PROTOCOL_RENDEZVOUS;

    fprintf(stderr, "ex3: unknown PG_PROTOCOL '%s'; using auto\n", value);
    return PROTOCOL_AUTO;
}

static size_t datatype_size(DATATYPE datatype)
{
    switch (datatype) {
    case DATATYPE_INT:
        return sizeof(int);
    case DATATYPE_FLOAT:
        return sizeof(float);
    case DATATYPE_DOUBLE:
        return sizeof(double);
    }
    return 0;
}

static int split_hosts(const char *host_list, char ***hosts_out, int *size_out)
{
    char *copy = NULL;
    char *save = NULL;
    char *token;
    char **hosts = NULL;
    int count = 1;
    int index = 0;

    if (!host_list || !*host_list)
        return report_error("the host list is empty");

    for (const char *p = host_list; *p; ++p)
        count += (*p == ',');

    copy = strdup(host_list);
    hosts = calloc((size_t)count, sizeof(*hosts));
    if (!copy || !hosts)
        goto allocation_failure;

    for (token = strtok_r(copy, ",", &save); token;
         token = strtok_r(NULL, ",", &save)) {
        if (!*token || index == count)
            goto invalid_list;
        hosts[index] = strdup(token);
        if (!hosts[index])
            goto allocation_failure;
        ++index;
    }

    free(copy);
    if (index < 2) {
        for (int i = 0; i < index; ++i)
            free(hosts[i]);
        free(hosts);
        return report_error("at least two hosts/processes are required");
    }

    *hosts_out = hosts;
    *size_out = index;
    return 0;

invalid_list:
    fprintf(stderr, "ex3: invalid host list '%s'\n", host_list);
    for (int i = 0; i < index; ++i)
        free(hosts[i]);
    free(hosts);
    free(copy);
    return -1;

allocation_failure:
    for (int i = 0; i < index; ++i)
        free(hosts[i]);
    free(hosts);
    free(copy);
    return report_errno("allocating host list");
}

static void free_hosts(pg_handle_t *handle)
{
    if (!handle->hosts)
        return;
    for (int i = 0; i < handle->size; ++i)
        free(handle->hosts[i]);
    free(handle->hosts);
    handle->hosts = NULL;
}

static int send_all(int fd, const void *buffer, size_t length)
{
    const uint8_t *cursor = buffer;

    while (length) {
        ssize_t written = send(fd, cursor, length, MSG_NOSIGNAL);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            return report_errno("send");
        }
        if (!written)
            return report_error("socket closed during send");
        cursor += written;
        length -= (size_t)written;
    }
    return 0;
}

static int recv_all(int fd, void *buffer, size_t length)
{
    uint8_t *cursor = buffer;

    while (length) {
        ssize_t received = recv(fd, cursor, length, 0);
        if (received < 0) {
            if (errno == EINTR)
                continue;
            return report_errno("recv");
        }
        if (!received)
            return report_error("socket closed during receive");
        cursor += received;
        length -= (size_t)received;
    }
    return 0;
}

static int set_tcp_nodelay(int fd)
{
    int enabled = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled,
                      sizeof(enabled));
}

static int create_listener(int port)
{
    struct addrinfo hints = {
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_flags = AI_PASSIVE
    };
    struct addrinfo *results = NULL;
    struct addrinfo *entry;
    char service[16];
    int listener = -1;
    int reuse = 1;

    snprintf(service, sizeof(service), "%d", port);
    if (getaddrinfo(NULL, service, &hints, &results)) {
        report_error("getaddrinfo failed while creating listener");
        return -1;
    }

    for (entry = results; entry; entry = entry->ai_next) {
        listener = socket(entry->ai_family, entry->ai_socktype,
                          entry->ai_protocol);
        if (listener < 0)
            continue;
        setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        if (!bind(listener, entry->ai_addr, entry->ai_addrlen) &&
            !listen(listener, 4))
            break;
        close(listener);
        listener = -1;
    }

    freeaddrinfo(results);
    if (listener < 0)
        report_errno("creating TCP listener");
    return listener;
}

static int connect_retry(const char *host, int port)
{
    struct addrinfo hints = {
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM
    };
    struct addrinfo *results = NULL;
    struct addrinfo *entry;
    char service[16];
    int fd = -1;

    snprintf(service, sizeof(service), "%d", port);

    for (int attempt = 0; attempt < CONNECT_RETRIES; ++attempt) {
        int gai_result = getaddrinfo(host, service, &hints, &results);
        if (gai_result) {
            fprintf(stderr, "ex3: getaddrinfo(%s): %s\n", host,
                    gai_strerror(gai_result));
            return -1;
        }

        for (entry = results; entry; entry = entry->ai_next) {
            fd = socket(entry->ai_family, entry->ai_socktype,
                        entry->ai_protocol);
            if (fd < 0)
                continue;
            if (!connect(fd, entry->ai_addr, entry->ai_addrlen))
                break;
            close(fd);
            fd = -1;
        }
        freeaddrinfo(results);
        results = NULL;

        if (fd >= 0) {
            set_tcp_nodelay(fd);
            return fd;
        }
        usleep(100000);
    }

    fprintf(stderr, "ex3: could not connect to %s:%d\n", host, port);
    return -1;
}

static int accept_connection(int listener)
{
    int fd;
    do {
        fd = accept(listener, NULL, NULL);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0)
        return report_errno("accept");
    set_tcp_nodelay(fd);
    return fd;
}

static void encode_endpoint(const qp_endpoint_t *source,
                            qp_endpoint_wire_t *destination)
{
    destination->qpn = htonl(source->qpn);
    destination->psn = htonl(source->psn);
    destination->lid = htons(source->lid);
    destination->mtu = source->mtu;
    memcpy(destination->gid, source->gid, sizeof(destination->gid));
    destination->scratch_addr = host_to_be64(source->scratch_addr);
    destination->scratch_rkey = htonl(source->scratch_rkey);
}

static void decode_endpoint(const qp_endpoint_wire_t *source,
                            qp_endpoint_t *destination)
{
    destination->qpn = ntohl(source->qpn);
    destination->psn = ntohl(source->psn);
    destination->lid = ntohs(source->lid);
    destination->mtu = source->mtu;
    memcpy(destination->gid, source->gid, sizeof(destination->gid));
    destination->scratch_addr = be64_to_host(source->scratch_addr);
    destination->scratch_rkey = ntohl(source->scratch_rkey);
}

static int send_endpoint(int fd, const qp_endpoint_t *endpoint)
{
    qp_endpoint_wire_t wire;
    encode_endpoint(endpoint, &wire);
    return send_all(fd, &wire, sizeof(wire));
}

static int recv_endpoint(int fd, qp_endpoint_t *endpoint)
{
    qp_endpoint_wire_t wire;
    if (recv_all(fd, &wire, sizeof(wire)))
        return -1;
    decode_endpoint(&wire, endpoint);
    return 0;
}

static int modify_qp_to_init(struct ibv_qp *qp, int ib_port)
{
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_INIT,
        .port_num = (uint8_t)ib_port,
        .pkey_index = 0,
        .qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
    };
    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                IBV_QP_ACCESS_FLAGS;

    if (ibv_modify_qp(qp, &attr, flags))
        return report_errno("moving QP to INIT");
    return 0;
}

static int modify_qp_to_rtr(pg_handle_t *handle, struct ibv_qp *qp,
                            const qp_endpoint_t *remote)
{
    enum ibv_mtu local_mtu = handle->port_attr.active_mtu;
    enum ibv_mtu remote_mtu = (enum ibv_mtu)remote->mtu;
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = local_mtu < remote_mtu ? local_mtu : remote_mtu,
        .dest_qp_num = remote->qpn,
        .rq_psn = remote->psn,
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12,
        .ah_attr = {
            .dlid = remote->lid,
            .sl = 0,
            .src_path_bits = 0,
            .port_num = (uint8_t)handle->ib_port
        }
    };
    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    if (handle->gid_index >= 0) {
        attr.ah_attr.is_global = 1;
        memcpy(&attr.ah_attr.grh.dgid, remote->gid,
               sizeof(attr.ah_attr.grh.dgid));
        attr.ah_attr.grh.sgid_index = (uint8_t)handle->gid_index;
        attr.ah_attr.grh.hop_limit = 1;
    }

    if (ibv_modify_qp(qp, &attr, flags))
        return report_errno("moving QP to RTR");
    return 0;
}

static int modify_qp_to_rts(struct ibv_qp *qp, uint32_t psn)
{
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTS,
        .timeout = 14,
        .retry_cnt = 7,
        .rnr_retry = 7,
        .sq_psn = psn,
        .max_rd_atomic = 1
    };
    int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                IBV_QP_MAX_QP_RD_ATOMIC;

    if (ibv_modify_qp(qp, &attr, flags))
        return report_errno("moving QP to RTS");
    return 0;
}

static qp_endpoint_t local_endpoint(const pg_handle_t *handle,
                                    const struct ibv_qp *qp, uint32_t psn)
{
    qp_endpoint_t endpoint = {
        .qpn = qp->qp_num,
        .psn = psn,
        .lid = handle->port_attr.lid,
        .mtu = (uint8_t)handle->port_attr.active_mtu,
        .scratch_addr = (uintptr_t)handle->scratch,
        .scratch_rkey = handle->scratch_mr->rkey
    };
    memcpy(endpoint.gid, handle->gid.raw, sizeof(endpoint.gid));
    return endpoint;
}

static int outgoing_handshake(pg_handle_t *handle)
{
    qp_endpoint_t local =
        local_endpoint(handle, handle->tx_qp, handle->tx_psn);

    if (send_endpoint(handle->next_sock, &local) ||
        recv_endpoint(handle->next_sock, &handle->next_rx))
        return -1;

    if (modify_qp_to_rtr(handle, handle->tx_qp, &handle->next_rx) ||
        modify_qp_to_rts(handle->tx_qp, handle->tx_psn))
        return -1;
    return 0;
}

static int incoming_handshake(pg_handle_t *handle)
{
    qp_endpoint_t previous_tx;
    qp_endpoint_t local =
        local_endpoint(handle, handle->rx_qp, handle->rx_psn);

    if (recv_endpoint(handle->prev_sock, &previous_tx))
        return -1;
    if (modify_qp_to_rtr(handle, handle->rx_qp, &previous_tx) ||
        modify_qp_to_rts(handle->rx_qp, handle->rx_psn))
        return -1;
    return send_endpoint(handle->prev_sock, &local);
}

static int create_verbs_resources(pg_handle_t *handle)
{
    struct ibv_device **devices;
    struct ibv_device *selected = NULL;
    const char *requested_device = getenv("PG_DEVICE");
    int device_count = 0;
    struct ibv_qp_init_attr qp_init = {
        .cap = {
            .max_send_wr = MAX_QP_WR,
            .max_recv_wr = MAX_QP_WR,
            .max_send_sge = 1,
            .max_recv_sge = 1
        },
        .qp_type = IBV_QPT_RC
    };

    devices = ibv_get_device_list(&device_count);
    if (!devices || !device_count)
        return report_error("no RDMA device is available");

    for (int i = 0; i < device_count; ++i) {
        if (!requested_device ||
            !strcmp(requested_device, ibv_get_device_name(devices[i]))) {
            selected = devices[i];
            break;
        }
    }
    if (!selected) {
        ibv_free_device_list(devices);
        return report_error("PG_DEVICE does not name an available device");
    }

    handle->context = ibv_open_device(selected);
    ibv_free_device_list(devices);
    if (!handle->context)
        return report_errno("ibv_open_device");

    if (ibv_query_port(handle->context, (uint8_t)handle->ib_port,
                       &handle->port_attr))
        return report_errno("ibv_query_port");
    if (handle->port_attr.state != IBV_PORT_ACTIVE)
        return report_error("selected RDMA port is not active");

    memset(&handle->gid, 0, sizeof(handle->gid));
    if (handle->gid_index >= 0 &&
        ibv_query_gid(handle->context, (uint8_t)handle->ib_port,
                      handle->gid_index, &handle->gid))
        return report_errno("ibv_query_gid");

    handle->pd = ibv_alloc_pd(handle->context);
    if (!handle->pd)
        return report_errno("ibv_alloc_pd");

    handle->cq = ibv_create_cq(handle->context, CQ_CAPACITY, NULL, NULL, 0);
    if (!handle->cq)
        return report_errno("ibv_create_cq");

    qp_init.send_cq = handle->cq;
    qp_init.recv_cq = handle->cq;
    handle->tx_qp = ibv_create_qp(handle->pd, &qp_init);
    handle->rx_qp = ibv_create_qp(handle->pd, &qp_init);
    if (!handle->tx_qp || !handle->rx_qp)
        return report_errno("ibv_create_qp");

    if (modify_qp_to_init(handle->tx_qp, handle->ib_port) ||
        modify_qp_to_init(handle->rx_qp, handle->ib_port))
        return -1;

    if (posix_memalign(&handle->scratch, 4096, handle->pipeline_bytes))
        return report_error("could not allocate the pipeline scratch buffer");
    memset(handle->scratch, 0, handle->pipeline_bytes);

    handle->scratch_mr =
        ibv_reg_mr(handle->pd, handle->scratch, handle->pipeline_bytes,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!handle->scratch_mr)
        return report_errno("ibv_reg_mr(scratch)");

    return 0;
}

static uint32_t random_psn(int rank, unsigned salt)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint32_t)(now.tv_nsec ^ getpid() ^ (rank << 12) ^ salt) &
           UINT32_C(0x00ffffff);
}

int connect_process_group(char *servername, void **pg_handle)
{
    pg_handle_t *handle;
    int next_rank;

    if (!pg_handle)
        return report_error("pg_handle is NULL");
    *pg_handle = NULL;

    handle = calloc(1, sizeof(*handle));
    if (!handle)
        return report_errno("allocating process-group handle");

    handle->listen_fd = -1;
    handle->next_sock = -1;
    handle->prev_sock = -1;
    handle->base_port =
        (int)env_long("PG_BASE_PORT", DEFAULT_BASE_PORT, 1024, 65000);
    handle->ib_port =
        (int)env_long("PG_IB_PORT", DEFAULT_IB_PORT, 1, 255);
    handle->gid_index =
        (int)env_long("PG_GID_INDEX", DEFAULT_GID_INDEX, -1, 255);
    handle->eager_threshold =
        (size_t)env_long("PG_EAGER_THRESHOLD", DEFAULT_EAGER_THRESHOLD,
                         0, 1L << 30);
    handle->pipeline_bytes =
        (size_t)env_long("PG_PIPELINE_BYTES", DEFAULT_PIPELINE_BYTES,
                         1, 1L << 30);
    if (handle->pipeline_bytes < sizeof(double))
        handle->pipeline_bytes = sizeof(double);
    handle->pipeline_bytes -= handle->pipeline_bytes % sizeof(double);
    handle->protocol = protocol_from_env();

    if (split_hosts(servername, &handle->hosts, &handle->size))
        goto failure;
    if (handle->size > 255) {
        report_error("the immediate-data format supports at most 255 ranks");
        goto failure;
    }

    handle->rank = (int)env_long("PG_RANK", -1, -1, handle->size - 1);
    if (handle->rank < 0) {
        report_error("PG_RANK must identify this process in the host list");
        goto failure;
    }

    handle->tx_psn = random_psn(handle->rank, 0x13579U);
    handle->rx_psn = random_psn(handle->rank, 0x2468aU);

    if (create_verbs_resources(handle))
        goto failure;

    handle->listen_fd =
        create_listener(handle->base_port + handle->rank);
    if (handle->listen_fd < 0)
        goto failure;

    next_rank = (handle->rank + 1) % handle->size;
    handle->next_sock =
        connect_retry(handle->hosts[next_rank],
                      handle->base_port + next_rank);
    if (handle->next_sock < 0)
        goto failure;

    /*
     * Rank zero breaks the ring handshake dependency. Every listener is
     * already active, so all TCP connects can complete before this ordering.
     */
    if (handle->rank == 0) {
        handle->prev_sock = accept_connection(handle->listen_fd);
        if (handle->prev_sock < 0 || incoming_handshake(handle) ||
            outgoing_handshake(handle))
            goto failure;
    } else {
        if (outgoing_handshake(handle))
            goto failure;
        handle->prev_sock = accept_connection(handle->listen_fd);
        if (handle->prev_sock < 0 || incoming_handshake(handle))
            goto failure;
    }

    close(handle->listen_fd);
    handle->listen_fd = -1;
    *pg_handle = handle;
    return 0;

failure:
    pg_close(handle);
    return -1;
}

static int ring_barrier(pg_handle_t *handle)
{
    uint32_t enter = htonl(UINT32_C(0x454e5445));
    uint32_t release = htonl(UINT32_C(0x52454c53));
    uint32_t token;

    if (handle->rank == 0) {
        if (send_all(handle->next_sock, &enter, sizeof(enter)) ||
            recv_all(handle->prev_sock, &token, sizeof(token)) ||
            token != enter ||
            send_all(handle->next_sock, &release, sizeof(release)) ||
            recv_all(handle->prev_sock, &token, sizeof(token)) ||
            token != release)
            return report_error("process-group barrier failed");
    } else {
        if (recv_all(handle->prev_sock, &token, sizeof(token)) ||
            token != enter ||
            send_all(handle->next_sock, &token, sizeof(token)) ||
            recv_all(handle->prev_sock, &token, sizeof(token)) ||
            token != release ||
            send_all(handle->next_sock, &token, sizeof(token)))
            return report_error("process-group barrier failed");
    }
    return 0;
}

static int exchange_next_mr(pg_handle_t *handle, const struct ibv_mr *local,
                            uint64_t local_addr, uint64_t *next_addr,
                            uint32_t *next_rkey)
{
    mr_wire_t outgoing = {
        .addr = host_to_be64(local_addr),
        .rkey = htonl(local->rkey)
    };
    mr_wire_t incoming;

    /*
     * Each rank publishes its destination MR to the previous rank and reads
     * the next rank's MR. The descriptors are small enough to avoid a socket
     * send-cycle deadlock.
     */
    if (send_all(handle->prev_sock, &outgoing, sizeof(outgoing)) ||
        recv_all(handle->next_sock, &incoming, sizeof(incoming)))
        return -1;

    *next_addr = be64_to_host(incoming.addr);
    *next_rkey = ntohl(incoming.rkey);
    return 0;
}

static size_t chunk_count(int total_count, int size, int chunk)
{
    size_t base = (size_t)total_count / (size_t)size;
    size_t remainder = (size_t)total_count % (size_t)size;
    return base + ((size_t)chunk < remainder);
}

static size_t chunk_offset(int total_count, int size, int chunk)
{
    size_t base = (size_t)total_count / (size_t)size;
    size_t remainder = (size_t)total_count % (size_t)size;
    size_t prefix_remainder =
        (size_t)chunk < remainder ? (size_t)chunk : remainder;
    return (size_t)chunk * base + prefix_remainder;
}

static int reduce_values(void *destination, const void *source, size_t count,
                         DATATYPE datatype, OPERATION operation)
{
#define REDUCE_TYPED(type)                                                     \
    do {                                                                       \
        type *dst = destination;                                                \
        const type *src = source;                                               \
        for (size_t i = 0; i < count; ++i) {                                   \
            switch (operation) {                                                \
            case OP_SUM:                                                        \
                dst[i] += src[i];                                               \
                break;                                                         \
            case OP_PROD:                                                       \
                dst[i] *= src[i];                                               \
                break;                                                         \
            case OP_MAX:                                                        \
                if (src[i] > dst[i])                                            \
                    dst[i] = src[i];                                            \
                break;                                                         \
            case OP_MIN:                                                        \
                if (src[i] < dst[i])                                            \
                    dst[i] = src[i];                                            \
                break;                                                         \
            default:                                                           \
                return -1;                                                      \
            }                                                                  \
        }                                                                      \
    } while (0)

    switch (datatype) {
    case DATATYPE_INT:
        REDUCE_TYPED(int);
        break;
    case DATATYPE_FLOAT:
        REDUCE_TYPED(float);
        break;
    case DATATYPE_DOUBLE:
        REDUCE_TYPED(double);
        break;
    default:
        return report_error("unsupported datatype");
    }
#undef REDUCE_TYPED
    return 0;
}

static uint32_t make_tag(unsigned phase, unsigned step, unsigned block)
{
    return ((phase & 0xffU) << 24) | ((step & 0xffU) << 16) |
           (block & 0xffffU);
}

static int poll_one(pg_handle_t *handle, struct ibv_wc *completion)
{
    for (;;) {
        int result = ibv_poll_cq(handle->cq, 1, completion);
        if (result < 0)
            return report_error("ibv_poll_cq failed");
        if (!result)
            continue;
        if (completion->status != IBV_WC_SUCCESS) {
            fprintf(stderr, "ex3: work completion failed: %s (%d)\n",
                    ibv_wc_status_str(completion->status),
                    completion->status);
            return -1;
        }
        return 0;
    }
}

static int post_receive(pg_handle_t *handle, uint32_t tag, void *destination,
                        size_t length, const struct ibv_mr *mr,
                        bool eager)
{
    struct ibv_sge sge = {
        .addr = (uintptr_t)destination,
        .length = (uint32_t)length,
        .lkey = mr ? mr->lkey : 0
    };
    struct ibv_recv_wr wr = {
        .wr_id = tag,
        .sg_list = eager && length ? &sge : NULL,
        .num_sge = eager && length ? 1 : 0
    };
    struct ibv_recv_wr *bad_wr = NULL;

    if (ibv_post_recv(handle->rx_qp, &wr, &bad_wr))
        return report_errno("ibv_post_recv");
    return 0;
}

static int post_transfer(pg_handle_t *handle, uint32_t tag,
                         const void *source, size_t length,
                         const struct ibv_mr *source_mr, bool eager,
                         uint64_t remote_addr, uint32_t remote_rkey)
{
    struct ibv_sge sge = {
        .addr = (uintptr_t)source,
        .length = (uint32_t)length,
        .lkey = source_mr ? source_mr->lkey : 0
    };
    struct ibv_send_wr wr = {
        .wr_id = WR_SEND_BIT | tag,
        .sg_list = length ? &sge : NULL,
        .num_sge = length ? 1 : 0,
        .opcode = eager || !length ? IBV_WR_SEND_WITH_IMM
                                   : IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data = htonl(tag)
    };
    struct ibv_send_wr *bad_wr = NULL;

    if (!eager && length) {
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey = remote_rkey;
    }

    if (ibv_post_send(handle->tx_qp, &wr, &bad_wr))
        return report_errno("ibv_post_send");
    return 0;
}

static int wait_for_receive(pg_handle_t *handle, uint32_t tag,
                            bool *send_completed)
{
    struct ibv_wc completion;

    for (;;) {
        if (poll_one(handle, &completion))
            return -1;

        if (completion.wr_id & WR_SEND_BIT) {
            if ((uint32_t)(completion.wr_id & ~WR_SEND_BIT) != tag)
                return report_error("unexpected send completion");
            *send_completed = true;
            continue;
        }

        if ((uint32_t)completion.wr_id != tag)
            return report_error("unexpected receive completion");
        if (!(completion.wc_flags & IBV_WC_WITH_IMM) ||
            ntohl(completion.imm_data) != tag)
            return report_error("invalid immediate completion tag");
        return 0;
    }
}

static int wait_for_send(pg_handle_t *handle, uint32_t tag)
{
    struct ibv_wc completion;

    if (poll_one(handle, &completion))
        return -1;
    if (!(completion.wr_id & WR_SEND_BIT) ||
        (uint32_t)(completion.wr_id & ~WR_SEND_BIT) != tag)
        return report_error("unexpected completion while waiting for send");
    return 0;
}

static bool use_eager(const pg_handle_t *handle, size_t total_bytes)
{
    if (handle->protocol == PROTOCOL_EAGER)
        return true;
    if (handle->protocol == PROTOCOL_RENDEZVOUS)
        return false;
    return total_bytes <= handle->eager_threshold;
}

static int reduce_scatter_phase(pg_handle_t *handle, void *buffer,
                                int total_count, DATATYPE datatype,
                                OPERATION operation,
                                const struct ibv_mr *buffer_mr, bool eager)
{
    size_t element_size = datatype_size(datatype);
    size_t maximum_chunk =
        chunk_count(total_count, handle->size, 0) * element_size;
    size_t block_count =
        (maximum_chunk + handle->pipeline_bytes - 1) /
        handle->pipeline_bytes;

    if (!block_count)
        block_count = 1;
    if (block_count > UINT16_MAX)
        return report_error("PG_PIPELINE_BYTES creates too many blocks");

    for (int step = 0; step < handle->size - 1; ++step) {
        int send_chunk =
            (handle->rank - step - 1 + handle->size) % handle->size;
        int receive_chunk =
            (handle->rank - step - 2 + 2 * handle->size) % handle->size;
        size_t send_bytes =
            chunk_count(total_count, handle->size, send_chunk) *
            element_size;
        size_t receive_bytes =
            chunk_count(total_count, handle->size, receive_chunk) *
            element_size;
        size_t send_base =
            chunk_offset(total_count, handle->size, send_chunk) *
            element_size;
        size_t receive_base =
            chunk_offset(total_count, handle->size, receive_chunk) *
            element_size;

        for (size_t block = 0; block < block_count; ++block) {
            size_t block_offset = block * handle->pipeline_bytes;
            size_t outgoing =
                block_offset < send_bytes
                    ? send_bytes - block_offset
                    : 0;
            size_t incoming =
                block_offset < receive_bytes
                    ? receive_bytes - block_offset
                    : 0;
            uint32_t tag =
                make_tag(PHASE_REDUCE_SCATTER, (unsigned)step,
                         (unsigned)block);
            bool send_completed = false;

            if (outgoing > handle->pipeline_bytes)
                outgoing = handle->pipeline_bytes;
            if (incoming > handle->pipeline_bytes)
                incoming = handle->pipeline_bytes;

            if (post_receive(handle, tag, handle->scratch, incoming,
                             handle->scratch_mr, eager) ||
                post_transfer(handle, tag,
                              outgoing
                                  ? (uint8_t *)buffer + send_base + block_offset
                                  : buffer,
                              outgoing, buffer_mr, eager,
                              handle->next_rx.scratch_addr,
                              handle->next_rx.scratch_rkey) ||
                wait_for_receive(handle, tag, &send_completed))
                return -1;

            /*
             * The outgoing transfer may still be active while the CPU
             * reduces the block that arrived from the previous rank.
             */
            if (incoming &&
                reduce_values((uint8_t *)buffer + receive_base + block_offset,
                              handle->scratch, incoming / element_size,
                              datatype, operation))
                return -1;

            if (!send_completed && wait_for_send(handle, tag))
                return -1;
        }
    }
    return 0;
}

static int all_gather_phase(pg_handle_t *handle, void *buffer,
                            int total_count, DATATYPE datatype,
                            const struct ibv_mr *buffer_mr, bool eager,
                            int owned_chunk)
{
    size_t element_size = datatype_size(datatype);
    size_t maximum_chunk =
        chunk_count(total_count, handle->size, 0) * element_size;
    size_t block_count =
        (maximum_chunk + handle->pipeline_bytes - 1) /
        handle->pipeline_bytes;
    uint64_t next_buffer_addr = 0;
    uint32_t next_buffer_rkey = 0;

    if (!block_count)
        block_count = 1;
    if (block_count > UINT16_MAX)
        return report_error("PG_PIPELINE_BYTES creates too many blocks");

    if (!eager &&
        exchange_next_mr(handle, buffer_mr, (uintptr_t)buffer,
                         &next_buffer_addr, &next_buffer_rkey))
        return -1;

    for (int step = 0; step < handle->size - 1; ++step) {
        int send_chunk =
            (owned_chunk - step + handle->size) % handle->size;
        int receive_chunk =
            (owned_chunk - step - 1 + handle->size) % handle->size;
        size_t send_bytes =
            chunk_count(total_count, handle->size, send_chunk) *
            element_size;
        size_t receive_bytes =
            chunk_count(total_count, handle->size, receive_chunk) *
            element_size;
        size_t send_base =
            chunk_offset(total_count, handle->size, send_chunk) *
            element_size;
        size_t receive_base =
            chunk_offset(total_count, handle->size, receive_chunk) *
            element_size;

        for (size_t block = 0; block < block_count; ++block) {
            size_t block_offset = block * handle->pipeline_bytes;
            size_t outgoing =
                block_offset < send_bytes
                    ? send_bytes - block_offset
                    : 0;
            size_t incoming =
                block_offset < receive_bytes
                    ? receive_bytes - block_offset
                    : 0;
            uint32_t tag =
                make_tag(PHASE_ALL_GATHER, (unsigned)step,
                         (unsigned)block);
            bool send_completed = false;

            if (outgoing > handle->pipeline_bytes)
                outgoing = handle->pipeline_bytes;
            if (incoming > handle->pipeline_bytes)
                incoming = handle->pipeline_bytes;

            if (post_receive(handle, tag,
                             incoming
                                 ? (uint8_t *)buffer + receive_base +
                                       block_offset
                                 : buffer,
                             incoming, buffer_mr, eager) ||
                post_transfer(handle, tag,
                              outgoing
                                  ? (uint8_t *)buffer + send_base + block_offset
                                  : buffer,
                              outgoing, buffer_mr, eager,
                              outgoing
                                  ? next_buffer_addr + send_base + block_offset
                                  : next_buffer_addr,
                              next_buffer_rkey) ||
                wait_for_receive(handle, tag, &send_completed))
                return -1;

            if (!send_completed && wait_for_send(handle, tag))
                return -1;
        }
    }
    return 0;
}

static struct ibv_mr *register_collective_buffer(pg_handle_t *handle,
                                                 void *buffer,
                                                 size_t bytes)
{
    struct ibv_mr *mr;

    if (!bytes)
        bytes = 1;
    mr = ibv_reg_mr(handle->pd, buffer, bytes,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!mr)
        report_errno("ibv_reg_mr(collective buffer)");
    return mr;
}

int pg_all_reduce(void *sendbuf, void *recvbuf, int count,
                  DATATYPE datatype, OPERATION op, void *pg_handle)
{
    pg_handle_t *handle = pg_handle;
    struct ibv_mr *recv_mr = NULL;
    size_t element_size = datatype_size(datatype);
    size_t bytes;
    bool eager;
    int owned_chunk;
    int result = -1;

    if (!handle || !sendbuf || !recvbuf || count < 0 || !element_size)
        return report_error("invalid pg_all_reduce arguments");
    if ((size_t)count > SIZE_MAX / element_size)
        return report_error("all-reduce size overflow");

    bytes = (size_t)count * element_size;
    eager = use_eager(handle, bytes);

    if (ring_barrier(handle))
        return -1;
    if (sendbuf != recvbuf && bytes)
        memcpy(recvbuf, sendbuf, bytes);

    recv_mr = register_collective_buffer(handle, recvbuf, bytes);
    if (!recv_mr)
        goto out;

    if (reduce_scatter_phase(handle, recvbuf, count, datatype, op,
                             recv_mr, eager))
        goto out;

    owned_chunk = handle->rank;
    if (all_gather_phase(handle, recvbuf, count, datatype, recv_mr,
                         eager, owned_chunk))
        goto out;

    result = 0;

out:
    if (recv_mr && ibv_dereg_mr(recv_mr)) {
        report_errno("ibv_dereg_mr");
        result = -1;
    }
    if (ring_barrier(handle))
        result = -1;
    return result;
}

int pg_reduce_scatter(void *sendbuf, void *recvbuf, int recv_count,
                      DATATYPE datatype, OPERATION op, void *pg_handle)
{
    pg_handle_t *handle = pg_handle;
    struct ibv_mr *work_mr = NULL;
    void *work = NULL;
    size_t element_size = datatype_size(datatype);
    size_t total_count;
    size_t bytes;
    bool eager;
    int owned_chunk;
    int result = -1;

    if (!handle || !sendbuf || !recvbuf || recv_count < 0 || !element_size)
        return report_error("invalid pg_reduce_scatter arguments");
    if ((size_t)recv_count > (size_t)INT32_MAX / (size_t)handle->size)
        return report_error("reduce-scatter count overflow");

    total_count = (size_t)recv_count * (size_t)handle->size;
    bytes = total_count * element_size;
    eager = use_eager(handle, bytes);

    if (ring_barrier(handle))
        return -1;
    work = malloc(bytes ? bytes : 1);
    if (!work) {
        report_errno("allocating reduce-scatter work buffer");
        goto out;
    }
    if (bytes)
        memcpy(work, sendbuf, bytes);

    work_mr = register_collective_buffer(handle, work, bytes);
    if (!work_mr)
        goto out;
    if (reduce_scatter_phase(handle, work, (int)total_count, datatype, op,
                             work_mr, eager))
        goto out;

    owned_chunk = handle->rank;
    if (recv_count)
        memcpy(recvbuf,
               (uint8_t *)work +
                   (size_t)owned_chunk * (size_t)recv_count * element_size,
               (size_t)recv_count * element_size);
    result = 0;

out:
    if (work_mr && ibv_dereg_mr(work_mr)) {
        report_errno("ibv_dereg_mr");
        result = -1;
    }
    free(work);
    if (ring_barrier(handle))
        result = -1;
    return result;
}

int pg_all_gather(void *sendbuf, void *recvbuf, int send_count,
                  DATATYPE datatype, void *pg_handle)
{
    pg_handle_t *handle = pg_handle;
    struct ibv_mr *recv_mr = NULL;
    size_t element_size = datatype_size(datatype);
    size_t total_count;
    size_t bytes;
    size_t local_bytes;
    bool eager;
    int result = -1;

    if (!handle || !sendbuf || !recvbuf || send_count < 0 || !element_size)
        return report_error("invalid pg_all_gather arguments");
    if ((size_t)send_count > (size_t)INT32_MAX / (size_t)handle->size)
        return report_error("all-gather count overflow");

    total_count = (size_t)send_count * (size_t)handle->size;
    local_bytes = (size_t)send_count * element_size;
    bytes = total_count * element_size;
    eager = use_eager(handle, bytes);

    if (ring_barrier(handle))
        return -1;
    if (local_bytes)
        memcpy((uint8_t *)recvbuf +
                   (size_t)handle->rank * local_bytes,
               sendbuf, local_bytes);

    recv_mr = register_collective_buffer(handle, recvbuf, bytes);
    if (!recv_mr)
        goto out;
    if (all_gather_phase(handle, recvbuf, (int)total_count, datatype,
                         recv_mr, eager, handle->rank))
        goto out;
    result = 0;

out:
    if (recv_mr && ibv_dereg_mr(recv_mr)) {
        report_errno("ibv_dereg_mr");
        result = -1;
    }
    if (ring_barrier(handle))
        result = -1;
    return result;
}

int pg_close(void *pg_handle)
{
    pg_handle_t *handle = pg_handle;
    int result = 0;

    if (!handle)
        return 0;

    if (handle->listen_fd >= 0)
        close(handle->listen_fd);
    if (handle->next_sock >= 0)
        close(handle->next_sock);
    if (handle->prev_sock >= 0)
        close(handle->prev_sock);
    if (handle->scratch_mr && ibv_dereg_mr(handle->scratch_mr))
        result = report_errno("ibv_dereg_mr(scratch)");
    free(handle->scratch);
    if (handle->tx_qp && ibv_destroy_qp(handle->tx_qp))
        result = report_errno("ibv_destroy_qp(tx)");
    if (handle->rx_qp && ibv_destroy_qp(handle->rx_qp))
        result = report_errno("ibv_destroy_qp(rx)");
    if (handle->cq && ibv_destroy_cq(handle->cq))
        result = report_errno("ibv_destroy_cq");
    if (handle->pd && ibv_dealloc_pd(handle->pd))
        result = report_errno("ibv_dealloc_pd");
    if (handle->context && ibv_close_device(handle->context))
        result = report_errno("ibv_close_device");
    free_hosts(handle);
    free(handle);
    return result;
}

typedef struct {
    int rank;
    const char *hosts;
    char *owned_hosts;
    int count;
    int iterations;
    DATATYPE datatype;
    OPERATION operation;
    const char *protocol;
} options_t;

static void usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s -myindex N -list h1 h2 [h3 h4] [options]\n"
            "   or: %s --rank N --hosts h0,h1[,h2,h3] [options]\n"
            "Options:\n"
            "  --count N             Elements per all-reduce (default 1048576)\n"
            "  --iterations N        Timed iterations (default 20)\n"
            "  --datatype TYPE       int, float, or double (default int)\n"
            "  --op OPERATION        sum, prod, max, or min (default sum)\n"
            "  --protocol PROTOCOL   eager, rendezvous, or auto (default auto)\n",
            program, program);
}

static int parse_int(const char *text, int minimum, int *value)
{
    char *end = NULL;
    long parsed;

    errno = 0;
    parsed = strtol(text, &end, 10);
    if (errno || !end || *end || parsed < minimum || parsed > INT32_MAX)
        return -1;
    *value = (int)parsed;
    return 0;
}

static int parse_host_list(int argc, char **argv, int *index,
                           options_t *options)
{
    int first = *index + 1;
    int end = first;
    size_t length = 1;
    char *hosts;
    char *cursor;

    while (end < argc && argv[end][0] != '-') {
        length += strlen(argv[end]) + 1;
        ++end;
    }
    if (end == first)
        return -1;

    hosts = malloc(length);
    if (!hosts)
        return -1;

    cursor = hosts;
    for (int i = first; i < end; ++i) {
        size_t host_length = strlen(argv[i]);
        memcpy(cursor, argv[i], host_length);
        cursor += host_length;
        *cursor++ = i + 1 < end ? ',' : '\0';
    }

    free(options->owned_hosts);
    options->owned_hosts = hosts;
    options->hosts = hosts;
    *index = end - 1;
    return 0;
}

static int parse_options(int argc, char **argv, options_t *options)
{
    *options = (options_t){
        .rank = -1,
        .count = 1024 * 1024,
        .iterations = 20,
        .datatype = DATATYPE_INT,
        .operation = OP_SUM,
        .protocol = "auto"
    };

    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--rank") && i + 1 < argc) {
            if (parse_int(argv[++i], 0, &options->rank))
                return -1;
        } else if (!strcmp(argv[i], "-myindex") && i + 1 < argc) {
            if (parse_int(argv[++i], 1, &options->rank))
                return -1;
            --options->rank;
        } else if (!strcmp(argv[i], "--hosts") && i + 1 < argc) {
            free(options->owned_hosts);
            options->owned_hosts = NULL;
            options->hosts = argv[++i];
        } else if (!strcmp(argv[i], "-list")) {
            if (parse_host_list(argc, argv, &i, options))
                return -1;
        } else if (!strcmp(argv[i], "--count") && i + 1 < argc) {
            if (parse_int(argv[++i], 0, &options->count))
                return -1;
        } else if (!strcmp(argv[i], "--iterations") && i + 1 < argc) {
            if (parse_int(argv[++i], 1, &options->iterations))
                return -1;
        } else if (!strcmp(argv[i], "--datatype") && i + 1 < argc) {
            const char *value = argv[++i];
            if (!strcmp(value, "int"))
                options->datatype = DATATYPE_INT;
            else if (!strcmp(value, "float"))
                options->datatype = DATATYPE_FLOAT;
            else if (!strcmp(value, "double"))
                options->datatype = DATATYPE_DOUBLE;
            else
                return -1;
        } else if (!strcmp(argv[i], "--op") && i + 1 < argc) {
            const char *value = argv[++i];
            if (!strcmp(value, "sum"))
                options->operation = OP_SUM;
            else if (!strcmp(value, "prod"))
                options->operation = OP_PROD;
            else if (!strcmp(value, "max"))
                options->operation = OP_MAX;
            else if (!strcmp(value, "min"))
                options->operation = OP_MIN;
            else
                return -1;
        } else if (!strcmp(argv[i], "--protocol") && i + 1 < argc) {
            options->protocol = argv[++i];
            if (strcmp(options->protocol, "auto") &&
                strcmp(options->protocol, "eager") &&
                strcmp(options->protocol, "rendezvous"))
                return -1;
        } else {
            return -1;
        }
    }

    return options->rank < 0 || !options->hosts ? -1 : 0;
}

static int host_count(const char *hosts)
{
    int count = 1;
    for (const char *p = hosts; *p; ++p)
        count += (*p == ',');
    return count;
}

static size_t benchmark_type_size(DATATYPE datatype)
{
    if (datatype == DATATYPE_INT)
        return sizeof(int);
    if (datatype == DATATYPE_FLOAT)
        return sizeof(float);
    return sizeof(double);
}

static void initialize_input(void *buffer, int count, DATATYPE datatype,
                             int rank)
{
    if (datatype == DATATYPE_INT) {
        int *values = buffer;
        for (int i = 0; i < count; ++i)
            values[i] = rank + 1;
    } else if (datatype == DATATYPE_FLOAT) {
        float *values = buffer;
        for (int i = 0; i < count; ++i)
            values[i] = (float)(rank + 1);
    } else {
        double *values = buffer;
        for (int i = 0; i < count; ++i)
            values[i] = (double)(rank + 1);
    }
}

static double expected_value(int size, OPERATION operation)
{
    double value;

    if (operation == OP_SUM)
        return (double)size * (double)(size + 1) / 2.0;
    if (operation == OP_MAX)
        return (double)size;
    if (operation == OP_MIN)
        return 1.0;

    value = 1.0;
    for (int rank = 1; rank <= size; ++rank)
        value *= rank;
    return value;
}

static int verify_result(const void *buffer, int count, DATATYPE datatype,
                         double expected)
{
    for (int i = 0; i < count; ++i) {
        double actual;
        if (datatype == DATATYPE_INT)
            actual = ((const int *)buffer)[i];
        else if (datatype == DATATYPE_FLOAT)
            actual = ((const float *)buffer)[i];
        else
            actual = ((const double *)buffer)[i];

        if (fabs(actual - expected) >
            1e-5 * (fabs(expected) > 1.0 ? fabs(expected) : 1.0)) {
            fprintf(stderr,
                    "verification failed at element %d: got %.10g, expected %.10g\n",
                    i, actual, expected);
            return -1;
        }
    }
    return 0;
}

static double elapsed_seconds(const struct timespec *start,
                              const struct timespec *end)
{
    return (double)(end->tv_sec - start->tv_sec) +
           (double)(end->tv_nsec - start->tv_nsec) / 1e9;
}

int main(int argc, char **argv)
{
    options_t options;
    void *handle = NULL;
    void *send_buffer = NULL;
    void *receive_buffer = NULL;
    struct timespec start;
    struct timespec end;
    size_t bytes;
    int size;
    int result = EXIT_FAILURE;

    if (parse_options(argc, argv, &options)) {
        usage(argv[0]);
        free(options.owned_hosts);
        return EXIT_FAILURE;
    }

    size = host_count(options.hosts);
    if (options.rank >= size) {
        fprintf(stderr, "rank %d is outside a %d-process host list\n",
                options.rank, size);
        return EXIT_FAILURE;
    }

    bytes = (size_t)options.count *
            benchmark_type_size(options.datatype);
    send_buffer = malloc(bytes ? bytes : 1);
    receive_buffer = malloc(bytes ? bytes : 1);
    if (!send_buffer || !receive_buffer) {
        perror("malloc");
        goto out;
    }

    initialize_input(send_buffer, options.count, options.datatype,
                     options.rank);
    {
        char rank_text[32];
        snprintf(rank_text, sizeof(rank_text), "%d", options.rank);
        setenv("PG_RANK", rank_text, 1);
    }
    setenv("PG_PROTOCOL", options.protocol, 1);

    if (connect_process_group((char *)options.hosts, &handle))
        goto out;

    if (pg_all_reduce(send_buffer, receive_buffer, options.count,
                      options.datatype, options.operation, handle))
        goto out;

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int iteration = 0; iteration < options.iterations; ++iteration) {
        initialize_input(send_buffer, options.count, options.datatype,
                         options.rank);
        if (pg_all_reduce(send_buffer, receive_buffer, options.count,
                          options.datatype, options.operation, handle))
            goto out;
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    if (verify_result(receive_buffer, options.count, options.datatype,
                      expected_value(size, options.operation)))
        goto out;

    if (options.rank == 0) {
        double average_us =
            elapsed_seconds(&start, &end) * 1e6 / options.iterations;
        printf("ranks=%d count=%d bytes=%zu protocol=%s iterations=%d "
               "average=%.3f us\n",
               size, options.count, bytes, options.protocol,
               options.iterations, average_us);
    }
    result = EXIT_SUCCESS;

out:
    pg_close(handle);
    free(receive_buffer);
    free(send_buffer);
    free(options.owned_hosts);
    return result;
}
