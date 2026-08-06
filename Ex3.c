#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <infiniband/verbs.h>
#include <arpa/inet.h>

typedef struct
{
    uint32_t qpn;
    uint16_t lid;
    uint32_t rkey;
    uint64_t vaddr;
} oob_data_t;

// info between QPs
typedef struct
{
    uint32_t qpn;
    uint16_t lid;
    uint32_t psn;
} qp_info_t;

typedef enum
{
    DATATYPE_INT,
    DATATYPE_FLOAT,
    DATATYPE_DOUBLE
} DATATYPE;

typedef enum
{
    OP_SUM,
    OP_PROD,
    OP_MAX,
    OP_MIN
} OPERATION;

typedef struct
{
    int my_index;
    int num_processes;

    struct ibv_context *context;
    struct ibv_pd *pd;
    struct ibv_cq *cq;

    struct ibv_qp *qp_send; // connection to the next neighbor
    struct ibv_qp *qp_recv; // connection to the previous neighbor

    // info on neighbors
    qp_info_t remote_send_info;
    qp_info_t remote_recv_info;

    int ib_port;
} pg_handle_t;

int exchange_oob_data(const char *next_node_ip, int next_node_port, int my_listen_port, oob_data_t *local_data, oob_data_t *remote_data)
{
    int server_fd, sock = -1;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    // Opening the server to listen to the previous neighbor
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(my_listen_port);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, 3);

    // Connecting to the next neighbor
    struct sockaddr_in serv_addr;
    int client_sock = socket(AF_INET, SOCK_STREAM, 0);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(next_node_port);
    inet_pton(AF_INET, next_node_ip, &serv_addr.sin_addr);

    while (connect(client_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        usleep(100000);
    }

    sock = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen);

    send(client_sock, local_data, sizeof(oob_data_t), 0);
    recv(sock, remote_data, sizeof(oob_data_t), MSG_WAITALL);

    close(client_sock);
    close(sock);
    close(server_fd);

    return 0;
}

int connect_process_group(char *servername, void **pg_handle)
{
    pg_handle_t *handle = (pg_handle_t *)calloc(1, sizeof(pg_handle_t));
    if (!handle)
        return -1;

    handle->ib_port = 1;

    // getting RDMA devices
    int num_devices;
    struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list || num_devices == 0)
        return -1;

    handle->context = ibv_open_device(dev_list[0]);
    ibv_free_device_list(dev_list);
    if (!handle->context)
        return -1;

    // Allocating Protection domain and Completion queue
    handle->pd = ibv_alloc_pd(handle->context);
    handle->cq = ibv_create_cq(handle->context, 100, NULL, NULL, 0);

    // Creating Queue-Pairs (one for send, one for receive)
    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = handle->cq,
        .recv_cq = handle->cq,
        .cap = {
            .max_send_wr = 10,
            .max_recv_wr = 10,
            .max_send_sge = 1,
            .max_recv_sge = 1},
        .qp_type = IBV_QPT_RC};

    handle->qp_send = ibv_create_qp(handle->pd, &qp_init_attr);
    handle->qp_recv = ibv_create_qp(handle->pd, &qp_init_attr);

    // Critical stage
    struct ibv_port_attr port_attr;
    ibv_query_port(handle->context, handle->ib_port, &port_attr);
    uint16_t my_lid = port_attr.lid;

    // switching two of the QPs into INIT
    modify_qp_to_init(handle->qp_send, handle->ib_port);
    modify_qp_to_init(handle->qp_recv, handle->ib_port);

    // Preparing data to send
    oob_data_t my_data_to_next;
    my_data_to_next.qpn = handle->qp_recv->qp_num;
    my_data_to_next.lid = my_lid;

    oob_data_t next_neighbor_data;

    // Switching OOB
    int my_listen_port = 5000 + handle->my_index;
    int next_node_port = 5000 + ((handle->my_index + 1) % handle->num_processes);

    exchange_oob_data(next_node_ip, next_node_port, my_listen_port, &my_data_to_next, &next_neighbor_data);

    modify_qp_to_rtr(handle->qp_send, next_neighbor_data.qpn, next_neighbor_data.lid, handle->ib_port);
    modify_qp_to_rts(handle->qp_send);

    return 0;
}

int pg_all_reduce(void *send_buf, void *recv_buf, int count, DATATYPE datatype, OPERATION op, void *pg_handle)
{
    pg_handle_t *handle = (pg_handle_t *)pg_handle;
    size_t data_size = count * sizeof(int);

    // Registering the memory for RDMA
    struct ibv_mr *mr_send = ibv_reg_mr(handle->pd, send_buf, data_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    struct ibv_mr *mr_recv = ibv_reg_mr(handle->pd, recv_buf, data_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

    // Replacing the R_Key
    uint32_t next_neighbor_rkey = ;  // the key from the next neighbor;
    uint64_t next_neighbor_vaddr = ; // neighbor address

    memcpy(recv_buf, send_buf, data_size); // copying the data into a buffer

    int num_steps = handle->num_processes - 1;
    int chunk_size = count / handle->num_processes;
    int remainder = count % handle->num_processes;
    int my_chunk_size = chunk_size + (handle->my_index < remainder ? 1 : 0);

    // Reduce-scatter
    for (int step = 0; step < num_steps; step++)
    {
        // calculating the index of current segment to send and compute
        int send_chunk_idx = (handle->my_index - step + handle->num_processes) % handle->num_processes;
        int recv_chunk_idx = (handle->my_index - step - 1 + handle->num_processes) % handle->num_processes;

        // sending current segment to the next neighbor
        struct ibv_sge sge;
        sge.addr = (uint64_t)((int *)recv_buf + send_chunk_idx * chunk_size);
        sge.length = chunk_size * sizeof(int);
        sge.lkey = mr_recv->lkey;

        struct ibv_send_wr wr = {}, *bad_wr = NULL;
        wr.wr_id = step;
        wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.imm_data = htonl(step); // sending the step number as an indication
        wr.wr.rdma.remote_addr = next_neighbor_vaddr + (send_chunk_idx * chunk_size * sizeof(int));
        wr.wr.rdma.rkey = next_neighbor_rkey;

        ibv_post_send(handle->qp_send, &wr, &bad_wr);

        // Waiting for the message of the previous neighbor
        struct ibv_recv_wr rwr = {}, *bad_rwr = NULL;
        ibv_post_recv(handle->qp_recv, &rwr, &bad_rwr);
        poll_completion(handle->cq); // Waiting for a signal from the neighbor
        poll_completion(handle->cq); // Waiting for the completion of our send

        // Reduce
        int *local_data = (int *)send_buf + recv_chunk_idx * chunk_size;
        int *received_data = (int *)recv_buf + recv_chunk_idx * chunk_size;

        for (int i = 0; i < chunk_size; i++)
        {
            if (op == OP_SUM)
            {
                received_data[i] += local_data[i];
            }
        }
    }
    // All-gather
    for (int step = 0; step < num_steps; step++)
    {
        // TODO
    }

    ibv_dereg_mr(mr_send);
    ibv_dereg_mr(mr_recv);

    return 0;
}

int poll_completion(struct ibv_cq *cq)
{
    struct ibv_wc wc;
    int ne;
    do
    {
        ne = ibv_poll_cq(cq, 1, &wc);
    } while (ne == 0);

    if (ne < 0 || wc.status != IBV_WC_SUCCESS)
    {
        fprintf(stderr, "CQ polling failed with status %d\n", wc.status);
        return -1;
    }
    return wc.imm_data; // return the immediate data for indication
}

int pg_close(void *pg_handle)
{
    if (!pg_handle)
        return -1;
    pg_handle_t *handle = (pg_handle_t *)pg_handle;

    if (handle->qp_send)
        ibv_destroy_qp(handle->qp_send);
    if (handle->qp_recv)
        ibv_destroy_qp(handle->qp_recv);
    if (handle->cq)
        ibv_destroy_cq(handle->cq);
    if (handle->pd)
        ibv_dealloc_pd(handle->pd) גג;
    if (handle->context)
        ibv_close_device(handle->context);

    free(handle);
    return 0;
}