#ifndef ALLREDUCE_H
#define ALLREDUCE_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum DATATYPE
{
    TYPE_INT32,
    TYPE_FP64
} DATATYPE;

typedef enum OPERATION
{
    OP_SUM,
    OP_PRODUCT
} OPERATION;

int connect_process_group(char *servername, void **pg_handle);

int pg_all_reduce(void *send_buf,
                  void *recv_buf,
                  int count,
                  DATATYPE datatype,
                  OPERATION op,
                  void *pg_handle);

int pg_close(void *pg_handle);

#ifdef __cplusplus
}
#endif

#endif
