#include "allreduce.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

static bool run_test(const char *protocol,
                     int rank,
                     int num_processes,
                     void *pg_handle)
{
    if (setenv("ALLREDUCE_PROTOCOL", protocol, 1) != 0)
    {
        std::cerr << "Failed to select protocol\n";
        return false;
    }

    const int count = num_processes * 65536;
    const int32_t expected = num_processes * (num_processes + 1) / 2;

    std::vector<int32_t> send_buffer(count, rank + 1);
    std::vector<int32_t> recv_buffer(count, 0);

    const std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();

    const int rc = pg_all_reduce(send_buffer.data(),
                                 recv_buffer.data(),
                                 count,
                                 TYPE_INT32,
                                 OP_SUM,
                                 pg_handle);

    const std::chrono::steady_clock::time_point end =
        std::chrono::steady_clock::now();

    if (rc != 0)
    {
        std::cerr << "Rank " << rank << ": " << protocol
                  << " All-Reduce failed\n";
        return false;
    }

    for (int i = 0; i < count; ++i)
    {
        if (recv_buffer[i] != expected)
        {
            std::cerr << "Rank " << rank << ": " << protocol
                      << " produced an incorrect result at index " << i
                      << ": expected " << expected
                      << ", received " << recv_buffer[i] << '\n';
            return false;
        }
    }

    if (rank == 0)
    {
        const double milliseconds =
            std::chrono::duration<double, std::milli>(end - start).count();

        std::cout << protocol << ": PASS, " << milliseconds << " ms\n";
    }

    return true;
}

int main(int argc, char **argv)
{
    if (argc < 6 ||
        std::strcmp(argv[1], "-myindex") != 0 ||
        std::strcmp(argv[3], "-list") != 0)
    {
        std::cerr << "Usage: " << argv[0]
                  << " -myindex INDEX -list HOST1 HOST2 [HOST3 ...]\n";
        return 1;
    }

    char *end = NULL;
    const long one_based_rank = std::strtol(argv[2], &end, 10);
    const int num_processes = argc - 4;

    if (*end != '\0' ||
        one_based_rank < 1 ||
        one_based_rank > num_processes)
    {
        std::cerr << "Invalid process index\n";
        return 1;
    }

    const int rank = static_cast<int>(one_based_rank - 1);

    std::ostringstream config_stream;
    for (int i = 1; i < argc; ++i)
    {
        if (i > 1)
            config_stream << ' ';
        config_stream << argv[i];
    }

    std::string config = config_stream.str();
    void *pg_handle = NULL;

    if (connect_process_group(&config[0], &pg_handle) != 0)
    {
        std::cerr << "Failed to connect process group\n";
        return 1;
    }

    const bool eager_ok =
        run_test("eager", rank, num_processes, pg_handle);
    const bool rendezvous_ok =
        run_test("rendezvous", rank, num_processes, pg_handle);

    const int close_rc = pg_close(pg_handle);

    if (!eager_ok || !rendezvous_ok || close_rc != 0)
        return 1;

    return 0;
}
