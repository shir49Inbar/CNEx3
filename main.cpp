#include "allreduce.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

struct benchmark_result
{
    bool passed;
    double average_ms;
    double minimum_ms;
};

static bool verify_result(const std::vector<int32_t> &buffer,
                          int count,
                          int32_t expected)
{
    for (int i = 0; i < count; ++i)
    {
        if (buffer[i] != expected)
        {
            std::cerr << "Incorrect result at index " << i
                      << ": expected " << expected
                      << ", received " << buffer[i] << '\n';
            return false;
        }
    }

    return true;
}

static benchmark_result run_benchmark(const char *protocol,
                                      int rank,
                                      int num_processes,
                                      int count,
                                      int iterations,
                                      std::vector<int32_t> &send_buffer,
                                      std::vector<int32_t> &recv_buffer,
                                      void *pg_handle)
{
    benchmark_result result = {false, 0.0, 0.0};

    if (setenv("ALLREDUCE_PROTOCOL", protocol, 1) != 0)
    {
        std::cerr << "Failed to select protocol\n";
        return result;
    }

    const int32_t expected = num_processes * (num_processes + 1) / 2;

    std::fill(send_buffer.begin(), send_buffer.begin() + count, rank + 1);
    std::fill(recv_buffer.begin(), recv_buffer.begin() + count, 0);

    if (pg_all_reduce(send_buffer.data(),
                      recv_buffer.data(),
                      count,
                      TYPE_INT32,
                      OP_SUM,
                      pg_handle) != 0)
    {
        std::cerr << "Rank " << rank << ": " << protocol
                  << " warm-up failed\n";
        return result;
    }

    if (!verify_result(recv_buffer, count, expected))
        return result;

    double total_ms = 0.0;
    double minimum_ms = std::numeric_limits<double>::max();

    for (int iteration = 0; iteration < iterations; ++iteration)
    {
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
            return result;
        }

        if (!verify_result(recv_buffer, count, expected))
            return result;

        const double milliseconds =
            std::chrono::duration<double, std::milli>(end - start).count();

        total_ms += milliseconds;
        if (milliseconds < minimum_ms)
            minimum_ms = milliseconds;
    }

    result.passed = true;
    result.average_ms = total_ms / iterations;
    result.minimum_ms = minimum_ms;
    return result;
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

    const int iterations = 5;
    const size_t maximum_bytes = 1024 * 1024;
    const size_t minimum_bytes =
        static_cast<size_t>(num_processes) * sizeof(int32_t);
    const int maximum_count =
        static_cast<int>(maximum_bytes / sizeof(int32_t));

    std::vector<int32_t> send_buffer(maximum_count);
    std::vector<int32_t> recv_buffer(maximum_count);

    if (rank == 0)
    {
        std::cout << "Ring All-Reduce benchmark\n"
                  << "Processes: " << num_processes << '\n'
                  << "Operation: INT32 SUM\n"
                  << "Measured iterations: " << iterations << "\n\n"
                  << std::left
                  << std::setw(12) << "Bytes"
                  << std::setw(16) << "Eager avg ms"
                  << std::setw(16) << "Eager min ms"
                  << std::setw(20) << "Rendezvous avg ms"
                  << std::setw(20) << "Rendezvous min ms"
                  << std::setw(12) << "Speedup"
                  << "Result\n";
    }

    bool all_passed = true;

    for (size_t message_bytes = minimum_bytes;
         message_bytes <= maximum_bytes;
         message_bytes *= 2)
    {
        const int count =
            static_cast<int>(message_bytes / sizeof(int32_t));

        const benchmark_result eager =
            run_benchmark("eager",
                          rank,
                          num_processes,
                          count,
                          iterations,
                          send_buffer,
                          recv_buffer,
                          pg_handle);

        const benchmark_result rendezvous =
            run_benchmark("rendezvous",
                          rank,
                          num_processes,
                          count,
                          iterations,
                          send_buffer,
                          recv_buffer,
                          pg_handle);

        const bool passed = eager.passed && rendezvous.passed;
        all_passed = all_passed && passed;

        if (rank == 0)
        {
            const double speedup =
                rendezvous.average_ms > 0.0
                    ? eager.average_ms / rendezvous.average_ms
                    : 0.0;

            std::cout << std::left
                      << std::setw(12) << message_bytes
                      << std::setw(16) << std::fixed << std::setprecision(4)
                      << eager.average_ms
                      << std::setw(16) << eager.minimum_ms
                      << std::setw(20) << rendezvous.average_ms
                      << std::setw(20) << rendezvous.minimum_ms
                      << std::setw(12) << speedup
                      << (passed ? "PASS" : "FAIL") << '\n';
        }

        if (!passed)
            break;

        if (message_bytes > maximum_bytes / 2)
            break;
    }

    const int close_rc = pg_close(pg_handle);

    if (!all_passed || close_rc != 0)
        return 1;

    return 0;
}
