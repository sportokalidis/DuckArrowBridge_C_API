#ifndef TASK_EXECUTOR_HPP
#define TASK_EXECUTOR_HPP

#include <string>
#include <memory>
#include "thread_pool.hpp"
#include <arrow/api.h>
#include <duckdb.h>
#include <arrow/python/pyarrow.h>

enum class ResultType {
    PARQUET,
    IN_MEMORY
};

class TaskExecutor {
public:
    TaskExecutor(const std::string& query, const std::string& outputPath, int maxThreads, ResultType resultType, size_t memoryLimit = 0);
    std::shared_ptr<arrow::Table> run(); // Runs the task and returns an Arrow Table if in-memory is chosen

private:
    std::string query;
    std::string outputPath;
    int maxThreads;
    ResultType resultType;
    size_t memoryLimit; // Memory limit in bytes
    static void WriteParquetFile(const std::shared_ptr<arrow::Table>& table, const std::string& filepath);
    void writeBatchesToParquet(const std::vector<duckdb_arrow_array>& batches, const std::shared_ptr<arrow::Schema>& schema, int fileCounter);
};

#endif // TASK_EXECUTOR_HPP
