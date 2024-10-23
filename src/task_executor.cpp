#include "task_executor.hpp"
#include "data_processor.hpp" // Include for WriteParquetFile
#include <duckdb.h>
#include <arrow/c/bridge.h>
#include <iostream>
#include <chrono>
#include <arrow/c/helpers.h>

#include <arrow/c/bridge.h>  // For converting DuckDB Arrow to Arrow C++
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/filesystem/localfs.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <arrow/c/helpers.h>
#include <chrono>
#include "task_executor.hpp"

TaskExecutor::TaskExecutor(const std::string& query, const std::string& outputPath, int maxThreads, ResultType resultType, size_t memoryLimit)
    : query(query), outputPath(outputPath), maxThreads(maxThreads), resultType(resultType), memoryLimit(memoryLimit) {}

std::shared_ptr<arrow::Table> TaskExecutor::run() {
    // Initialize the thread pool
    ThreadPool pool(maxThreads);
    duckdb_database db;
    duckdb_connection conn;
    duckdb_arrow result;

    // Assume DataProcessor has methods for DuckDB initialization
    if (duckdb_open(NULL, &db) != DuckDBSuccess || duckdb_connect(db, &conn) != DuckDBSuccess) {
        std::cerr << "Failed to initialize DuckDB" << std::endl;
        return nullptr;
    }

    // Execute the query
    if (duckdb_query_arrow(conn, query.c_str(), &result) != DuckDBSuccess) {
        std::cerr << "Error executing query and retrieving Arrow result" << std::endl;
        return nullptr;
    }

    // Get the schema
    auto arrow_schema = static_cast<duckdb_arrow_schema>(malloc(sizeof(struct ArrowSchema)));
    if (!arrow_schema || duckdb_query_arrow_schema(result, &arrow_schema) != DuckDBSuccess) {
        std::cerr << "Error retrieving Arrow schema" << std::endl;
        duckdb_destroy_arrow(&result);
        return nullptr;
    }
    std::shared_ptr<arrow::Schema> schema = arrow::ImportSchema(reinterpret_cast<struct ArrowSchema*>(arrow_schema)).ValueOrDie();
    free(arrow_schema);

    // std::vector<duckdb_arrow_array> batches;
    int fileCounter = 0;
    int64_t totalRows = 0;
    int64_t total_rows = 0;
    int file_counter = 0;
    std::vector<std::shared_ptr<arrow::RecordBatch>> recordBatches;
    std::vector<duckdb_arrow_array> batches;

    while (true) {
        auto arrow_array = static_cast<duckdb_arrow_array>(malloc(sizeof(struct ArrowArray)));
        if (!arrow_array) {
            std::cerr << "Failed to allocate memory for Arrow array" << std::endl;
            break;
        }

        if (duckdb_query_arrow_array(result, &arrow_array) != DuckDBSuccess) {
            free(arrow_array);
            break;
        }

        // Check if the Arrow array is released (empty)
        if (ArrowArrayIsReleased(reinterpret_cast<struct ArrowArray*>(arrow_array))) {
            std::cout << "Reached the end of data (empty Arrow array). Exiting loop." << std::endl;
            std::vector<duckdb_arrow_array> batches_to_write = std::move(batches);
            int current_file_counter = file_counter++;

            std::cout << batches_to_write.empty() << std::endl;
            // Enqueue a task in the thread pool to write the parquet file
            if (!batches_to_write.empty()) {
                pool.enqueue([batches_to_write, schema, current_file_counter, this] {
                    auto start = std::chrono::high_resolution_clock::now();

                    std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
                    for (auto& arrow_array : batches_to_write) {
                        // Convert duckdb_arrow_array to arrow::RecordBatch
                        std::shared_ptr<arrow::RecordBatch> record_batch = arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(arrow_array), schema).ValueOrDie();
                        record_batches.push_back(record_batch);

                        // Release the Arrow array after conversion
                        //reinterpret_cast<struct ArrowArray*>(arrow_array)->release(reinterpret_cast<struct ArrowArray*>(arrow_array));
                        //free(arrow_array);  // Free the memory
                    }
                    record_batches.clear();
                    auto arrow_table = arrow::Table::FromRecordBatches(schema, record_batches).ValueOrDie();
                    std::string output_file = "./output_parquet/output_part_" + std::to_string(current_file_counter) + ".parquet";
                    TaskExecutor::WriteParquetFile(arrow_table, output_file);

                    auto end = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double> elapsed = end - start;
                    std::cout << "Data ||saved to " << output_file << "  , Time: " << elapsed.count() << " seconds " << std::endl;
                    });
            }

            // Reset the batches and total rows
            batches.clear();
            total_rows = 0;
            break;
        }

        // Add non-empty arrow_array to the batches
        if (!ArrowArrayIsReleased(reinterpret_cast<struct ArrowArray*>(arrow_array)) && reinterpret_cast<struct ArrowArray*>(arrow_array)->length > 0 && reinterpret_cast<struct ArrowArray*>(arrow_array)->length <= 2048) {
            batches.push_back(arrow_array);
            total_rows += reinterpret_cast<struct ArrowArray*>(arrow_array)->length;
            std::cout << total_rows << std::endl;
        }
        if (total_rows >= 100000) {
            // Capture the current batches and file counter to pass to the thread
            std::vector<duckdb_arrow_array> batches_to_write = std::move(batches);
            int current_file_counter = file_counter++;

            // Enqueue a task in the thread pool to write the parquet file
            pool.enqueue([batches_to_write, schema, current_file_counter, this] {
                auto start = std::chrono::high_resolution_clock::now();

                std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
                for (auto& arrow_array : batches_to_write) {
                    // Convert duckdb_arrow_array to arrow::RecordBatch
                    std::shared_ptr<arrow::RecordBatch> record_batch = arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(arrow_array), schema).ValueOrDie();
                    record_batches.push_back(record_batch);
                }

                auto arrow_table = arrow::Table::FromRecordBatches(schema, record_batches).ValueOrDie();
                std::string output_file = "./output_parquet/output_part_" + std::to_string(current_file_counter) + ".parquet";
                TaskExecutor::WriteParquetFile(arrow_table, output_file);

                auto end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double> elapsed = end - start;
                std::cout << "Data saved to " << output_file << "  , Time: " << elapsed.count() << " seconds " << std::endl;
                });

            // Reset the batches and total rows
            batches.clear();
            total_rows = 0;
        }
    }

    free(arrow_schema);
    duckdb_destroy_arrow(&result);

    return nullptr;
}

void TaskExecutor::WriteParquetFile(const std::shared_ptr<arrow::Table>& table, const std::string& filepath) {
    auto start = std::chrono::high_resolution_clock::now();

    // Open file output stream
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(filepath)
    );

    // Set the Parquet writer properties
    std::shared_ptr<parquet::WriterProperties> writer_properties = parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_properties = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    // Create a Parquet file writer
    std::unique_ptr<parquet::arrow::FileWriter> parquet_writer;
    PARQUET_ASSIGN_OR_THROW(
        parquet_writer,
        parquet::arrow::FileWriter::Open(
            *table->schema().get(),                           // schema of the table
            arrow::default_memory_pool(),               // memory pool 
            outfile,                                    // output stream
            writer_properties,                          // writer properties
            arrow_properties                            // arrow writer properties
        )
    );

    // Write the Arrow table to the Parquet file
    PARQUET_THROW_NOT_OK(parquet_writer->WriteTable(*table, table->num_rows()));

    // Finalize and close the writer
    PARQUET_THROW_NOT_OK(parquet_writer->Close());
    PARQUET_THROW_NOT_OK(outfile->Close());

    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed = end - start;
    //std::cout << "Data saved to " << filepath << "  , Time: " << elapsed.count() << " seconds " << std::endl;
}