#ifndef DATA_PROCESSOR_HPP
#define DATA_PROCESSOR_HPP

#include <string>
#include <memory>
#include "duckdb.h"  // Include DuckDB C API header
#include <arrow/api.h>

#ifdef _WIN32
    #ifdef BUILD_DLL
        #define DLL_EXPORT __declspec(dllexport)
    #else
        #define DLL_EXPORT __declspec(dllimport)
    #endif
#else
    #define DLL_EXPORT
#endif

class DLL_EXPORT DataProcessor {
public:
    DataProcessor();
    void loadParquet(const std::string& filepath);
    std::shared_ptr<arrow::Table> process(const std::string& filepath);
    static void WriteParquetFile(const std::shared_ptr<arrow::Table>& table, const std::string& filepath);
private:
    duckdb_database db;
    duckdb_connection conn;
};

#endif // DATA_PROCESSOR_HPP
