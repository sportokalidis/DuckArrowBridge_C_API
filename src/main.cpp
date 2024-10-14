#include <iostream>
#include <string>
#include "data_processor.hpp"
#include <chrono>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <iostream>


// Function to print the data in an Apache Arrow Table
void PrintArrowTable(const std::shared_ptr<arrow::Table>& table) {
    if (!table) {
        std::cerr << "The table is empty or invalid." << std::endl;
        return;
    }

    // Print the number of rows in the table
    std::cout << "Total rows: " << table->num_rows() << std::endl;

    // Print column names using the table's schema
    for (int i = 0; i < table->num_columns(); ++i) {
        std::cout << table->schema()->field(i)->name() << "\t";
    }
    std::cout << std::endl;

    // Iterate over each row in the table
    for (int64_t row_idx = 0; row_idx < table->num_rows(); ++row_idx) {
        // For each column in the row
        for (int col_idx = 0; col_idx < table->num_columns(); ++col_idx) {
            auto column = table->column(col_idx);

            // Find the correct chunk and the corresponding row within that chunk
            int chunk_idx = 0;
            int64_t local_row_idx = row_idx;

            // Find the chunk that contains this row (since the table is split into chunks)
            while (local_row_idx >= column->chunk(chunk_idx)->length()) {
                local_row_idx -= column->chunk(chunk_idx)->length();
                chunk_idx++;
            }

            // Now `local_row_idx` refers to the row within the correct chunk
            auto array = column->chunk(chunk_idx);

            // Print the value in the correct row for each column, handling multiple types
            switch (array->type_id()) {
                case arrow::Type::INT32: {
                    auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    if (int_array->IsNull(local_row_idx)) {
                        std::cout << "NULL";
                    } else {
                        std::cout << int_array->Value(local_row_idx);
                    }
                    break;
                }
                case arrow::Type::STRING: {
                    auto string_array = std::static_pointer_cast<arrow::StringArray>(array);
                    if (string_array->IsNull(local_row_idx)) {
                        std::cout << "NULL";
                    } else {
                        std::cout << string_array->GetString(local_row_idx);
                    }
                    break;
                }
                case arrow::Type::FLOAT: {
                    auto float_array = std::static_pointer_cast<arrow::FloatArray>(array);
                    if (float_array->IsNull(local_row_idx)) {
                        std::cout << "NULL";
                    } else {
                        std::cout << float_array->Value(local_row_idx);
                    }
                    break;
                }
                default:
                    std::cout << "Unsupported type";
                    break;
            }
            std::cout << "\t";
        }
        std::cout << std::endl;
    }
}




int main(int argc, char* argv[]) {
    // TODO: create an 50,000,000 file with 30 cols
    std::string filepath = "..\\data\\parqtest\\*.parquet";
    std::cout << "Reading file: " << filepath << std::endl;
    bool printTable = true;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--enable-print") {
            printTable = true;
        }
    }

    DataProcessor processor;
    // processor.loadParquet(filepath);

    auto start = std::chrono::high_resolution_clock::now();
    std::shared_ptr<arrow::Table> table = processor.process(filepath);
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Time taken by function: " << elapsed.count() << " seconds." << std::endl;

    if (table) {
        std::cout << "Successfully processed data into Arrow Table." << std::endl;
        if (printTable) {
            //PrintArrowTable(table);
        }
    } else {
        std::cerr << "Failed to process data." << std::endl;
    }

    return 0;
}

