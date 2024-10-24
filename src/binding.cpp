#include <pybind11/pybind11.h>
#include <pybind11/stl.h>  // Enables support for STL types (like std::string)
#include "data_processor.hpp"
#include <memory>
#include <arrow/table.h>
#include <arrow/c/bridge.h>  // For converting DuckDB Arrow to Arrow C++
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/c/helpers.h>
#include <iostream>

// PyArrow conversion
#include <arrow/python/pyarrow.h>  // For pyarrow conversions

namespace py = pybind11;

// Function to print the data in an Apache Arrow Table
void PrintArrowTable2(const std::shared_ptr<arrow::Table>& table) {
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





PYBIND11_MODULE(duckarrow, m) {
    m.doc() = "DuckArrowBridge Python bindings using pybind11";  // Optional module docstring
    
    
    if (arrow::py::import_pyarrow() != 0) {
        throw std::runtime_error("Failed to import pyarrow");
    }

    // Bind the DataProcessor class
    py::class_<DataProcessor>(m, "DataProcessor")
        .def(py::init<>())  // Expose the constructor
        .def("process", &DataProcessor::process, py::arg("filepath"), "Process a parquet file and write results to Parquet")  // Bind the process method (returns void)
        .def("loadParquet", &DataProcessor::loadParquet, py::arg("filepath"), "Load a parquet file")  // Bind loadParquet
        .def_static("write_parquet", &DataProcessor::WriteParquetFile, py::arg("table"), py::arg("filepath"), "Write an Arrow table to a Parquet file")  // Bind WriteParquetFile
        .def("processQuery", [](DataProcessor& self, const std::string& query) {
            // Call the processQuery method to get an Arrow Table
            auto table = self.processQuery(query);
            if (!table) {
                throw std::runtime_error("Failed to process query and get an Arrow Table");
            }

            // Print the table for debugging
            PrintArrowTable2(table);

            // Attempt to convert Arrow Table to PyArrow Table
            try {
                // Convert the result from PyObject* to py::object
                PyObject* raw_pyarrow_table = arrow::py::wrap_table(table);
                if (!raw_pyarrow_table) {
                    throw std::runtime_error("Failed to wrap the Arrow Table in a PyArrow Table");
                }
                py::object pyarrow_table = py::reinterpret_steal<py::object>(raw_pyarrow_table);
                return pyarrow_table;
            } catch (const std::exception& e) {
                // Catch any exceptions during conversion and provide a detailed error message
                throw std::runtime_error(std::string("Failed to convert Arrow Table to PyArrow Table: ") + e.what());
            }
        }, py::arg("query"), "Execute a query and return the resulting PyArrow Table");
        // .def("processQuery", &DataProcessor::processQuery, py::arg("query"), "Execute a query and return the resulting Arrow Table as an arrow::Table");  // Bind processQuery
}
