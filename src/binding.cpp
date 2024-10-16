#include <pybind11/pybind11.h>
#include <pybind11/stl.h>  // Enables support for STL types (like std::string)
#include "data_processor.hpp"

namespace py = pybind11;

PYBIND11_MODULE(duckarrow, m) {
    m.doc() = "DuckArrowBridge Python bindings using pybind11";  // Optional module docstring

    // Bind the DataProcessor class
    py::class_<DataProcessor>(m, "DataProcessor")
        .def(py::init<>())  // Expose the constructor
        .def("process", &DataProcessor::process, py::arg("filepath"), "Process a parquet file and write results to Parquet")  // Bind the process method (returns void)
        .def("loadParquet", &DataProcessor::loadParquet, py::arg("filepath"), "Load a parquet file")  // Bind loadParquet
        .def_static("write_parquet", &DataProcessor::WriteParquetFile, py::arg("table"), py::arg("filepath"), "Write an Arrow table to a Parquet file");  // Bind WriteParquetFile
}
