# DuckArrowBridge

## Project Structure
The project follows this structure:

```
DuckArrowBridge/
├── build/                # Build directory
├── data/                 # Directory to store parquet files
├── dll/                  # Dynamic-link library files
├── include/              # Header files (duckdb.hpp, data_processor.hpp)
├── lib/                  # Library files
├── src/                  # Source files (main.cpp, data_processor.cpp)
└── CMakeLists.txt        # CMake build script
```

## Build and Run Instructions
### Dependencies
Ensure that you have installed the following dependencies via vcpkg:
` Apache Arrow `

### Building the Project
You can build the project in both Debug and Release modes.

1. Debug Mode:

```shell
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_PREFIX_PATH=C:/vcpkg/installed/x64-windows
cmake --build . --config Debug
.\Debug\DuckArrowBridge.exe --enable-print
```

2. Release Mode:
```shell
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=C:/vcpkg/installed/x64-windows
cmake --build . --config Release
.\Release\DuckArrowBridge.exe --enable-print
```

### Command-Line Flags:

`--enable-print`: Enables printing of the Apache Arrow table at the end of execution. If this flag is not provided, the table will be processed but not displayed.

### Input:
The input Parquet file is hardcoded in main.cpp as:
```cpp
std::string filepath = "..\\data\\test_output_light.parquet";
```