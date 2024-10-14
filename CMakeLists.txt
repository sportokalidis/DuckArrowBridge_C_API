cmake_minimum_required(VERSION 3.10)
project(DuckArrowBridge)

set(CMAKE_CXX_STANDARD 17)

# Include directories
include_directories(include)

# Link directories
link_directories(lib)

# Add source files, excluding main.cpp
file(GLOB LIB_SOURCES "src/*.cpp")
list(REMOVE_ITEM LIB_SOURCES "src/main.cpp")

# Create the shared library (DLL)
add_library(DuckArrowBridgeLib SHARED ${LIB_SOURCES})

## Find Arrow package
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

# -DVCPKG_TARGET_TRIPLET:STRING=x64-mingw-dynamic
set(VCPKG_TARGET_TRIPLET "x64-mingw-dynamic")
## Link libraries
target_link_libraries(${PROJECT_NAME}Lib PRIVATE duckdb Arrow::arrow_shared Parquet::parquet_shared)

# Define the DLL export macro for the shared library
target_compile_definitions(DuckArrowBridgeLib PRIVATE BUILD_DLL)

# Create the executable and link it to the shared library
add_executable(${PROJECT_NAME} src/main.cpp)
# Link the shared library to the executable
target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}Lib duckdb Arrow::arrow_shared Parquet::parquet_shared)

# Define the DLL directory based on the build configuration
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(DLL_DIR "${PROJECT_SOURCE_DIR}/dll/Debug")
else()
    set(DLL_DIR "${PROJECT_SOURCE_DIR}/dll")
endif()

# List all required DLLs (add other required DLLs to this list)
set(REQUIRED_DLLS
#    "${DLL_DIR}/arrow.dll"
#    "${DLL_DIR}/brotlicommon.dll"
#    "${DLL_DIR}/brotlidec.dll"
#    "${DLL_DIR}/brotlienc.dll"
#    "${DLL_DIR}/bz2d.dll"
    "${DLL_DIR}/duckdb.dll"
#    "${DLL_DIR}/lz4d.dll"
#    "${DLL_DIR}/snappy.dll"
#    "${DLL_DIR}/zlibd1.dll"
#    "${DLL_DIR}/zstd.dll"
    # Add more DLLs here if needed
)

# For Windows, copy the DLL to the output directory
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Ensure the executable path is used for the destination of the DLLs
    foreach(DLL ${REQUIRED_DLLS})
        add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
            ${DLL} $<TARGET_FILE_DIR:${PROJECT_NAME}>)
    endforeach()

    # add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
    #     COMMAND ${CMAKE_COMMAND} -E copy
    #     ${PROJECT_SOURCE_DIR}/dll/Debug/duckdb.dll
    #     $<TARGET_FILE_DIR:${PROJECT_NAME}>)
else()
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        ${PROJECT_SOURCE_DIR}/dll/duckdb.dll
        $<TARGET_FILE_DIR:${PROJECT_NAME}>)
endif()


# Copy the output DLL and import library to a directory for easier use
if(WIN32)
    set(OUTPUT_DIR "${CMAKE_BINARY_DIR}/output")
    file(MAKE_DIRECTORY ${OUTPUT_DIR})
    add_custom_command(TARGET DuckArrowBridgeLib POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
            $<TARGET_FILE:DuckArrowBridgeLib> ${OUTPUT_DIR}
            COMMAND ${CMAKE_COMMAND} -E copy
            $<TARGET_LINKER_FILE:DuckArrowBridgeLib> ${OUTPUT_DIR}
    )
endif()