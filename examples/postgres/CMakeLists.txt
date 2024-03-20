cmake_minimum_required(VERSION 2.8.12)
project(example-c)

include_directories(../../src/include)
link_directories(../../../build/release/src)

add_library(duckconvert STATIC GetDuckData.c ConvertDuckType.c)

add_executable(typetest  typetest.c ConvertDuckType.c)
target_link_libraries(typetest duckconvert duckdb)

add_executable(datatest datatest.c)
target_link_libraries(datatest duckconvert duckdb)
