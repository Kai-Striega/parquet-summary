cmake_minimum_required(VERSION 4.1)
project(parquet_summary)

set(CMAKE_CXX_STANDARD 23)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

add_compile_options(-Wall -Wextra -Wpedantic)

add_executable(parquet_summary main.cpp)

target_link_libraries(parquet_summary
    PRIVATE
        Arrow::arrow_shared
        Parquet::parquet_shared
)
