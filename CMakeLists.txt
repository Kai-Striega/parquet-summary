cmake_minimum_required(VERSION 3.28)

project(
    parquet_summary
    VERSION 0.1.0
    LANGUAGES CXX
)

add_executable(parquet_summary main.cpp)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)


# N.B. (Kai) Arrow can be built as either a shared or static library.
#      We explicitly want to link against the shared library as I
#      eventually want to make this available on Windows and do not
#      want to deal with building Arrow and Parquet myself.
target_link_libraries(parquet_summary
    PRIVATE
        Arrow::arrow_shared
        Parquet::parquet_shared
)

target_compile_features(parquet_summary PRIVATE cxx_std_23)

option(PARQUET_SUMMARY_SANITIZERS "Enable sanitizers" OFF)

if (PARQUET_SUMMARY_SANITIZERS)
    if(NOT MSVC)
        target_compile_options(parquet_summary PRIVATE -fsanitize=address,undefined,thread)
        target_link_options(parquet_summary PRIVATE -fsanitize=address,undefined,thread)
    else()
        message(WARNING "Sanitizers are not supported with MSVC. Consider using Clang-cl.")
    endif()
endif()

if (MSVC)
    target_compile_options(parquet_summary PRIVATE /W4)
else()
    target_compile_options(parquet_summary PRIVATE -Wall -Wextra -Wpedantic)
endif()
