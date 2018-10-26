cmake_minimum_required(VERSION 2.8.11)

cmake_policy(VERSION 2.8)
cmake_policy(SET CMP0026 NEW)
cmake_policy(SET CMP0042 NEW)
cmake_policy(SET CMP0046 NEW)
cmake_policy(SET CMP0051 NEW)
cmake_policy(SET CMP0056 NEW)
cmake_policy(SET CMP0067 NEW)

set(CMAKE_BUILD_TYPE RELEASE)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

project(engine)
add_definitions(-std=c++11)

if (MSVC)
    add_definitions(-D_XKEYCHECK_H)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    #add_definitions(-D_UNICODE -DUNICODE)
    add_definitions("/wd4819")
    foreach(flag_var
            CMAKE_C_FLAGS
            CMAKE_C_FLAGS_DEBUG
            CMAKE_C_FLAGS_RELEASE
            CMAKE_C_FLAGS_MINSIZEREL
            CMAKE_C_FLAGS_RELWITHDEBINFO
            CMAKE_CXX_FLAGS
            CMAKE_CXX_FLAGS_DEBUG
            CMAKE_CXX_FLAGS_RELEASE
            CMAKE_CXX_FLAGS_MINSIZEREL
            CMAKE_CXX_FLAGS_RELWITHDEBINFO
        )
        if(${flag_var} MATCHES "/MD")
            string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
        endif(${flag_var} MATCHES "/MD")
    endforeach(flag_var)
    add_compile_options("$<$<C_COMPILER_ID:MSVC>:/utf-8>")
    add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/utf-8>")
    add_definitions(-DBUILD_TEST)
endif()


add_subdirectory(util)
add_subdirectory(engine_race)
add_subdirectory(test)
