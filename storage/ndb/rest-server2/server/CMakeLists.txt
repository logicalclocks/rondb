project(rdrs2 LANGUAGES CXX C VERSION 1.0.0)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

# RonDB passes -std=c++20 in CMAKE_CXX_FLAGS, and CMake appends -std=gnu++17
# for the standard set above. gcc then compiles as C++17 but keeps the
# coroutine support that -std=c++20 switched on. Drogon's headers test for it
# (__cpp_impl_coroutine) and include drogon/utils/coroutine.h, which needs the
# C++20 standard library and does not compile as C++17. Switch coroutines off,
# which is what clang does when the last -std is C++17. The REST server does
# not use coroutines.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fno-coroutines>)
endif()

add_subdirectory(avro)
add_subdirectory(src)
add_subdirectory(test)
