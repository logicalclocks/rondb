project(rdrs2 LANGUAGES CXX C VERSION 1.0.0)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(avro)
add_subdirectory(src)
if(WITH_UNIT_TESTS)
  add_subdirectory(test)
endif()
