
set(RDRS_CLIENT_LIB_DIR ${CMAKE_BINARY_DIR}/library_output_directory)
set(RDRS_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/rest-api-server/bin/server/)
add_subdirectory(data-access-rondb)
add_subdirectory(rest-api-server)
