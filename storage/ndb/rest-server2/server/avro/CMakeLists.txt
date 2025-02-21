project(libavro)

set(SHARED_LIB "${CMAKE_BINARY_DIR}/library_output_directory/libavro.a")

# Build the Go library


#COMMAND go build -ldflags=-extldflags=-Wl,-rpath,"${CMAKE_CURRENT_BINARY_DIR}/lib" -o ${SHARED_LIB}  -buildmode=c-shared avro.go 
add_custom_target(LIBAVRO_BUILD ALL
    COMMAND ${CMAKE_COMMAND} -E env CGO_LDFLAGS="-O3" go build -o ${SHARED_LIB}  -buildmode=c-archive avro.go  avro_parser.go
    COMMAND mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/include 
    COMMAND mv ${CMAKE_BINARY_DIR}/library_output_directory/libavro.h ${CMAKE_CURRENT_BINARY_DIR}/include/avro.h
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building libavro (Go CGO shared library)"
)

# Ensure the headers and libraries are available
add_library(libavro SHARED IMPORTED)
set_target_properties(libavro PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/${SHARED_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}/include"
)

# Ensure the library is built before the main project uses it
add_dependencies(libavro LIBAVRO_BUILD)

# Set PATHS
SET(RDRS_LIBAVRO_LIB_DIR     "${CMAKE_BINARY_DIR}/library_output_directory"     CACHE INTERNAL "RDRS libavro dir" FORCE)
SET(RDRS_LIBAVRO_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/include"              CACHE INTERNAL "RDRS libavro include dir" FORCE)

# Install step
INSTALL(FILES "${SHARED_LIB}" DESTINATION lib 
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ GROUP_EXECUTE
  WORLD_READ WORLD_EXECUTE
  )
