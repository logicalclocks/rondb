set(AVRO_RDRS_LIB "${CMAKE_BINARY_DIR}/library_output_directory/libavro.a")
set(AVRO_RDRS_INCLUDE "${CMAKE_CURRENT_BINARY_DIR}/include")
set(AVRO_RDRS_HEADER "${AVRO_RDRS_INCLUDE}/avro.h")

# A custom command defines input and output files, using DEPENDS and OUTPUT,
# respectively. This way the command will only be executed when inputs change,
# similarly to most build steps under CMake.
add_custom_command(
    OUTPUT ${AVRO_RDRS_LIB} ${AVRO_RDRS_HEADER}
    COMMAND ${CMAKE_COMMAND} -E env CGO_LDFLAGS="-O3" go build -o ${AVRO_RDRS_LIB} -buildmode=c-archive avro.go avro_parser.go
    COMMAND mkdir -p ${AVRO_RDRS_INCLUDE}
    COMMAND mv ${CMAKE_BINARY_DIR}/library_output_directory/libavro.h ${AVRO_RDRS_HEADER}
    DEPENDS avro.go avro_parser.go go.mod go.sum
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building libavro (Go CGO shared library)"
)

# Since add_dependencies can only be used with target names, not file names, we
# need a target to represent the outputs from the custom command above. It
# appears that custom targets are only phony when they lack dependencies, so
# dependency tracking is not ruined by phoniness.
add_custom_target(AVRO_RDRS_LIB_BUILD
    DEPENDS "${AVRO_RDRS_LIB}" "${AVRO_RDRS_HEADER}"
)

# "An IMPORTED library... has scope in the directory... the GLOBAL option
# extends visibility."
# https://cmake.org/cmake/help/v3.9/command/add_library.html#imported-libraries
add_library(libavro STATIC IMPORTED GLOBAL)

# Populate the library properties.
set_target_properties(libavro PROPERTIES
    IMPORTED_LOCATION "${AVRO_RDRS_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${AVRO_RDRS_INCLUDE}"
)

# We need to create the include directory at configure time, otherwise CMake
# considers its use invalid in the INTERFACE_INCLUDE_DIRECTORIES property above.
file(MAKE_DIRECTORY "${AVRO_RDRS_INCLUDE}")

# Cause the library to be built before it is used
add_dependencies(libavro AVRO_RDRS_LIB_BUILD)

# Install step
INSTALL(FILES "${AVRO_RDRS_LIB}" DESTINATION lib
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ GROUP_EXECUTE
  WORLD_READ WORLD_EXECUTE
  )

