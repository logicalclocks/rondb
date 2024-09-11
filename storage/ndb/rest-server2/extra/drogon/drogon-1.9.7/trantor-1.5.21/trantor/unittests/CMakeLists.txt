find_package(GTest REQUIRED)
add_executable(msgbuffer_unittest MsgBufferUnittest.cc)
add_executable(inetaddress_unittest InetAddressUnittest.cc)
add_executable(date_unittest DateUnittest.cc)
add_executable(split_string_unittest splitStringUnittest.cc)
add_executable(string_encoding_unittest stringEncodingUnittest.cc)
add_executable(ssl_name_verify_unittest sslNameVerifyUnittest.cc)
add_executable(hash_unittest HashUnittest.cc)
set(UNITTEST_TARGETS
    msgbuffer_unittest
    inetaddress_unittest
    date_unittest
    split_string_unittest
    string_encoding_unittest
    ssl_name_verify_unittest
    hash_unittest
)
set_property(TARGET ${UNITTEST_TARGETS} PROPERTY CXX_STANDARD 14)
set_property(TARGET ${UNITTEST_TARGETS} PROPERTY CXX_STANDARD_REQUIRED ON)
set_property(TARGET ${UNITTEST_TARGETS} PROPERTY CXX_EXTENSIONS OFF)

include(GoogleTest)
foreach(T ${UNITTEST_TARGETS})
  target_link_libraries(${T} PRIVATE trantor GTest::GTest)
  gtest_discover_tests(${T})
endforeach()
