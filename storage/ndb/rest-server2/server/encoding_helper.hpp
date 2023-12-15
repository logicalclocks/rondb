#ifndef ENCODING_HELPER_HPP
#define ENCODING_HELPER_HPP

#include <cstdint>
#include <vector>
#include <string>
#include <string_view>
#include <iostream>
#include "pk_data_structs.hpp"
#include "src/rdrs-const.h"
#include <utility>
#include <algorithm>

void printCharArray(const char*, size_t);

void unquote(std::vector<char>&, bool);

uint32_t copy_str_to_buffer(const std::vector<char>&, void*, uint32_t);

uint32_t copy_ndb_str_to_buffer(std::vector<char>&, void*, uint32_t);

std::vector<char> string_to_byte_array(std::string);

std::vector<char> string_view_to_byte_array(const std::string_view&);

uint32_t align_word(uint32_t);

uint32_t data_return_type(std::string_view);

std::string quote_if_string(uint32_t, std::string);

void printReqBuffer(const RS_Buffer*);

void printStatus(RS_Status);

void printCharArray(const char*, size_t);

void unquote(std::vector<char>&, bool);

#endif // ENCODING_HELPER_HPP
