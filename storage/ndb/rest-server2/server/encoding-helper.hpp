#ifndef ENCODING_HELPER_HPP
#define ENCODING_HELPER_HPP

#include <cstdint>
#include <vector>
#include <string>
#include <string_view>
#include <iostream>
#include "pk-data-structs.hpp"
#include "src/rdrs-const.h"

uint32_t copy_str_to_buffer(std::vector<char> src, void* dst, uint32_t offset) {
	if (dst == nullptr) {
		throw std::invalid_argument("Destination buffer pointer is null");
	}

	uint32_t src_length = static_cast<uint32_t>(src.size());

	for (uint32_t i = offset, j = 0; i < offset + src_length; ++i, ++j) {
		static_cast<char*>(dst)[i] = src[j];
	}

	static_cast<char*>(dst)[offset + src_length] = 0x00;

	return offset + src_length + 1;
}

uint32_t copy_ndb_str_to_buffer(std::vector<char> src, void* dst, uint32_t offset) {
	if (dst == nullptr) {
		throw std::invalid_argument("Destination buffer pointer is null");
	}

	// Remove quotation marks from string, if present
	std::vector<char> processed_src;
	if (src.size() >= 2 && src.front() == '\"' && src.back() == '\"') {
		processed_src.insert(processed_src.end(), src.begin() + 1, src.end() - 1);
	} else {
		processed_src = src;
	}

	uint32_t src_length = static_cast<uint32_t>(processed_src.size());

	// Write immutable length of the string
	static_cast<char*>(dst)[offset] = static_cast<char>(src_length % 256);
	static_cast<char*>(dst)[offset + 1] = static_cast<char>(src_length / 256);
	offset += 2;

	static_cast<char*>(dst)[offset] = 0;
	static_cast<char*>(dst)[offset + 1] = 0;
	offset += 2;

	for (uint32_t i = offset, j = 0; i < offset + src_length; ++i, ++j) {
		static_cast<char*>(dst)[i] = processed_src[j];
	}

	static_cast<char*>(dst)[offset + src_length] = 0x00;

	return offset + src_length + 1;
}

std::vector<char> string_to_byte_array(const std::string& str) {
	return std::vector<char>(str.begin(), str.end());
}

std::vector<char> string_view_to_byte_array(const std::string_view& str_view) {
	return std::vector<char>(str_view.begin(), str_view.end());
}

uint32_t align_word(uint32_t head) {
	uint32_t a = head % 4;
	if (a != 0)
		head += (4 - a);
	return head;
}

uint32_t data_return_type(std::string_view drt) {
	if (drt == to_string(DataReturnType::DEFAULT_DRT)) {
		return DataReturnType::DEFAULT_DRT;
	} else {
		std::cout << "Unknown data return type: " << drt << std::endl;
		return UINT32_MAX;
	}
}

/*
For both JSON and gRPC we need a way of letting the client know the datatype.

In JSON, strings are generally represented by using quotes (for numbers they are omitted).
For gRPC, we fixed our datatype to strings. So we pretend to have a JSON setup and also add
quotes when we are actually dealing with strings.

Since binary data is encoded as base64 strings, we also add quotes for these.
*/
std::string quote_if_string(uint32_t dataType, std::string value) {
	if (dataType == RDRS_INTEGER_DATATYPE || dataType == RDRS_FLOAT_DATATYPE) {
		return value;
	} else {
		return "\"" + value + "\"";
	}
}

void printCharArray(const char* array, size_t length) {
    for (size_t i = 0; i < length; ++i) {
        std::cout << array[i];
    }
    std::cout << std::endl;
}

void printReqBuffer(const RS_Buffer* reqBuff) {
  char* reqData = reqBuff->buffer;
  unsigned int reqSize = reqBuff->size;
  std::cout << "Request buffer: " << std::endl;
  std::cout << "OP Type: " << std::hex << "0x" << ((uint32_t*)reqData)[0] << std::endl;
  std::cout << "Capacity: " << std::hex << "0x" << ((uint32_t*)reqData)[1] << std::endl;
  std::cout << "Length: " << std::hex << "0x" << ((uint32_t*)reqData)[2] << std::endl;
  std::cout << "Flags: " << std::hex << "0x" << ((uint32_t*)reqData)[3] << std::endl;
  std::cout << "DB Idx: " << std::hex << "0x" << ((uint32_t*)reqData)[4] << std::endl;
  std::cout << "Table Idx: " << std::hex << "0x" << ((uint32_t*)reqData)[5] << std::endl;
  std::cout << "PK Cols Idx: " << std::hex << "0x" << ((uint32_t*)reqData)[6] << std::endl;
  std::cout << "Read Cols Idx: " << std::hex << "0x" << ((uint32_t*)reqData)[7] << std::endl;
  std::cout << "OP ID Idx: " << std::hex << "0x" << ((uint32_t*)reqData)[8] << std::endl;
  std::cout << "DB: ";
  uint32_t dbIdx = ((uint32_t*)reqData)[4];
  std::cout << (char*)((uintptr_t)reqData + dbIdx) << std::endl;
  std::cout << "Table: ";
  uint32_t tableIdx = ((uint32_t*)reqData)[5];
  std::cout << (char*)((uintptr_t)reqData + tableIdx) << std::endl;
  uint32_t pkColsIdx = ((uint32_t*)reqData)[6];
  std::cout << "PK Cols Count: " << std::hex << "0x" << 
    *((uint32_t*)((uintptr_t)reqData + pkColsIdx)) << std::endl;
  for (int i = 0; i < *((uint32_t*)((uintptr_t)reqData + pkColsIdx)); i++) {
    int step = (i + 1) * ADDRESS_SIZE;
    std::cout << "KV pair " << i << " Idx: " << std::hex << "0x" << 
      *((uint32_t*)((uintptr_t)reqData + pkColsIdx + step)) << std::endl;
    uint32_t kvPairIdx = *((uint32_t*)((uintptr_t)reqData + pkColsIdx + step));
    uint32_t keyIdx = ((uint32_t*)reqData)[kvPairIdx / ADDRESS_SIZE];
    std::cout << "Key idx: " << std::hex << "0x" << keyIdx << std::endl;
    std::cout << "Key " << i + 1 << ": ";
    std::cout << (char*)((uintptr_t)reqData + keyIdx) << std::endl;
    uint32_t valueIdx = ((uint32_t*)reqData)[(kvPairIdx / ADDRESS_SIZE) + 1];
    std::cout << "Value idx: " << std::hex << "0x" << valueIdx << std::endl;
    std::cout << "Size " << i + 1 << ": ";
    std::cout << *((uint16_t*)((uintptr_t)reqData + valueIdx)) << std::endl;
    std::cout << "Value " << i + 1 << ": ";
    std::cout << (char*)((uintptr_t)reqData + valueIdx + ADDRESS_SIZE) << std::endl;
  }
  uint32_t readColsIdx = ((uint32_t*)reqData)[7];
  std::cout << "Read Cols Count: " << std::hex << "0x" <<
    *((uint32_t*)((uintptr_t)reqData + readColsIdx)) << std::endl;
  for (int i = 0; i < *((uint32_t*)((uintptr_t)reqData + readColsIdx)); i++) {
    int step = (i + 1) * ADDRESS_SIZE;
    std::cout << "Read Col " << i << " Idx: " << std::hex << "0x" << 
      *((uint32_t*)((uintptr_t)reqData + readColsIdx + step)) << std::endl;
    uint32_t readColIdx = *((uint32_t*)((uintptr_t)reqData + readColsIdx + step));
    uint32_t returnType = ((uint32_t*)reqData)[readColIdx / ADDRESS_SIZE];
    std::cout << "Return type: " << std::hex << "0x" << returnType << std::endl;
    std::cout << "Col " << i + 1 << ": ";
    std::cout << (char*)((uintptr_t)reqData + readColIdx + ADDRESS_SIZE) << std::endl;
  }
  uint32_t opIDIdx = ((uint32_t*)reqData)[8];
  std::cout << "Op ID: ";
  std::cout << (char*)((uintptr_t)reqData + opIDIdx) << std::endl;
}

#endif // ENCODING_HELPER_HPP
