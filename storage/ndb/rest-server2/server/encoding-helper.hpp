#ifndef ENCODING_HELPER_HPP
#define ENCODING_HELPER_HPP
#include <cstdint>
#include <vector>
#include <string>
#include <string_view>
#include <iostream>

uint32_t copy_str_to_buffer(std::vector<char> src, void* dst, uint32_t offset) {
	if (dst == nullptr) {
			throw std::invalid_argument("Destination buffer pointer is null");
	}

	uint32_t src_length = static_cast<uint32_t>(src.size());

	std::memcpy(static_cast<uint8_t*>(dst) + offset, src.data(), src_length);

	// Append a NULL terminator after the copied string
	static_cast<uint8_t*>(dst)[offset + src_length] = 0x00;

	return offset + src_length + 1;
}

uint32_t copy_ndb_str_to_buffer(std::vector<char> src, void* dst, uint32_t offset) {
	if (dst == nullptr) {
			throw std::invalid_argument("Destination buffer pointer is null");
	}

	// Remove quotation marks from string, if present
	std::vector<char> processed_src;
	if (src.size() >= 2 && src.front() == '\"' && src.back() == '\"') {
			// Copy string without the first and last character (quotation marks)
			processed_src.insert(processed_src.end(), src.begin() + 1, src.end() - 1);
	} else {
			processed_src = src;
	}

	uint32_t src_length = static_cast<uint32_t>(processed_src.size());

	// Write immutable length of the string
	static_cast<char*>(dst)[offset] = static_cast<char>(src_length % 256);
	static_cast<char*>(dst)[offset + 1] = static_cast<char>(src_length / 256);
	offset += 2;

	// Reserve space for mutable length, manipulated by the C layer (initialized to zero)
	static_cast<char*>(dst)[offset] = 0;
	static_cast<char*>(dst)[offset + 1] = 0;
	offset += 2;

	// Copy the processed source data to the destination buffer
	std::memcpy(reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(dst) + offset), processed_src.data(), src_length);

	// Append a NULL terminator after the copied data
	static_cast<uint8_t*>(dst)[offset + src_length] = 0x00;

	return offset + src_length + 1;
}

std::vector<char> string_to_byte_array(const std::string& str) {
    return std::vector<char>(str.begin(), str.end());
}

std::vector<char> string_view_to_byte_array(const std::string_view& str_view) {
    return std::vector<char>(str_view.begin(), str_view.end());
}

uint32_t align_word(uint32_t head) {
	uint32_t a = head / 4;
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

#endif // ENCODING_HELPER_HPP
