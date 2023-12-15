#ifndef JSON_PARSER_HPP
#define JSON_PARSER_HPP

#define SIMDJSON_VERBOSE_LOGGING 0
#include <simdjson.h>
#define MAX_THREADS 16
#define BODY_MAX_SIZE 1024 * 1024 + SIMDJSON_PADDING
#include "pk_data_structs.hpp"

using namespace simdjson;

extern char buffers[MAX_THREADS][BODY_MAX_SIZE];

namespace json_parser {
	int parse(std::string_view&, PKReadParams&, int);
}

#endif // JSON_PARSER_HPP
