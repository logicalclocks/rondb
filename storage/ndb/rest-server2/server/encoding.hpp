#ifndef ENCODING_HPP
#define ENCODING_HPP

#include "pk_data_structs.hpp"
#include "src/rdrs-const.h"
#include "encoding_helper.hpp"
#include <iostream>
#include "src/rdrs-dal.h"
#include <drogon/HttpTypes.h>
#include <drogon/drogon.h>

void create_native_request(PKReadParams&,	void*, void*);

std::string process_pkread_response(void*,	PKReadResponseJSON&);

#endif // ENCODING_HPP
