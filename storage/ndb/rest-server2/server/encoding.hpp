#ifndef ENCODING_HPP
#define ENCODING_HPP

#include "pk-data-structs.hpp"
#include "src/rdrs-const.h"
#include "encoding-helper.hpp"
#include <iostream>
#include "src/rdrs-dal.h"
#include <drogon/HttpTypes.h>
#include <drogon/drogon.h>

void create_native_request(
	PKReadParams pkReadParams,
	void* reqBuff,
	void* respBuff
) {
	uint32_t* buf = (uint32_t*)(reqBuff);

	uint32_t head = PK_REQ_HEADER_END;

	uint32_t dbOffset = head;

	try
	{
		head = copy_str_to_buffer(string_view_to_byte_array(pkReadParams.path.db), reqBuff, head);
	}
	catch(const std::exception& e)
	{
		std::cerr << e.what() << '\n';
		return;
	}

	uint32_t tableOffset = head;

	try
	{
		head = copy_str_to_buffer(string_view_to_byte_array(pkReadParams.path.table), reqBuff, head);
	}
	catch(const std::exception& e)
	{
		std::cerr << e.what() << '\n';
		return;
	}

	// PK Filters
	head = align_word(head);
	uint32_t pkOffset = head;
	buf[head / ADDRESS_SIZE] = uint32_t(pkReadParams.filters.size());
	head += ADDRESS_SIZE;

	uint32_t kvi = head / ADDRESS_SIZE; // index for storing offsets for each key/value pair
	// skip for N number of offsets one for each key/value pair
	head += uint32_t(pkReadParams.filters.size()) * ADDRESS_SIZE;

	for (auto filter: pkReadParams.filters) {
		head = align_word(head);

		uint32_t tupleOffset = head;

		head += 8;

		uint32_t keyOffset = head;

		try
		{
			head = copy_str_to_buffer(string_view_to_byte_array(filter.column), reqBuff, head);
		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
			return;
		}

		uint32_t value_offset = head;
		
		try
		{
			head = copy_ndb_str_to_buffer(filter.value, reqBuff, head);
		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
			return;
		}

		buf[kvi] = tupleOffset;
		kvi++;
		buf[tupleOffset / ADDRESS_SIZE] = keyOffset;
		buf[tupleOffset / ADDRESS_SIZE + 1] = value_offset;
	}

	// Read Columns
	head = align_word(head);
	uint32_t readColsOffset = 0;
	if (!pkReadParams.readColumns.empty()) {
		readColsOffset = head;
		buf[head / ADDRESS_SIZE] = (uint32_t)(pkReadParams.readColumns.size());
		head += ADDRESS_SIZE;

		uint32_t rci = head / ADDRESS_SIZE;
		head += uint32_t(pkReadParams.readColumns.size()) * ADDRESS_SIZE;

		for (auto col: pkReadParams.readColumns) {
			head = align_word(head);

			buf[rci] = head;
			rci++;

			//return type
			uint32_t drt = DEFAULT_DRT;
			if (!col.returnType.empty()) {
					drt = data_return_type(col.returnType);
					if (drt == UINT32_MAX)
						return;
			}

			buf[head / ADDRESS_SIZE] = drt;
			head += ADDRESS_SIZE;

			// col name
			try
			{
				head = copy_str_to_buffer(string_view_to_byte_array(col.column), reqBuff, head);
			}
			catch(const std::exception& e)
			{
				std::cerr << e.what() << '\n';
				return;
			}
		}
	}

	// Operation ID
	uint32_t op_id_offset = 0;
	if (!pkReadParams.operationId.empty()) {
		op_id_offset = head;
		try
		{
			head = copy_str_to_buffer(string_view_to_byte_array(pkReadParams.operationId), reqBuff, head);
		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
			return;
		}
	}

	// request buffer header
	buf[PK_REQ_OP_TYPE_IDX] = (uint32_t)(RDRS_PK_REQ_ID);
	buf[PK_REQ_CAPACITY_IDX] = (uint32_t)(5 * 1024 * 1024);
	buf[PK_REQ_LENGTH_IDX] = (uint32_t)(head);
	buf[PK_REQ_FLAGS_IDX] = (uint32_t)(0); // TODO fill in. is_grpc, is_http ...
	buf[PK_REQ_DB_IDX] = (uint32_t)(dbOffset);
	buf[PK_REQ_TABLE_IDX] = (uint32_t)(tableOffset);
	buf[PK_REQ_PK_COLS_IDX] = (uint32_t)(pkOffset);
	buf[PK_REQ_READ_COLS_IDX] = (uint32_t)(readColsOffset);
	buf[PK_REQ_OP_ID_IDX] = (uint32_t)(op_id_offset);
}

std::string process_pkread_response(
	void* respBuff,
	PKReadResponseJSON response
) {
	uint32_t* buf = (uint32_t*)(respBuff);

	uint32_t responseType = buf[PK_RESP_OP_TYPE_IDX];

	if (responseType != RDRS_PK_RESP_ID) {
		std::string message = "internal server error. Wrong response type";
		return message;
	}

	// some sanity checks
	uint32_t capacity = buf[PK_RESP_CAPACITY_IDX];
	uint32_t dataLength = buf[PK_RESP_LENGTH_IDX];
	if (capacity < dataLength) {
		std::string message = "internal server error. response buffer may be corrupt. ";
		message +=	"Buffer capacity: " + std::to_string(capacity) + ", data length: " + std::to_string(dataLength);
		return message;	
	}

	uint32_t opIDX = buf[PK_RESP_OP_ID_IDX];
	if (opIDX != 0) {
		uintptr_t opIDXPtr = (uintptr_t)respBuff + (uintptr_t)opIDX;
		std::string goOpID = std::string((char*)opIDXPtr);
		response.setOperationID(goOpID);
	}

	int32_t status = (int32_t)buf[PK_RESP_OP_STATUS_IDX];
	if (status == drogon::HttpStatusCode::k200OK) {
		uint32_t colIDX = buf[PK_RESP_COLS_IDX];
		uintptr_t colIDXPtr = (uintptr_t)respBuff + (uintptr_t)colIDX;
		uint32_t colCount = *(uint32_t*)colIDXPtr;
 
		for (uint32_t i = 0; i < colCount; i++) {
			uint32_t* colHeaderStart = reinterpret_cast<uint32_t*>(
				reinterpret_cast<uintptr_t>(respBuff) + 
				colIDX + 
				ADDRESS_SIZE + 
				i * 4 * ADDRESS_SIZE
			);

			uint32_t colHeader[4];
			for (uint32_t j = 0; j < 4; j++) {
				colHeader[j] = colHeaderStart[j];
			}

			uint32_t nameAdd = colHeader[0];
			std::string name = std::string((char*)(
				reinterpret_cast<uintptr_t>(respBuff) + 
				nameAdd
			));

			uint32_t valueAdd = colHeader[1];

			uint32_t isNull = colHeader[2];

			uint32_t dataType = colHeader[3];

			if (isNull == 0) {
				std::string value = std::string((char*)(
					reinterpret_cast<uintptr_t>(respBuff) + 
					valueAdd
				));

				std::string quotedValue = quote_if_string(dataType, value);
				response.setColumnData(name, string_to_byte_array(quotedValue));
			} else {
				response.setColumnData(name, std::vector<char>());
			}
		}
	}

	std::string message = "";
	uint32_t messageIDX = buf[PK_RESP_OP_MESSAGE_IDX];
	if (messageIDX != 0) {
		uintptr_t messageIDXPtr = (uintptr_t)respBuff + (uintptr_t)messageIDX;
		message = std::string((char*)messageIDXPtr);
	}
	return message;
}

#endif // ENCODING_HPP