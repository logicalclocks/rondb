#include "pk_read_ctrl.hpp"
#include "pk_data_structs.hpp"
#include <drogon/HttpTypes.h>
#include "json_parser.hpp"
#include "encoding.hpp"

void PKReadCtrl::ping(const HttpRequestPtr &req,
											std::function<void(const HttpResponsePtr &)> &&callback) {
	auto resp = HttpResponse::newHttpResponse();
	resp->setBody("Hello, World!");
	resp->setStatusCode(drogon::HttpStatusCode::k200OK);
	callback(resp);
}

void PKReadCtrl::pkRead(const HttpRequestPtr &req,
												std::function<void(const HttpResponsePtr &)> &&callback,
												const std::string &db, const std::string &table) {
	std::string_view reqBody = std::string_view(req->getBody().data());
	PKReadParams reqStruct;
	reqStruct.path.db = db;
	reqStruct.path.table = table;

	int error = 0;
	try 
	{
		error = json_parser::parse(reqBody, reqStruct, app().getCurrentThreadIndex());
	}
	catch(const std::exception& e)
	{
		auto resp = HttpResponse::newHttpResponse();
		resp->setBody(e.what());
		resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
		callback(resp);
		return;
	}

	try
	{
		reqStruct.validate();
	}
	catch(const std::exception& e)
	{
		auto resp = HttpResponse::newHttpResponse();
		resp->setBody(e.what());
		resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
		callback(resp);
		return;
	}

	if (error == simdjson::SUCCESS) {
		RS_BufferManager reqBuffManager = RS_BufferManager(globalConfig.internal.bufferSize);
		RS_BufferManager respBuffManager = RS_BufferManager(globalConfig.internal.bufferSize);
		RS_Buffer* reqBuff = reqBuffManager.getBuffer();
		RS_Buffer* respBuff = respBuffManager.getBuffer();

		create_native_request(reqStruct, reqBuff->buffer, respBuff->buffer);
		uintptr_t length_ptr = (uintptr_t)reqBuff->buffer + PK_REQ_LENGTH_IDX * ADDRESS_SIZE;
		uint32_t* length_ptr_casted = reinterpret_cast<uint32_t*>(length_ptr);
		reqBuff->size = *length_ptr_casted;

		// pk_read
		RS_Status status = pk_read(reqBuff, respBuff);  

		auto resp = HttpResponse::newHttpResponse(static_cast<HttpStatusCode>(status.http_code), drogon::CT_APPLICATION_JSON);

		if (status.http_code != drogon::HttpStatusCode::k200OK) {
			std::string msg = status.message;
			resp->setBody(msg);
		} else {
			// convert resp to json
			char* respData = respBuff->buffer;

			PKReadResponseJSON respJson;
			respJson.init();
			process_pkread_response(respData, respJson);

			resp->setBody(respJson.to_string());
		}
		callback(resp);
		return;
	} else {
		printf("Operation failed\n");
		auto resp = HttpResponse::newHttpResponse();
		resp->setBody("NACK");
		resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
		callback(resp);
		return;
	}
}
