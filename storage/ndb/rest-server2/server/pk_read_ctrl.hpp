#ifndef PK_READ_CTRL_HPP
#define PK_READ_CTRL_HPP

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>

using namespace drogon;
class PKReadCtrl: public drogon::HttpController<PKReadCtrl> {
public:
	METHOD_LIST_BEGIN
	ADD_METHOD_TO(PKReadCtrl::ping,"/0.1.0/ping", Get);
	ADD_METHOD_TO(PKReadCtrl::pkRead,"/0.1.0/{db}/{table}/pk-read", Post);
	METHOD_LIST_END

	void ping(const HttpRequestPtr &req,
						std::function<void(const HttpResponsePtr &)> &&callback);

	void pkRead(const HttpRequestPtr &req,
							std::function<void(const HttpResponsePtr &)> &&callback,
							const std::string &db, const std::string &table);
};

#endif // PK_READ_CTRL_HPP
