#ifndef PK_DATA_STRUCTS_HPP
#define PK_DATA_STRUCTS_HPP

#include <string>
#include <vector>
#include "src/rdrs-dal.h"
#include <map>
#include <iostream>
#include <sstream>
#include <iomanip>
#include "error.hpp"
#include "config_structs.hpp"
#include <unordered_map>
#include <codecvt> // for std::codecvt_utf8
#include <locale>  // for std::wstring_convert

std::string to_string(DataReturnType);

uint32_t decode_utf8_to_unicode(const std::string&, size_t&);

void validate_db_identifier(std::string);

void validate_operation_id(std::string);

class PKReadFilter {
public:
	std::string column;
	std::vector<char> value;
	void validate();
};

class PKReadReadColumn {
public:
	std::string column;
	std::string returnType;
};

class PKReadPath {
public:
	std::string db; // json:"db" uri:"db"  binding:"required,min=1,max=64"
	std::string table; // Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"
};

class PKReadParams {
public:
	PKReadPath path;
	std::vector<PKReadFilter> filters;
	std::vector<PKReadReadColumn> readColumns;
	std::string operationId;
	std::string to_string();
	void validate();
};

struct Column {
    std::string name;
    std::vector<char> value; // Byte array for the value
};

class PKReadResponse {
public:
	PKReadResponse() = default; // Default constructor
	virtual void init() = 0;
	virtual void setOperationID(std::string& opID) = 0;
	virtual void setColumnData(std::string& column, const std::vector<char>& value) = 0;
	virtual std::string to_string() const = 0;

	virtual ~PKReadResponse() = default;
};

class PKReadResponseJSON : public PKReadResponse {
private:
	std::string operationID;
	std::map<std::string, std::vector<char>> data;

public:
	PKReadResponseJSON() : PKReadResponse() {}

	PKReadResponseJSON(const PKReadResponseJSON& other) : PKReadResponse() {
		operationID = other.operationID;
		data = other.data;
	}

	PKReadResponseJSON& operator=(const PKReadResponseJSON& other) {	
		operationID = other.operationID;
		data = other.data;
		return *this;
	}

	void init() override {
		operationID.clear();
		data.clear();
	}

	void setOperationID(std::string& opID) override {
		operationID = opID;
	}

	void setColumnData(std::string& column, const std::vector<char>& value) override {
		data[column] = value;
	}

	std::string getOperationID() const {
		return operationID;
	}
	
	std::map<std::string, std::vector<char>> getData() const {
		return data;
	}
	
	std::string to_string() const override;
};

#endif // PK_DATA_STRUCTS_HPP