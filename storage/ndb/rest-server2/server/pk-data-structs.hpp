#ifndef PK_DATA_STRUCTS_HPP
#define PK_DATA_STRUCTS_HPP

#include <string>
#include <vector>
#include "src/rdrs-dal.h"
#include <map>
#include <iostream>
#include <sstream>

std::string to_string(DataReturnType drt) {
	switch (drt) {
			case DataReturnType::DEFAULT_DRT:
					return "default";
			default:
					return "unknown";
	}
}

class PKReadFilter {
public:
	std::string_view column;
	std::vector<char> value;
};

class PKReadReadColumn {
public:
	std::string_view column;
	std::string_view returnType;
};

class PKReadPath {
public:
	std::string_view db; // json:"db" uri:"db"  binding:"required,min=1,max=64"
	std::string_view table; // Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"
};

class PKReadParams {
public:
	PKReadPath path;
	std::vector<PKReadFilter> filters;
	std::vector<PKReadReadColumn> readColumns;
	std::string_view operationId;
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
    virtual std::string toString() const = 0;

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

		std::string toString() const override {
			std::stringstream ss;
			ss << "{ \"operation_id\": \"" << operationID << "\", \"data\": {";
			for (auto& [column, value] : data) {
					ss << "\"" << column << "\": \"" << std::string(value.begin(), value.end()) << "\",";
			}
			ss << "} }";
			return ss.str();
		}
		
		std::string getOperationID() const {
			return operationID;
		}
		
		std::map<std::string, std::vector<char>> getData() const {
			return data;
		}
};

#endif // PK_DATA_STRUCTS_HPP