#ifndef ERROR_HPP
#define ERROR_HPP

#include <string>
#include <exception>
#include <sstream>
#include "src/rdrs-dal.h"

class DalError: public std::exception {
private:
    int httpCode;
    std::string message;
    int errLineNo;
    std::string errFileName;

public:
    DalError(int code, const std::string& msg, int lineNo, const std::string& fileName) 
        : httpCode(code), message(msg), errLineNo(lineNo), errFileName(fileName) {}

		DalError(RS_Status status) {
			httpCode = status.http_code;
			message = status.message;
			errLineNo = status.err_line_no;
			errFileName = status.err_file_name;
		}

    virtual const char* what() const noexcept {
        return message.c_str();
    }

    std::string verboseError() const {
        std::ostringstream oss;
        oss << message << "; File: " << errFileName << ", Line: " << errLineNo;
        return oss.str();
    }

    int getHttpCode() const { return httpCode; }
    int getErrLineNo() const { return errLineNo; }
    std::string getErrFileName() const { return errFileName; }
};

#endif
