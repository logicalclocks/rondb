#include "pk_data_structs.hpp"

std::string to_string(DataReturnType drt) {
	switch (drt) {
		case DataReturnType::DEFAULT_DRT:
			return "default";
		default:
			return "unknown";
	}
}

uint32_t decode_utf8_to_unicode(const std::string &str, size_t &i) {
	uint32_t codepoint = 0;
	if ((str[i] & 0x80) == 0) {
			// 1-byte character
			codepoint = str[i];
	} else if ((str[i] & 0xE0) == 0xC0) {
			// 2-byte character
			codepoint = (str[i] & 0x1F) << 6 | (str[i + 1] & 0x3F);
			i += 1;
	} else if ((str[i] & 0xF0) == 0xE0) {
			// 3-byte character
			codepoint = (str[i] & 0x0F) << 12 | (str[i + 1] & 0x3F) << 6 | (str[i + 2] & 0x3F);
			i += 2;
	} else if ((str[i] & 0xF8) == 0xF0) {
			// 4-byte character
			codepoint = (str[i] & 0x07) << 18 | (str[i + 1] & 0x3F) << 12 | (str[i + 2] & 0x3F) << 6 | (str[i + 3] & 0x3F);
			i += 3;
	}
	return codepoint;
}

void validate_db_identifier(std::string identifier) {
	if (identifier.empty()) {
		throw ValidationError("identifier is empty");
	} else if (identifier.length() > 64) {
		throw ValidationError("identifier is too large: " + identifier);
	}

	// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
	// ASCII: U+0001 .. U+007F
	// Extended: U+0080 .. U+FFFF
	for (size_t i = 0; i < identifier.length(); ++i) {
		uint32_t code = decode_utf8_to_unicode(identifier, i);		
		
		if (!((code >= 0x01 && code <= 0x7F) || (code >= 0x80 && code <= 0xFFFF))) {
			throw ValidationError("identifier carries an invalid character: " + std::to_string(code));
		}			
	}
}

void validate_operation_id(std::string opId) {
	uint32_t operationIdMaxSize = AllConfigs::getAll().internal.operationIdMaxSize;
	if (static_cast<uint32_t>(opId.length()) > operationIdMaxSize) {
		throw ValidationError("max allowed length is " + std::to_string(operationIdMaxSize));
	}
}

void PKReadFilter::validate() {
	if (column.empty()) {
		throw ValidationError("filter column name is invalid; ");
	}
	// make sure the column name is not null
	if (std::string(column.begin(), column.end()) == "null") {
		throw ValidationError("Field validation for 'Column' failed on the 'required' tag");
	}

	// make sure the column name is valid
	try
	{
		validate_db_identifier(column);
	}
	catch(const std::exception& e)
	{
		std::string errorMessage = e.what();
		if (errorMessage == "identifier is empty")
			throw ValidationError("Field validation for 'Column' failed on the 'min' tag': " + column);
		else if (errorMessage == "identifier is too large: " + column)
			throw ValidationError("Field validation for 'Column' failed on the 'max' tag': " + column);
		else if (errorMessage.find("carries an invalid character") != std::string::npos)
			throw ValidationError(errorMessage);
		else
			throw ValidationError("column name is invalid; error: " + errorMessage);
	}
	
	if (value.empty()) {
		throw ValidationError("Value cannot be empty");
	}

	// make sure the value is not null
	if (std::string(value.begin(), value.end()) == "null") {
		throw ValidationError("Field validation for 'Value' failed on the 'required' tag");
	}
}

std::string PKReadParams::to_string() {
	std::stringstream ss;
	ss << "PKReadParams: { path: { db: " << path.db << ", table: " << path.table << " }, filters: [";
	for (auto& filter : filters) {
		ss << "{ column: " << filter.column;
		ss << ", value with each byte separately: ";
		for (auto& byte : filter.value) {
			ss << byte << " ";
		}
		ss << "}, ";			
	}
	ss << "], readColumns: [";
	for (auto& readColumn : readColumns) {
		ss << "{ column: " << readColumn.column << ", returnType: " << readColumn.returnType << " }, ";
	}
	ss << "], operationId: " << operationId << " }";
	return ss.str();
}

void PKReadParams::validate() {
	try
	{
		validate_db_identifier(path.db);
	}
	catch(const ValidationError& e)
	{
		std::string errorMessage = e.what();
		if (errorMessage == "identifier is empty")
			throw ValidationError("Field validation for 'DB' failed on the 'min' tag': " + path.db);
		else if (errorMessage == "identifier is too large: " + path.db)
			throw ValidationError("Field validation for 'DB' failed on the 'max' tag': " + path.db);
		else if (errorMessage.find("carries an invalid character") != std::string::npos)
			throw ValidationError("db name: " + path.db + " contains invalid characters");
		else
			throw ValidationError("db name is invalid; error: " + errorMessage);
	}

	try
	{
		validate_db_identifier(path.table);
	}
	catch(const ValidationError& e)
	{
		std::string errorMessage = e.what();
		if (errorMessage == "identifier is empty")
			throw ValidationError("Field validation for 'Table' failed on the 'min' tag': " + path.table);
		else if (errorMessage == "identifier is too large: " + path.table)
			throw ValidationError("Field validation for 'Table' failed on the 'max' tag': " + path.table);
		else if (errorMessage.find("carries an invalid character") != std::string::npos)
			throw ValidationError("table name: " + path.table + " contains invalid characters");
		else
			throw ValidationError("table name is invalid; error: " + errorMessage);
	}

	try
	{
		validate_operation_id(operationId);
	}
	catch(const ValidationError& e)
	{
		throw ValidationError("operationId is invalid; error: " + std::string(e.what()));
	}

	// make sure filters is not empty
	if (filters.empty()) {
		throw ValidationError("Error: Field validation for 'Filters' failed");
	}

	// make sure filter columns are valid
	for (auto& filter : filters) {
		filter.validate();
	}

	// make sure that the columns are unique
	std::unordered_map<std::string, bool> existingFilters;
	for (auto& filter : filters) {
		if (existingFilters.find(filter.column) != existingFilters.end()) {
			throw ValidationError("field validation for filter failed on the 'unique' tag: " + filter.column);
		}
		existingFilters[filter.column] = true;
	}

	// make sure read columns are valid
	for (auto& readColumn : readColumns) {
		try
		{
			validate_db_identifier(readColumn.column);
		}
		catch(const ValidationError& e)
		{
			std::string errorMessage = e.what();
			std::cout << "error message: " << errorMessage << std::endl;
			if (errorMessage == "identifier is empty")
				throw ValidationError("Field validation for 'ReadColumn' failed on the 'min' tag': " + readColumn.column);
			else if (errorMessage == "identifier is too large: " + readColumn.column)
				throw ValidationError("identifier is too large: " + readColumn.column);
			else if (errorMessage.find("carries an invalid character") != std::string::npos)
				throw ValidationError("read column name: " + readColumn.column + " contains invalid characters");
			else
				throw ValidationError("read column name is invalid; error: " + errorMessage);
		}
	}

	// make sure that the filter columns and read columns do not overlap
	// and read cols are unique
	std::unordered_map<std::string, bool> existingCols;
	for (auto& readColumn : readColumns) {
		if (existingFilters.find(readColumn.column) != existingFilters.end()) {
			throw ValidationError("field validation for read columns failed. '" + readColumn.column + "' already included in filter");
		}

		if (existingCols.find(readColumn.column) != existingCols.end()) {
			throw ValidationError("field validation for 'ReadColumns' failed on the 'unique' tag: " + readColumn.column);
		} else {
			existingCols[readColumn.column] = true;
		}
	}		
}

std::string PKReadResponseJSON::to_string() const {
	std::stringstream ss;
	ss << "{" << std::endl;
	ss << "  \"operationId\": \"" << operationID << "\"," << std::endl;
	ss << "  \"data\": {";
	bool first = true;
	for (auto& [column, value] : data) {
		if (!first) {
			ss << ",";
		}
		first = false;
		ss << std::endl;
		ss << "    \"" << column << "\": ";
		if (value.empty()) {
			ss << "null";
		} else {
			ss << std::string(value.begin(), value.end());
		}
	}
	ss << std::endl << "  }" << std::endl;
	ss << "}" << std::endl;
	return ss.str();
}