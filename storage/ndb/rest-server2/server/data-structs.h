#ifndef  DATA_STRUCTS
#define  DATA_STRUCTS

#include <string>
#include <vector>

struct Filter {
    std::string Column;
    std::string Value;
};

struct ReadColumn {
    std::string Column;
    std::string DataReturnType;
};

struct PKReadBody {
    std::vector<Filter> Filters;
    std::vector<ReadColumn> ReadColumns;
    std::string OperationID;
};

struct BatchSubOp {
    std::string Method;
    std::string RelativeURL;
    PKReadBody Body;
};

struct BatchOpRequest {
    std::vector<BatchSubOp> Operations;
};

#endif
