#ifndef  DATA_STRUCTS
#define  DATA_STRUCTS

#include <string>
#include <vector>

using namespace std;

struct Filter {
    string Column;
    string Value;
};

struct ReadColumn {
    string Column;
    string DataReturnType;
};

struct PKReadBody {
    vector<Filter> Filters;
    vector<ReadColumn> ReadColumns;
    string OperationID;
};

struct BatchSubOp {
    string Method;
    string RelativeURL;
    PKReadBody Body;
};

struct BatchOpRequest {
    vector<BatchSubOp> Operations;
};

#endif
