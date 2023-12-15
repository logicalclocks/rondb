#ifndef CONFIG_STRUCTS_HPP
#define CONFIG_STRUCTS_HPP

#include <string>
#include "log.hpp"
#include <mutex>

class AllConfigs;

extern AllConfigs globalConfig;
extern std::mutex globalConfigMutex;

class Internal {
public:
	uint32_t bufferSize;
	uint32_t preAllocatedBuffers;
	int GOMAXPROCS;
	uint32_t batchMaxSize;
	uint32_t operationIdMaxSize;
	void validate();
	Internal();
	Internal(uint32_t, uint32_t, int, uint32_t, uint32_t);
};

class GRPC {
public:
	bool enable;
	std::string serverIP;
	uint16_t serverPort;
	void validate();
	GRPC();
	GRPC(bool, std::string, uint16_t);
};

class REST {
public:
	bool enable;
	std::string serverIP;
	uint16_t serverPort;
	void validate();
	REST();
	REST(bool, std::string, uint16_t);
};

class MySQLServer {
public:
	std::string IP;
	uint16_t port;
	void validate();
	MySQLServer();
	MySQLServer(std::string, uint16_t);
};

class MySQL {
public:
	std::vector<MySQLServer> servers;
	std::string user;
	std::string password;
	void validate();
	MySQL();
	MySQL(std::vector<MySQLServer>, std::string, std::string);
};

class Testing {
public:
	MySQL mySQL;
	MySQL mySQLMetadataCluster;
	void validate();
	std::string generate_mysqld_connect_string_data_cluster();
	std::string generate_mysqld_connect_string_metadata_cluster();
	Testing();
	Testing(MySQL, MySQL);
};

class Mgmd {
public:
	std::string IP;
	uint16_t port;
	void validate();
	Mgmd();
	Mgmd(std::string, uint16_t);
};

class RonDB {
public:
	std::vector<Mgmd> Mgmds;

	// Connection pool size. Default 1
	// Note current implementation only supports 1 connection
	// TODO JIRA RonDB-245
	uint32_t connectionPoolSize;

	// This is the list of node ids to force the connections to be assigned to specific node ids.
	// If this property is specified and connection pool size is not the default,
	// the number of node ids must match the connection pool size
	std::vector<uint32_t> nodeIDs;

	// Connection retry attempts.
	uint32_t connectionRetries;
	uint32_t connectionRetryDelayInSec;

	// Transient error retry count and initial delay
	uint32_t opRetryOnTransientErrorsCount;
	uint32_t opRetryInitialDelayInMS;
	uint32_t opRetryJitterInMS;
	void validate();
	std::string generate_Mgmd_connect_string();
	RonDB();
	RonDB(std::vector<Mgmd>, uint32_t, std::vector<uint32_t>, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t);
};

class TestParameters {
public:
	std::string clientCertFile;
	std::string clientKeyFile;
	TestParameters();
	TestParameters(std::string, std::string);
};

class APIKey {
public:
	bool useHopsworksAPIKeys;
	uint32_t cacheRefreshIntervalMS;
	uint32_t cacheUnusedEntriesEvictionMS;
	uint32_t cacheRefreshIntervalJitterMS;
	void validate();
	APIKey();
	APIKey(bool, uint32_t, uint32_t, uint32_t);
};

class TLS {
public:
	bool enableTLS;
	bool requireAndVerifyClientCert;
	std::string certificateFile;
	std::string privateKeyFile;
	std::string rootCACertFile;
	TestParameters testParameters;
	void validate();
	TLS();
	TLS(bool, bool, std::string, std::string, std::string, TestParameters);
};

class Security {
public:
	TLS tls;
	APIKey apiKey;
	void validate();
	Security();
	Security(TLS, APIKey);
};

class AllConfigs {
public:
	Internal internal;
	REST rest;
	GRPC grpc;
	RonDB ronDB;
	RonDB ronDbMetaDataCluster;
	Security security;
	LogConfig log;
	Testing testing;
	void validate();
	std::string string();
	AllConfigs();
	AllConfigs(Internal, REST, GRPC, RonDB, RonDB, Security, LogConfig, Testing);
	static AllConfigs getAll();
	static void setAll(AllConfigs newConfig);
	static void setToDefaults();
};

#endif