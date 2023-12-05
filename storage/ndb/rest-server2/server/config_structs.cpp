#include "config_structs.hpp"

Internal::Internal() {
	bufferSize = 5 * 1024 * 1024;
	preAllocatedBuffers = 32;
	GOMAXPROCS = -1;
	batchMaxSize = 256;
	operationIdMaxSize = 256;
}

GRPC::GRPC() {
	enable = true;
	serverIP = "0.0.0.0";
	serverPort = 5406;
}

REST::REST() {
	enable = true;
	serverIP = "0.0.0.0";
	serverPort = 5406;
}

RonDB::RonDB() {
	Mgmds = {Mgmd()};
	connectionPoolSize = 1;
	connectionRetries = 5;
	connectionRetryDelayInSec = 5;
	opRetryOnTransientErrorsCount = 3;
	opRetryInitialDelayInMS = 100;
	opRetryJitterInMS = 100;
}

TestParameters::TestParameters() {
	clientCertFile = "";
	clientKeyFile = "";
}

Mgmd::Mgmd() {
	IP = "localhost";
	port = 1186;
}

TLS::TLS() {
	enableTLS = false;
	requireAndVerifyClientCert = false;
	certificateFile = "";
	privateKeyFile = "";
	rootCACertFile = "";
}

APIKey::APIKey() {
	useHopsworksAPIKeys = true;
	cacheRefreshIntervalMS = 10000;
	cacheUnusedEntriesEvictionMS = 60000;
	cacheRefreshIntervalJitterMS = 1000;
}

Security::Security() {
	tls = TLS();
	apiKey = APIKey();
}

Testing::Testing() {
	mySQL = MySQL();
	mySQLMetadataCluster = MySQL();
}

MySQL::MySQL() {
	servers = {MySQLServer()};
	user = "root";
	password = "";
}

MySQLServer::MySQLServer() {
	IP = "localhost";
	port = 3306;
}

void Mgmd::validate() {
	if (IP.empty()) {
		throw std::runtime_error("the Management server IP cannot be empty");
	}
	if (port == 0) {
		throw std::runtime_error("the Management server port cannot be empty");
	}
}

void RonDB::validate() {
	if (Mgmds.size() < 1) {
		throw std::runtime_error("at least one Management server has to be defined");
	} else if (Mgmds.size() > 1) {
		throw std::runtime_error("we do not support specifying more than one Management server yet");
	}
	for (auto server: Mgmds) {
		try
		{
			server.validate();
		}
		catch(const std::runtime_error& e)
		{
			throw e;
		}
	}

	if (connectionPoolSize < 1 || connectionPoolSize > 1) {
		throw std::runtime_error("wrong connection pool size. Currently only single RonDB connection is supported");
	}

	if (nodeIDs.size() > 0 && nodeIDs.size() != connectionPoolSize) {
		throw std::runtime_error("wrong number of NodeIDs. The number of node ids must match the connection pool size");
	} else if (nodeIDs.empty()) {
		nodeIDs = {0};
	}
}

std::string RonDB::generate_Mgmd_connect_string() {
	Mgmd mgmd = Mgmds[0];
	return mgmd.IP + ":" + std::to_string(mgmd.port);
}

AllConfigs::AllConfigs() {
	// Call default constructors
	internal = Internal();
	rest = REST();
	grpc = GRPC();
	ronDB = RonDB();
	ronDbMetaDataCluster = RonDB();
	security = Security();
	log = LogConfig();
	testing = Testing();
}