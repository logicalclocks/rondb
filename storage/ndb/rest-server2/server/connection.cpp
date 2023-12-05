#include "connection.hpp"
#include "config_structs.hpp"
#include "src/rdrs-dal.h"
#include <drogon/HttpTypes.h>
#include "error.hpp"

void RonDBConnection::init_rondb_connection(RonDB& rondbDataCluster,
	RonDB& rondbMetaDataCluster) {
		// init RonDB client API
		std::cout << "Initializing RonDB connection" << std::endl;

		RS_Status ret = init();
		if (ret.http_code != drogon::HttpStatusCode::k200OK) {
			throw DalError(ret);
		}

		// Connect to data cluster
		std::string csd = rondbDataCluster.generate_Mgmd_connect_string();
		
		std::unique_ptr<unsigned int[]> dataClusterNodeIDsMem(new unsigned int[rondbDataCluster.nodeIDs.size()]);

		for (size_t i = 0; i < rondbDataCluster.nodeIDs.size(); ++i) {
				dataClusterNodeIDsMem[i] = static_cast<unsigned int>(rondbDataCluster.nodeIDs[i]);
		}

		ret = add_data_connection(
			csd.c_str(),
			rondbDataCluster.connectionPoolSize,
			dataClusterNodeIDsMem.get(),
			rondbDataCluster.nodeIDs.size(),
			rondbDataCluster.connectionRetries,
			rondbDataCluster.connectionRetryDelayInSec
		);

		if (ret.http_code != drogon::HttpStatusCode::k200OK) {
			throw DalError(ret);
		}

		ret = set_data_cluster_op_retry_props(
			rondbDataCluster.opRetryOnTransientErrorsCount,
			rondbDataCluster.opRetryInitialDelayInMS,
			rondbDataCluster.opRetryJitterInMS
		);
		if (ret.http_code != drogon::HttpStatusCode::k200OK) {
			throw DalError(ret);
		}

		// Connect to metadata cluster
		std::string csmd = rondbMetaDataCluster.generate_Mgmd_connect_string();

		std::unique_ptr<unsigned int[]> metaClusterNodeIDsMem(new unsigned int[rondbMetaDataCluster.nodeIDs.size()]);
		for (size_t i = 0; i < rondbMetaDataCluster.nodeIDs.size(); ++i) {
			metaClusterNodeIDsMem[i] = static_cast<unsigned int>(rondbMetaDataCluster.nodeIDs[i]);
		}

		ret = add_metadata_connection(
			csmd.c_str(),
			rondbMetaDataCluster.connectionPoolSize,
			metaClusterNodeIDsMem.get(),
			rondbMetaDataCluster.nodeIDs.size(),
			rondbMetaDataCluster.connectionRetries,
			rondbMetaDataCluster.connectionRetryDelayInSec
		);

		if (ret.http_code != drogon::HttpStatusCode::k200OK) {
			throw DalError(ret);
		}

		ret = set_metadata_cluster_op_retry_props(
			rondbMetaDataCluster.opRetryOnTransientErrorsCount,
			rondbMetaDataCluster.opRetryInitialDelayInMS,
			rondbMetaDataCluster.opRetryJitterInMS
		);

		if (ret.http_code != drogon::HttpStatusCode::k200OK) {
			throw DalError(ret);
		}
	}

	void RonDBConnection::shutdown_rondb_connection() {
		std::cout << "Shutting down RonDB connection" << std::endl;
		RS_Status ret = shutdown_connection();
		if (ret.http_code != drogon::HttpStatusCode::k200OK) {
			throw DalError(ret);
		}
	}

	void RonDBConnection::rondb_reconnect() {
		std::cout << "Restarting RonDB connection" << std::endl;
		RS_Status ret = reconnect();
		if (ret.http_code != drogon::HttpStatusCode::k200OK) {
			throw DalError(ret);
		}
	}
