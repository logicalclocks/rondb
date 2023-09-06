/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package dal

/*
 #include <stdlib.h>
 #include "./../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"net/http"
	"unsafe"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
)

func InitRonDBConnection(rondbDataCluster config.RonDB,
	rondbMetaDataCluster config.RonDB) *DalError {

	// init RonDB client API
	log.Info("Initialising RonDB connection")
	ret := C.init()
	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	// Connect to data cluster
	csd := C.CString(rondbDataCluster.GenerateMgmdConnectString())
	defer C.free(unsafe.Pointer(csd))

	dataClusterNodeIDsMem := C.malloc(C.size_t(len(rondbDataCluster.NodeIDs)) * C.size_t(C.sizeof_uint))
	defer C.free(dataClusterNodeIDsMem)
	sDataClusterNodeIDsMem := unsafe.Slice((*C.uint)(dataClusterNodeIDsMem), len(rondbDataCluster.NodeIDs))
	for i, node_id := range rondbDataCluster.NodeIDs {
		sDataClusterNodeIDsMem[i] = C.uint(node_id)
	}
	ret = C.add_data_connection(csd,
		C.uint(rondbDataCluster.ConnectionPoolSize),
		(*C.uint)(dataClusterNodeIDsMem),
		C.uint(len(rondbDataCluster.NodeIDs)),
		C.uint(rondbDataCluster.ConnectionRetries),
		C.uint(rondbDataCluster.ConnectionRetryDelayInSec))
	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	// set failed ops retry properties for data cluster
	ret = C.set_op_retry_props_data_cluster(
		C.uint(rondbDataCluster.OpRetryOnTransientErrorsCount),
		C.uint(rondbDataCluster.OpRetryInitialDelayInMS),
		C.uint(rondbDataCluster.OpRetryJitterInMS))
	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	// Connect to metadata cluster
	csmd := C.CString(rondbMetaDataCluster.GenerateMgmdConnectString())
	defer C.free(unsafe.Pointer(csmd))

	metadataClusterNodeIDsMem := C.malloc(C.size_t(len(rondbMetaDataCluster.NodeIDs)) * C.size_t(C.sizeof_uint))
	defer C.free(metadataClusterNodeIDsMem)
	sMetadataClusterNodeIDsMem := unsafe.Slice((*C.uint)(metadataClusterNodeIDsMem), len(rondbMetaDataCluster.NodeIDs))
	for i, node_id := range rondbMetaDataCluster.NodeIDs {
		sMetadataClusterNodeIDsMem[i] = C.uint(node_id)
	}
	ret = C.add_metadata_connection(csmd,
		C.uint(rondbMetaDataCluster.ConnectionPoolSize),
		(*C.uint)(metadataClusterNodeIDsMem),
		C.uint(len(rondbMetaDataCluster.NodeIDs)),
		C.uint(rondbMetaDataCluster.ConnectionRetries),
		C.uint(rondbMetaDataCluster.ConnectionRetryDelayInSec))
	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	// set failed ops retry properties for metadata cluster
	ret = C.set_op_retry_props_metadata_cluster(
		C.uint(rondbMetaDataCluster.OpRetryOnTransientErrorsCount),
		C.uint(rondbMetaDataCluster.OpRetryInitialDelayInMS),
		C.uint(rondbMetaDataCluster.OpRetryJitterInMS))
	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}
	return nil
}

func ShutdownConnection() *DalError {
	log.Info("Shutting down RonDB connection")
	ret := C.shutdown_connection()

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}
	return nil
}

func Reconnect() *DalError {
	log.Info("Restarting RonDB connection")
	ret := C.reconnect()
	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}
	return nil
}
