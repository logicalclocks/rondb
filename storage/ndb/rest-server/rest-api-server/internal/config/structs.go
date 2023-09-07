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

package config

import (
	"encoding/json"
	"errors"
	"fmt"

	"hopsworks.ai/rdrs/internal/log"
)

type Internal struct {
	BufferSize          uint32
	PreAllocatedBuffers uint32
	GOMAXPROCS          int
	BatchMaxSize        uint32
	OperationIDMaxSize  uint32
}

func (i *Internal) Validate() error {
	if i.PreAllocatedBuffers == 0 {
		log.Warnf("PreAllocatedBuffers is set to 0. It may impact performance")
	}

	if i.BufferSize < 256 {
		return errors.New("BufferSize is too low")
	}

	return nil
}

type GRPC struct {
	Enable     bool
	ServerIP   string
	ServerPort uint16
}

func (g *GRPC) Validate() error {
	if g.Enable {
		if g.ServerIP == "" {
			return errors.New("the gRPC server IP cannot be empty")
		} else if g.ServerPort == 0 {
			return errors.New("the gRPC server port cannot be empty")
		}
	}
	return nil
}

type REST struct {
	Enable     bool
	ServerIP   string
	ServerPort uint16
}

func (g *REST) Validate() error {
	if g.Enable {
		if g.ServerIP == "" {
			return errors.New("the REST server IP cannot be empty")
		} else if g.ServerPort == 0 {
			return errors.New("the REST server port cannot be empty")
		}
	}
	return nil
}

type MySQLServer struct {
	IP   string
	Port uint16
}

func (m *MySQLServer) Validate() error {
	if m.IP == "" {
		return errors.New("MySQL server IP cannot be empty")
	} else if m.Port == 0 {
		return errors.New("MySQL server port cannot be empty")
	}
	return nil
}

type Testing struct {
	MySQL                MySQL
	MySQLMetadataCluster MySQL
}

func (t *Testing) Validate() error {
	if err := t.MySQL.Validate(); err != nil {
		return err
	}

	if len(t.MySQLMetadataCluster.Servers) == 0 {
		t.MySQLMetadataCluster.Servers = t.MySQL.Servers
	}
	if err := t.MySQLMetadataCluster.Validate(); err != nil {
		return err
	}

	return nil
}

type MySQL struct {
	Servers  []MySQLServer
	User     string
	Password string
}

func (t Testing) GenerateMysqldConnectStringDataCluster() string {
	// user:password@tcp(IP:Port)/
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		t.MySQL.User,
		t.MySQL.Password,
		t.MySQL.Servers[0].IP,
		t.MySQL.Servers[0].Port)
}

func (t Testing) GenerateMysqldConnectStringMetadataCluster() string {
	// user:password@tcp(IP:Port)/
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		t.MySQLMetadataCluster.User,
		t.MySQLMetadataCluster.Password,
		t.MySQLMetadataCluster.Servers[0].IP,
		t.MySQLMetadataCluster.Servers[0].Port)
}

func (m *MySQL) Validate() error {

	if len(m.Servers) < 1 {
		return errors.New("at least one MySQL server has to be defined")
	} else if len(m.Servers) > 1 {
		return errors.New("we do not support specifying more than one MySQL server yet")
	}

	for _, server := range m.Servers {
		if err := server.Validate(); err != nil {
			return err
		}
	}

	if m.User == "" {
		return errors.New("the MySQL user cannot be empty")
	}
	return nil
}

type Mgmd struct {
	IP   string
	Port uint16
}

func (m *Mgmd) Validate() error {
	if m.IP == "" {
		return errors.New("the Management server IP cannot be empty")
	} else if m.Port == 0 {
		return errors.New("the Management server port cannot be empty")
	}
	return nil
}

type RonDB struct {
	Mgmds []Mgmd

	// Connection pool size. Default 1
	// Note current implementation only supports 1 connection
	// TODO JIRA RonDB-245
	ConnectionPoolSize uint32

	// This is the list of node ids to force the connections to be assigned to specific node ids.
	// If this property is specified and connection pool size is not the default,
	// the number of node ids must match the connection pool size
	NodeIDs []uint32

	// Connection retry attempts.
	ConnectionRetries         uint32
	ConnectionRetryDelayInSec uint32

	// Transient error retry count and initial delay
	OpRetryOnTransientErrorsCount uint32
	OpRetryInitialDelayInMS       uint32
	OpRetryJitterInMS             uint32
}

func (r *RonDB) Validate() error {
	if len(r.Mgmds) < 1 {
		return errors.New("at least one Management server has to be defined")
	} else if len(r.Mgmds) > 1 {
		return errors.New("we do not support specifying more than one Management server yet")
	}
	for _, server := range r.Mgmds {
		if err := server.Validate(); err != nil {
			return err
		}
	}

	if r.ConnectionPoolSize < 1 || r.ConnectionPoolSize > 1 {
		return errors.New("wrong connection pool size. Currently only single RonDB connection is supported")
	}

	if r.NodeIDs != nil && len(r.NodeIDs) != 0 && len(r.NodeIDs) != int(r.ConnectionPoolSize) {
		return errors.New("wrong number of NodeIDs. The number of node ids must match the connection pool size")
	} else if r.NodeIDs == nil || len(r.NodeIDs) == 0 {
		r.NodeIDs = []uint32{uint32(0)}
	}

	return nil
}

func (r RonDB) GenerateMgmdConnectString() string {
	mgmd := r.Mgmds[0]
	return fmt.Sprintf("%s:%d",
		mgmd.IP,
		mgmd.Port,
	)
}

type TestParameters struct {
	ClientCertFile string
	ClientKeyFile  string
}

func (t *TestParameters) Validate() error {
	return nil
}

type APIKey struct {
	UseHopsworksAPIKeys          bool
	CacheRefreshIntervalMS       uint32
	CacheUnusedEntriesEvictionMS uint32
	CacheRefreshIntervalJitterMS uint32
}

func (a *APIKey) Validate() error {
	if a.CacheRefreshIntervalMS == 0 {
		return errors.New("CacheRefreshIntervalMS cannot be 0")
	}

	if a.CacheUnusedEntriesEvictionMS == 0 {
		return errors.New("CacheUnusedEntriesEvictionMS cannot be 0")
	}

	if a.CacheRefreshIntervalMS > a.CacheUnusedEntriesEvictionMS {
		return errors.New("CacheRefreshIntervalMS can not be more that CacheUnusedEntriesEvictionMS")
	}

	if a.CacheRefreshIntervalJitterMS >= a.CacheRefreshIntervalMS {
		return errors.New("CacheRefreshIntervalJitterMS must be smaller than CacheRefreshIntervalMS")
	}

	return nil
}

type TLS struct {
	EnableTLS                  bool
	RequireAndVerifyClientCert bool
	CertificateFile            string
	PrivateKeyFile             string
	RootCACertFile             string
	TestParameters             TestParameters
}

func (t *TLS) Validate() error {
	if IsUnitTest() {
		// In case of unit tests, we create dummy certificates
		return nil
	}

	if t.EnableTLS {
		if t.CertificateFile == "" || t.PrivateKeyFile == "" {
			return errors.New("cannot enable TLS if `CertificateFile` or `PrivateKeyFile` is not set")
		}
	} else {
		if t.RequireAndVerifyClientCert {
			return errors.New("cannot require client certs if TLS is not enabled")
		}
	}

	return t.TestParameters.Validate()
}

type Security struct {
	TLS    TLS
	APIKey APIKey
}

func (c *Security) Validate() error {
	err := c.TLS.Validate()
	if err != nil {
		return err
	}

	err = c.APIKey.Validate()
	if err != nil {
		return err
	}

	return nil
}

/*
The RDRS is tested on a regular basis by RonDB's MTR tests. These MTR tests
have a config file defined for the RDRS in `mysql-test/suite/rdrs/include/have_rdrs.inc`.

Therefore, when committing a change to this struct or its defaults, change
the corresponding file for the MTR tests as well.
*/
type AllConfigs struct {
	Internal             Internal
	REST                 REST
	GRPC                 GRPC
	RonDB                RonDB
	RonDBMetadataCluster RonDB
	Security             Security
	Log                  log.LogConfig
	Testing              Testing
}

func (c *AllConfigs) Validate() error {
	var err error
	if err = c.GRPC.Validate(); err != nil {
		return err
	} else if err = c.REST.Validate(); err != nil {
		return err
	} else if err = c.RonDB.Validate(); err != nil {
		return fmt.Errorf("Config.RonDB: %s", err)
	} else if err = c.Testing.Validate(); err != nil {
		return err
	} else if err = c.Security.Validate(); err != nil {
		return err
	}

	// c.RonDBMetaCluster is optional. Copy the cluster
	// connection information from c.RonDB if it is not
	// set by the user
	if len(c.RonDBMetadataCluster.Mgmds) == 0 {
		c.RonDBMetadataCluster.Mgmds = c.RonDB.Mgmds
	}

	if err = c.RonDBMetadataCluster.Validate(); err != nil {
		return fmt.Errorf("Config.RonDBMetadataCluster: %s", err)
	}

	return nil
}

func (c AllConfigs) String() string {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Sprintf("error marshaling configs; error: %v", err)
	}
	return string(b)
}
