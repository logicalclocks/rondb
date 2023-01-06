package config

import (
	"encoding/json"
	"errors"
	"fmt"

	"hopsworks.ai/rdrs/internal/log"
)

type Internal struct {
	BufferSize          int
	PreAllocatedBuffers uint32
	GOMAXPROCS          int
}

type GRPC struct {
	ServerIP   string
	ServerPort uint16
}

type REST struct {
	ServerIP   string
	ServerPort uint16
}

// TODO: Replace with slice of mysqlds
type MySQLServer struct {
	IP       string
	Port     uint16
	User     string
	Password string
}

// TODO: Replace with slice of mgmds
type RonDB struct {
	MgmdIP   string
	MgmdPort uint16
}

type Security struct {
	EnableTLS                        bool
	RequireAndVerifyClientCert       bool
	CertificateFile                  string
	PrivateKeyFile                   string
	RootCACertFile                   string
	UseHopsworksAPIKeys              bool
	HopsworksAPIKeysCacheValiditySec int
}

func (c Security) Validate() error {
	if c.EnableTLS {
		if c.CertificateFile == "" || c.PrivateKeyFile == "" {
			return errors.New("Server Certificate/Key not set")
		}
	}
	return nil
}

type AllConfigs struct {
	Internal    Internal
	REST        REST
	GRPC        GRPC
	RonDB       RonDB
	MySQLServer MySQLServer
	Security    Security
	Log         log.LogConfig
}

func (c AllConfigs) Validate() error {
	return c.Security.Validate()
}

func (c AllConfigs) String() string {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Sprintf("error marshaling configs; error: %v", err)
	}
	return string(b)
}
