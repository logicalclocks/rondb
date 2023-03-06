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

type MySQLServer struct {
	IP   string
	Port uint16
}

type MySQL struct {
	Servers  []MySQLServer
	User     string
	Password string
}

func (m MySQL) Validate() error {
	if len(m.Servers) < 1 {
		return errors.New("at least one MySQL server has to be defined")
	} else if len(m.Servers) > 1 {
		return errors.New("we do not support specifying more than one MySQL server yet")
	}
	return nil
}

type Mgmd struct {
	IP   string
	Port uint16
}

type RonDB struct {
	Mgmds []Mgmd
}

func (r RonDB) Validate() error {
	if len(r.Mgmds) < 1 {
		return errors.New("at least one Management server has to be defined")
	} else if len(r.Mgmds) > 1 {
		return errors.New("we do not support specifying more than one Management server yet")
	}
	return nil
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
	Internal Internal
	REST     REST
	GRPC     GRPC
	RonDB    RonDB
	MySQL    MySQL
	Security Security
	Log      log.LogConfig
}

func (c AllConfigs) Validate() error {
	var err error
	if err = c.RonDB.Validate(); err != nil {
		return err
	} else if err = c.MySQL.Validate(); err != nil {
		return err
	} else if err = c.Security.Validate(); err != nil {
		return err
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
