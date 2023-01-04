package config

import "fmt"

func GenerateConnectionString(conf AllConfigs) string {
	// user:password@tcp(IP:Port)/
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		conf.MySQLServer.User,
		conf.MySQLServer.Password,
		conf.MySQLServer.IP,
		conf.MySQLServer.Port)
}
