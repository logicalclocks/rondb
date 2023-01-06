package config

import "fmt"

func GenerateMysqldConnectString(conf AllConfigs) string {
	// user:password@tcp(IP:Port)/
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		conf.MySQLServer.User,
		conf.MySQLServer.Password,
		conf.MySQLServer.IP,
		conf.MySQLServer.Port)
}

func GenerateMgmdConnectString(conf AllConfigs) string {
	return fmt.Sprintf("%s:%d",
		conf.RonDB.MgmdIP,
		conf.RonDB.MgmdPort,
	)
}
