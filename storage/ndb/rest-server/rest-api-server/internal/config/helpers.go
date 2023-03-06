package config

import "fmt"

func GenerateMysqldConnectString(conf AllConfigs) string {
	server := conf.MySQL.Servers[0]
	// user:password@tcp(IP:Port)/
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		conf.MySQL.User,
		conf.MySQL.Password,
		server.IP,
		server.Port)
}

func GenerateMgmdConnectString(conf AllConfigs) string {
	mgmd := conf.RonDB.Mgmds[0]
	return fmt.Sprintf("%s:%d",
		mgmd.IP,
		mgmd.Port,
	)
}
