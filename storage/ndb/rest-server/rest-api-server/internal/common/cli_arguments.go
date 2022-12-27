package common

import "flag"

var Version = flag.Bool("version", false, "Print API and application version")
var ConfigFile = flag.String("config", "", "Configuration file path")
