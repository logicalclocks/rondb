package testutils

import (
	"fmt"
	"strings"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/version"
)

func NewPingURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
		config.PING_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewStatURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
		config.STAT_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewPKReadURL(db string, table string) string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d%s%s",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		config.DB_OPS_EP_GROUP,
		config.PK_DB_OPERATION,
	)
	url = strings.Replace(url, ":"+config.DB_PP, db, 1)
	url = strings.Replace(url, ":"+config.TABLE_PP, table, 1)
	appendURLProtocol(&url)
	return url
}

func NewBatchReadURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
		config.BATCH_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func appendURLProtocol(url *string) {
	conf := config.GetAll()
	if conf.Security.EnableTLS {
		*url = fmt.Sprintf("https://%s", *url)
	} else {
		*url = fmt.Sprintf("http://%s", *url)
	}
}
