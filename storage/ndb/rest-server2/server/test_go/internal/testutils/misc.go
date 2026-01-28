/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

package testutils

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/log"
)

const HOPSWORKS_TEST_API_KEY = "bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"

var WithRonDB = flag.Bool("with-rondb", true, "test with a running RonDB instance")

func CreateMySQLConnectionDataCluster() (*sql.DB, error) {
	conf := config.GetAll()
	connectionString := conf.Testing.GenerateMysqldConnectStringDataCluster()
	log.Debugf("Connecting to data mysqld with '%s'", connectionString)
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		err = fmt.Errorf("failed to connect to data db; error: %w", err)
	}
	return dbConnection, err
}

func CreateMySQLConnectionMetadataCluster() (*sql.DB, error) {
	conf := config.GetAll()
	connectionString := conf.Testing.GenerateMysqldConnectStringMetadataCluster()
	log.Debugf("Connecting to metadata mysqld with '%s'", connectionString)
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		err = fmt.Errorf("failed to connect to metadata db; error: %w", err)
	}
	return dbConnection, err
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandString(n int) string {
	b := make([]rune, n)
	ra := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	for i := range b {
		b[i] = letterRunes[ra.Intn(len(letterRunes))]
	}
	return string(b)
}
