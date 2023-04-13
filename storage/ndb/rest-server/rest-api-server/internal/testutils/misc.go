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

package testutils

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
)

const HOPSWORKS_TEST_API_KEY = "bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"

var WithRonDB = flag.Bool("with-rondb", true, "test with a running RonDB instance")

func CreateMySQLConnection() (*sql.DB, error) {
	conf := config.GetAll()
	connectionString := conf.Testing.GenerateMysqldConnectString()
	log.Debugf("Connecting to mysqld with '%s'", connectionString)
	dbConnection, err := sql.Open("mysql", connectionString)
	if err != nil {
		err = fmt.Errorf("failed to connect to db; error: %w", err)
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
