/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation. The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
*/

package ronsqltpch

import (
	"fmt"
	"os"
	"runtime/debug"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
)

func TestMain(m *testing.M) {
	retcode := 0
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("caught a panic in main(); recovery: %s\nstacktrace:\n%s",
				r, debug.Stack())
			retcode = 1
		}
		os.Exit(retcode)
	}()

	conf := config.GetAll()
	log.InitLogger(conf.Log)

	if !*testutils.WithRonDB {
		return
	}

	// TPC-H tables are assumed to be already loaded by load_tpch.
	// When running via MTR, the .test file runs load_tpch before
	// and drops tables after the Go test.

	retcode = m.Run()
}
