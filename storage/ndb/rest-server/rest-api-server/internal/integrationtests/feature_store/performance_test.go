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

package feature_store

import (
	"encoding/json"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/resources/testdbs"
)

type RandomFSRequester struct {
	fsName    string
	fvName    string
	tableName string
	fvVersion int
}

func (req RandomFSRequester) GetRandomRequest(b *testing.B) (verb, url, body string) {
	var fsName = req.fsName
	var fvName = req.fvName
	var fvVersion = req.fvVersion
	var tableName = req.tableName

	verb = config.FEATURE_STORE_HTTP_VERB
	url = testutils.NewFeatureStoreURL()
	rows, pks, cols, err := GetSampleDataN(fsName, tableName, 1)

	if err != nil {
		b.Fatalf("Cannot get sample data with error %s ", err)
		return
	}

	var fsReq = CreateFeatureStoreRequest(
		fsName,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows[0], &pks, &cols),
		nil,
		nil)

	strBytes, err := json.MarshalIndent(fsReq, "", "")
	if err != nil {
		b.Fatalf("Failed to marshal FeatureStoreRequest. Error: %v", err)
		return
	}
	body = string(strBytes)

	return
}

func (req RandomFSRequester) GetBatchSize() int {
	return 1
}

func BenchmarkSimple(b *testing.B) {
	req := RandomFSRequester{
		fsName:    testdbs.FSDB001,
		fvName:    "sample_1",
		tableName: "sample_1_1",
		fvVersion: 1,
	}

	integrationtests.WrapperBenchmark(b, req)
}

func BenchmarkBatchJoin(b *testing.B) {
	req := RandomFSRequester{
		fsName:    testdbs.FSDB001,
		fvName:    "sample_1n2",
		tableName: "sample_1_1",
		fvVersion: 1,
	}

	integrationtests.WrapperBenchmark(b, req)
}

func BenchmarkBatchComplex512(b *testing.B) {

	// Uncomment this if you want to generate more random data in the test table
	// generateRandomComplexData(10/*number of rows*/, 512/*number of cols*/)

	req := RandomFSRequester{
		fsName:    testdbs.FSDB002,
		fvName:    "sample_complex_type_512",
		tableName: "sample_complex_type_512_1",
		fvVersion: 1,
	}

	integrationtests.WrapperBenchmark(b, req)
}
