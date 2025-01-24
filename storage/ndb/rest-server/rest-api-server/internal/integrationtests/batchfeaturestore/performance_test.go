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

package batchfeaturestore

import (
	"encoding/json"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/resources/testdbs"

	"hopsworks.ai/rdrs/internal/integrationtests"
	fshelper "hopsworks.ai/rdrs/internal/integrationtests/feature_store"
)

type RandomBatchFSRequester struct {
	fsName    string
	fvName    string
	tableName string
	fvVersion int
	batchSize int
}

func (req RandomBatchFSRequester) GetRandomRequest(b *testing.B) (verb, url, body string) {
	verb = config.FEATURE_STORE_HTTP_VERB
	url = testutils.NewBatchFeatureStoreURL()
	rows, pks, cols, err := fshelper.GetSampleDataN(req.fsName, req.tableName, req.batchSize)
	if err != nil {
		b.Fatalf("Cannot get sample data with error %s ", err)
		return
	}

	var fsReq = CreateFeatureStoreRequest(
		req.fsName,
		req.fvName,
		req.fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
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

func (req RandomBatchFSRequester) GetBatchSize() int {
	return req.batchSize
}

func BenchmarkBatchSimple(b *testing.B) {
	req := RandomBatchFSRequester{
		fsName:    testdbs.FSDB001,
		fvName:    "sample_1",
		tableName: "sample_1_1",
		fvVersion: 1,
		batchSize: 10,
	}

	integrationtests.WrapperBenchmark(b, req)
}

func BenchmarkBatchJoin(b *testing.B) {
	req := RandomBatchFSRequester{
		fsName:    testdbs.FSDB001,
		fvName:    "sample_1n2",
		tableName: "sample_1_1",
		fvVersion: 1,
		batchSize: 10,
	}

	integrationtests.WrapperBenchmark(b, req)
}

func BenchmarkBatchComplex512(b *testing.B) {

	// Uncomment this if you want to generate more random data in the test table
	// generateRandomComplexData(10/*number of rows*/, 512/*number of cols*/)

	req := RandomBatchFSRequester{
		fsName:    testdbs.FSDB002,
		fvName:    "sample_complex_type_512",
		tableName: "sample_complex_type_512_1",
		fvVersion: 1,
		batchSize: 10,
	}

	integrationtests.WrapperBenchmark(b, req)
}
