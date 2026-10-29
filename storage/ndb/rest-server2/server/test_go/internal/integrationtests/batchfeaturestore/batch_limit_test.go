/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
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
	"net/http"
	"testing"

	fshelper "hopsworks.ai/rdrs2/internal/integrationtests/feature_store"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1144: the batch feature store endpoint enforces the same
// Internal.MaxOperationsPerBatchRequest as batch pk-read, counted over the entries
// while parsing and answered with 413 before any lookup starts.

// sampleEntries returns n batch entries that all read the same existing row
// of fsdb002.sample_2_1, plus the pk column names.
func sampleEntries(t *testing.T, n uint32) ([]string, [][]interface{}) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 1)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one sample row, got %d", len(rows))
	}
	one := (*GetPkValues(&rows, &pks, &cols))[0]
	entries := make([][]interface{}, n)
	for i := range entries {
		entries[i] = one
	}
	return pks, entries
}

func TestBatchFeatureStoreAtOperationLimit(t *testing.T) {
	limit := uint32(testutils.GetLimits(t).MaxOperationsPerBatchRequest)
	pks, entries := sampleEntries(t, limit)
	fsReq := CreateBatchFeatureStoreRequest(testdbs.FSDB002, "sample_2", 1, pks, entries, nil, nil)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	if uint32(len(fsResp.Features)) != limit {
		t.Fatalf("expected %d feature vectors, got %d", limit, len(fsResp.Features))
	}
}

func TestBatchFeatureStoreOverOperationLimit(t *testing.T) {
	limit := uint32(testutils.GetLimits(t).MaxOperationsPerBatchRequest)
	pks, entries := sampleEntries(t, limit+1)
	fsReq := CreateBatchFeatureStoreRequest(testdbs.FSDB002, "sample_2", 1, pks, entries, nil, nil)
	resp := GetFeatureStoreResponseWithDetail(t, fsReq, "exceeds MaxOperationsPerBatchRequest",
		http.StatusRequestEntityTooLarge)
	if resp != nil {
		t.Fatalf("expected no response body on 413, got %s", resp.String())
	}
}
