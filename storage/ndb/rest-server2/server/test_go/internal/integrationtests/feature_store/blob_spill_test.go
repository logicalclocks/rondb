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
package feature_store

import (
	"testing"

	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1144: a blob feature larger than the room left in a response buffer
// continues into the next buffers of the request's chain, on the
// feature-store path too. Feature view sample_blob v1 serves
// fsdb002.sample_blob_1 (id bigint, payload longblob) whose single row is
// seeded with payload NULL; `payload` is a plain binary feature, so rdrs2
// returns it base64-encoded with no Avro schema involved. Every size is
// computed from the response buffer size rdrs2 reports, and after each read
// the pool must hold no buffer of ours.

const (
	blobFeatureView = "sample_blob"
	blobTable       = "sample_blob_1"
	blobColumn      = "payload"
	blobRowID       = 1
)

// setBlobFeature writes data into the payload of row 1 and resets it to
// NULL when the test ends, so the fixture row is the same for every test.
func setBlobFeature(t *testing.T, data []byte) {
	t.Helper()
	testutils.SetBlob(t, testdbs.FSDB002, blobTable, blobColumn, blobRowID, data)
	t.Cleanup(func() {
		testutils.SetBlobNull(t, testdbs.FSDB002, blobTable, blobColumn, blobRowID)
	})
}

// expectBlobFeature reads the feature vector of row 1 and checks that the
// payload feature decodes to want: same sha256 locally and in MySQL.
func expectBlobFeature(t *testing.T, want []byte) {
	t.Helper()
	fsReq := CreateFeatureStoreRequest(
		testdbs.FSDB002,
		blobFeatureView,
		1,
		[]string{"id"},
		[]interface{}{[]byte("1")},
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	if fsResp.Status != api.FEATURE_STATUS_COMPLETE {
		t.Fatalf("status: got %s, want %s", fsResp.Status, api.FEATURE_STATUS_COMPLETE)
	}
	// Features follow training_dataset_feature.idx: id, then payload.
	if len(fsResp.Features) != 2 {
		t.Fatalf("got %d features, want 2", len(fsResp.Features))
	}
	if id, _ := fsResp.Features[0].(float64); id != blobRowID {
		t.Fatalf("id: got %v", fsResp.Features[0])
	}
	s, ok := fsResp.Features[1].(string)
	if !ok {
		t.Fatalf("payload is not a JSON string: %v", fsResp.Features[1])
	}
	got := testutils.DecodeBase64Value(t, s)
	if len(got) != len(want) {
		t.Fatalf("payload: got %d bytes, want %d", len(got), len(want))
	}
	if testutils.SHA256Hex(got) != testutils.SHA256Hex(want) {
		t.Fatalf("payload: content differs")
	}
	if db := testutils.DBSHA256(t, testdbs.FSDB002, blobTable, blobColumn, blobRowID); db != testutils.SHA256Hex(want) {
		t.Fatalf("payload: MySQL holds a different value (sha %s)", db)
	}
}

func TestFeatureStoreBlobSpillOneBufferPlusOne(t *testing.T) {
	size := int(testutils.GetLimits(t).RespBufferBytes)
	before := testutils.TakePoolSnapshot(t)
	want := testutils.DeterministicBytes(size+1, 1)
	setBlobFeature(t, want)
	expectBlobFeature(t, want)
	testutils.AssertPoolBalanced(t, before)
}

func TestFeatureStoreBlobSpillTwoBuffersPlusOne(t *testing.T) {
	size := int(testutils.GetLimits(t).RespBufferBytes)
	before := testutils.TakePoolSnapshot(t)
	want := testutils.DeterministicBytes(2*size+1, 2)
	setBlobFeature(t, want)
	expectBlobFeature(t, want)
	testutils.AssertPoolBalanced(t, before)
}
