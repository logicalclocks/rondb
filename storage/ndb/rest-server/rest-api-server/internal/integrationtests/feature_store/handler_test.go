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
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	fsmetadata "hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/handlers/feature_store"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func TestFeatureStoreMetaData(t *testing.T) {
	_, err := fsmetadata.GetFeatureViewMetadata(testdbs.FSDB002, "sample_2", 1)
	if err != nil {
		t.Fatalf("Reading FS Metadata failed %v ", err)
	}
}

func TestMetadata_FsNotExist(t *testing.T) {
	_, err := fsmetadata.GetFeatureViewMetadata("NA", "sample_2", 1)
	if err == nil {
		t.Fatalf("This should fail.")
	}
	if !strings.Contains(err.Error(), fsmetadata.FS_NOT_EXIST.GetReason()) {
		t.Fatalf("This should fail with error message: %s.", fsmetadata.FS_NOT_EXIST.GetReason())
	}
}

func TestMetadata_ReadDeletedFg(t *testing.T) {
	_, err := fsmetadata.GetFeatureViewMetadata(testdbs.FSDB001, "test_deleted_fg", 1)
	if err == nil {
		t.Fatalf("This should fail.")
	}
	if !strings.Contains(err.Error(), fsmetadata.FG_NOT_EXIST.GetReason()) {
		t.Fatalf("This should fail with error message: %s. But found: %s", fsmetadata.FG_NOT_EXIST.GetReason(), err.Error())
	}
}

func TestMetadata_ReadDeletedJointFg(t *testing.T) {
	_, err := fsmetadata.GetFeatureViewMetadata(testdbs.FSDB001, "test_deleted_joint_fg", 1)
	if err == nil {
		t.Fatalf("This should fail.")
	}
	if !strings.Contains(err.Error(), fsmetadata.FG_NOT_EXIST.GetReason()) {
		t.Fatalf("This should fail with error message: %s. But found: %s", fsmetadata.FG_NOT_EXIST.GetReason(), err.Error())
	}
}

func TestMetadata_FvNotExist(t *testing.T) {
	_, err := fsmetadata.GetFeatureViewMetadata(testdbs.FSDB002, "NA", 1)
	if err == nil {
		t.Fatalf("This should fail.")
	}
	if !strings.Contains(err.Error(), fsmetadata.FV_NOT_EXIST.GetReason()) {
		t.Fatalf("This should fail with error message: %s.", fsmetadata.FV_NOT_EXIST.GetReason())
	}
}

func Test_GetFeatureVector_Success(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
		if fsResp.DetailedStatus != nil {
			t.Fatalf("DetailedStatus should be nil as includeDetailedStatus defaults to false.")
		}
	}
}

func Test_GetFeatureVector_WithMetadata_All_Success(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_2"
	var fvVersion = 1
	rows, pks, cols, err := GetSampleData(fsName, fmt.Sprintf("%s_%d", fvName, fvVersion))
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
		ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
	}
}

func Test_GetFeatureVector_CachedFG_WithMetadata_All_Success(t *testing.T) {
	var fsName = testdbs.FSDB001
	var fvName = "sample_cache"
	var fvVersion = 1
	rows, pks, cols, err := GetSampleData(fsName, fmt.Sprintf("%s_%d", fvName, fvVersion))
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
		ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
	}
}

func Test_GetFeatureVector_WithMetadata_Name_Success(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_2"
	var fvVersion = 1
	rows, pks, cols, err := GetSampleData(fsName, fmt.Sprintf("%s_%d", fvName, fvVersion))
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: false}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
		ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
	}
}

func Test_GetFeatureVector_RequiredParametersAreNil(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.FeatureStoreName = nil
		fsReq.FeatureViewName = nil
		fsReq.FeatureViewVersion = nil
		fsReq.Entries = nil
		GetFeatureStoreResponseWithDetail(t, fsReq, "Error:Field validation", http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_OptionalParametersAreNil(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.PassedFeatures = nil
		fsReq.MetadataRequest = nil
		GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	}
}

func Test_GetFeatureVector_FsNotExist(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"NA",
			"sample_2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FS_NOT_EXIST.GetReason(), http.StatusBadRequest)

	}
}

func Test_GetFeatureVector_FvNotExist(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"NA",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FV_NOT_EXIST.GetReason(), http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_ReadDeletedFg(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"test_deleted_fg",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_ReadDeletedJointFg(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"test_deleted_joint_fg",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_CompositePrimaryKey(t *testing.T) {
	rows, pks, cols, err := GetNSampleDataColumns(testdbs.FSDB001, "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_ReturnMixedDataType(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			2,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_Join(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB002, "sample_1_1", testdbs.FSDB002, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_1n2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_Join_FeatureNameCollision(t *testing.T) {
	rows, _, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "right_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n2_no_prefix",
			1,
			[]string{"id1"},
			[]interface{}{row[0]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Change the column name because it is conflicted with left fg
		cols[4] = "right_id1"
		cols[5] = "right_ts"
		cols[6] = "right_data1"
		cols[7] = "right_data2"
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_Join_PkOnly(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "id1"
	rows, pks, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_", joinKey,
		[]string{"id1"}, []string{"id1"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n2_pkonly",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_Join_Metadata_All(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_1n2"
	var fvVersion = 1
	rows, pks, cols, err := GetSampleDataWithJoin(fsName, "sample_1_1", fsName, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
		ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
	}
}

func Test_GetFeatureVector_Join_Metadata_Name(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_1n2"
	var fvVersion = 1
	rows, pks, cols, err := GetSampleDataWithJoin(fsName, "sample_1_1", fsName, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: false}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
		ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
	}
}

func Test_GetFeatureVector_JoinItself(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_1_1", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n1_self",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_JoinSameFg(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_1_2", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n1",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_Join(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		// exclude entry 'fg2_id1' from the request
		var pkFiltered, pkValueFiltered = GetPkValuesExclude(&row, &pks, &cols, []string{"fg2_id1"})
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n2",
			1,
			*pkFiltered,
			*pkValueFiltered,
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_Join_ExtraKey(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var pkValue = *GetPkValues(&row, &pks, &cols)
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n2",
			1,
			[]string{"id1", "fg2_id1"},
			// `fg2_id1` should be ignored
			[]interface{}{pkValue[0], []byte(fmt.Sprintf("%d", 999999))},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_SelfJoin(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_1_1", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		// exclude entry 'fg1_id1' from the request
		var pkFiltered, pkValueFiltered = GetPkValuesExclude(&row, &pks, &cols, []string{"fg1_id1"})
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n1_self",
			1,
			*pkFiltered,
			*pkValueFiltered,
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_Join_LeftColOnRightId(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["data1"] = "id1"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Exclude the primary key because they are not selected as features
	var exCols = make(map[string]bool)
	exCols["righId1"] = true
	exCols["id2"] = true
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3_joinoncol",
			1,
			// All required primary key
			[]string{"id1", "0_id1", "id2"},
			[]interface{}{row[0], row[4], row[5]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Change the column name because it is conflicted with col[0]
		cols[4] = "righId1"
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_Join_PkFallbackToRawFeatureName(t *testing.T) {
	var joinKey = make(map[string]string)
	// correct join key should be joinKey["data1"] = "id1", but since "0_id1" will not be provided and "id1" will be used.
	// That is essentially the result of joining of "id1"
	joinKey["id1"] = "id1"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Exclude the primary key because they are not selected as features
	var exCols = make(map[string]bool)
	exCols["righId1"] = true
	exCols["id2"] = true
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3_joinoncol",
			1,
			// "0_id1" is not provided, should fallback and use "id1" in right fg lookup
			[]string{"id1", "id2"},
			[]interface{}{row[0], row[5]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Change the column name because it is conflicted with col[0]
		cols[4] = "righId1"
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_Join_ColOnCol(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["data1"] = "bigint"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Exclude the primary key because they are not selected as features
	var exCols = make(map[string]bool)
	exCols["righId1"] = true
	exCols["id2"] = true
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3_joincoloncol",
			1,
			// All required primary key
			[]string{"id1", "0_id1", "id2"},
			[]interface{}{row[0], row[4], row[5]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Change the column name because it is conflicted with col[0]
		cols[4] = "righId1"
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_PrefixCollision(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "bigint"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Exclude the primary key because they are not selected as features
	var exCols = make(map[string]bool)
	exCols["righId1"] = true
	exCols["id2"] = true
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3",
			1,
			// All required primary key
			[]string{"id1", "0_id1", "id2"},
			[]interface{}{row[0], row[4], row[5]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Change the column name because it is conflicted with col[0]
		cols[4] = "righId1"
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_JoinOnCol_WithPrefix_PkFallbackToRawFeatureName_IncludePk(t *testing.T) {
	var joinKey = make(map[string]string)
	// correct join key should be joinKey["data1"] = "id1", but since "0_id1" will not be provided and "id1" will be used.
	// That is essentially the result of joining of "id1"
	joinKey["id1"] = "id1"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "right_", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3_pk",
			1,
			// "0_id1" is not provided, should fallback and use "id1" in right fg lookup
			[]string{"id1", "id2"},
			[]interface{}{row[0], row[5]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_TestServingKey_Join_WithoutPrefix_PkFallbackToRawFeatureName_IncludePk(t *testing.T) {
	var joinKey = make(map[string]string)
	// correct join key should be joinKey["data1"] = "id1", but since "0_id1" will not be provided and "id1" will be used.
	// That is essentially the result of joining of "id1"
	joinKey["id1"] = "id1"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3_no_prefix_pk",
			1,
			// "0_id1" is not provided, should fallback and use "id1" in right fg lookup
			[]string{"id1", "id2"},
			[]interface{}{row[0], row[5]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Change the column name because it is conflicted with col[0]
		cols[4] = "righId1"
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_TestCorrectPkValue_WithPrefix_RequiredValueProvided(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "id2"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_4_1", testdbs.FSDB001, "sample_3_1", "right_", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_4n3_on_id",
			1,
			// "right_id2", "id2" are not considered because required entry "id1" is provided.
			[]string{"id1", "right_id1", "right_id2", "id2"},
			[]interface{}{row[0], row[4], row[5], []byte(`"notvalid"`)},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_TestCorrectPkValue_WithPrefix_RequiredValueNotProvided(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "id2"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_4_1", testdbs.FSDB001, "sample_3_1", "right_", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_4n3_on_id",
			1,
			// "id2" are not considered because required entry "id1" is provided.
			[]string{"right_id1", "right_id2", "id2"},
			[]interface{}{row[4], row[5], []byte(`"notvalid"`)},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Only features from right fg are not null because primary key of the right fg is provided
		for j := range row {
			if j < 4 {
				row[j] = nil
			}
		}
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_TestCorrectPkValue_WithPrefix_PrefixNotProvided(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "id2"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_4_1", testdbs.FSDB001, "sample_3_1", "right_", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_4n3_on_id",
			1,
			[]string{"right_id1", "id2"},
			[]interface{}{row[4], row[5]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Only features from right fg are not null because primary key of the right fg is provided
		for j := range row {
			if j < 4 {
				row[j] = nil
			}
		}
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_ExcludeLabelColumn(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var exCols = make(map[string]bool)
	exCols["data1"] = true
	var fvName = "sample_1n2_label"
	var fvVersion = 1
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)
		ValidateResponseMetadataExCol(t, &fsResp.Metadata, fsReq.MetadataRequest, &exCols, testdbs.FSDB001, fvName, fvVersion)
	}
}

func Test_GetFeatureVector_ExcludeLabelFg(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var exCols = make(map[string]bool)
	exCols["data1"] = true
	exCols["id1"] = true
	exCols["data2"] = true
	exCols["ts"] = true
	var fvName = "sample_1n2_labelonly"
	var fvVersion = 1
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: false}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)
		ValidateResponseMetadataExCol(t, &fsResp.Metadata, fsReq.MetadataRequest, &exCols, testdbs.FSDB001, fvName, fvVersion)
	}
}

func Test_GetFeatureVector_Shared(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB002, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_share_1n2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_NotShared(t *testing.T) {
	rows, pks, cols, err := GetNSampleDataColumns(testdbs.FSDB001, "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb_isolate",
			"sample_4",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FEATURE_STORE_NOT_SHARED.GetMessage(), http.StatusUnauthorized)
	}
}

func Test_GetFeatureVector_WrongPrimaryKey_NotExist(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Make wrong primary key
	var wrongPks = make([]string, len(pks))
	for i, pk := range pks {
		wrongPks[i] = pk + "abcd"
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			wrongPks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.INCORRECT_PRIMARY_KEY.GetReason(), http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_PrimaryKeyNoMatch(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	for _, row := range rows {
		var pkValues = *GetPkValues(&row, &pks, &cols)
		for i := range pkValues {
			pkv := []byte(strconv.Itoa(9876543 + i))
			pkValues[i] = pkv
			for j := range row {
				row[j] = nil
			}
			row[0] = pkv
		}
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			pkValues,
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_NoPrimaryKey(t *testing.T) {
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		nil,
		nil,
		nil,
		nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.INCORRECT_PRIMARY_KEY.GetReason(), http.StatusBadRequest)
}

func Test_GetFeatureVector_IncompletePrimaryKey(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	// Exclude not selected features
	var exCols = make(map[string]bool)
	exCols["string"] = true
	exCols["date"] = true
	exCols["bool"] = true
	exCols["float"] = true
	exCols["double"] = true
	exCols["binary"] = true

	for _, row := range rows {
		var pkValues = *GetPkValues(&row, &pks, &cols)
		// set all feature to be none except pk
		for j := range row {
			if j != 0 {
				row[j] = nil
			}
		}
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			1,
			// Remove one pk
			[]string{pks[0]},
			[]interface{}{pkValues[0]},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)

	}
}

func Test_GetFeatureVector_IncompletePrimaryKey_Join(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "bigint"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	// Exclude the primary key because they are not selected as features
	var exCols = make(map[string]bool)
	exCols["righId1"] = true
	exCols["id2"] = true

	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3",
			1,
			// Do not include primary key from left fg
			[]string{"0_id1", "id2"},
			[]interface{}{row[4], row[5]},
			nil,
			nil,
		)
		// Only last feature is not null because primary key of the right fg is provided
		for j := range row {
			if j != 6 {
				row[j] = nil
			}
		}
		// Change the column name because it is conflicted with col[0]
		cols[4] = "righId1"
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)
	}
}

func Test_GetFeatureVector_IncompletePrimaryKey_JoinOnCol_PkWithPrefix_IncludePk(t *testing.T) {
	var joinKey = make(map[string]string)
	// correct join key should be joinKey["data1"] = "id1", but since "0_id1" will not be provided and "id1" will be used.
	// That is essentially the result of joining of "id1"
	joinKey["id1"] = "id1"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "right_", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3_pk",
			1,
			[]string{"right_id1", "right_id2", "id2"},
			[]interface{}{row[4], row[5], []byte(`"invalid"`)},
			nil,
			nil,
		)
		// Only features from right fg are not null because primary key of the right fg is provided
		for j := range row {
			if j < 4 {
				row[j] = nil
			}
		}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_IncompletePrimaryKey_JoinOnCol_PkWithoutPrefix_IncludePk(t *testing.T) {
	var joinKey = make(map[string]string)
	// correct join key should be joinKey["data1"] = "id1", but since "0_id1" will not be provided and "id1" will be used.
	// That is essentially the result of joining of "id1"
	joinKey["id1"] = "id1"
	rows, _, cols, err := GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "right_", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n3_pk",
			1,
			[]string{"right_id1", "id2"},
			[]interface{}{row[4], row[5]},
			nil,
			nil,
		)
		// Only features from right fg are not null because primary key of the right fg is provided
		for j := range row {
			if j < 4 {
				row[j] = nil
			}
		}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_IncompletePrimaryKey_Join_IncludePk(t *testing.T) {
	rows, _, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_1_1", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n1_self",
			1,
			[]string{"fg1_id1"},
			[]interface{}{row[4]},
			nil,
			nil,
		)
		// Only features from right fg are not null because primary key of the right fg is provided
		for j := range row {
			if j < 4 {
				row[j] = nil
			}
		}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_GetFeatureVector_WrongPrimaryKey_FeatureNotPk(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	for _, row := range rows {
		var pkValues = *GetPkValues(&row, &pks, &cols)
		pks[0] = "ts"
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			1,
			pks,
			pkValues,
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.INCORRECT_PRIMARY_KEY.GetReason(), http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_WrongPrimaryKey_TooManyPk(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	for _, row := range rows {
		var pkValues = *GetPkValues(&row, &pks, &cols)
		pks = append(pks, "ts")
		pkValues = append(pkValues, []byte(`"2022-01-01"`))
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			1,
			pks,
			pkValues,
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.INCORRECT_PRIMARY_KEY.GetReason(), http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_WrongPkType_Int(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	for _, row := range rows {
		// Make wrong primary key value
		var pkValues = *GetPkValues(&row, &pks, &cols)
		for i, pkv := range pkValues {
			// rondb can convert int in string quote to int
			pkValues[i] = []byte("\"" + string(pkv.([]byte)) + "\"")
		}
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			pkValues,
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	}
}

func Test_GetFeatureVector_WrongPkType_Str(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	for _, row := range rows {
		// Make wrong primary key value
		var pkValues = *GetPkValues(&row, &pks, &cols)
		for i := range pkValues {
			// rondb can convert int to string
			pkValues[i] = []byte(fmt.Sprintf("%d", i))
		}
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			1,
			pks,
			pkValues,
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	}
}

func Test_GetFeatureVector_WrongPkValue(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	for _, row := range rows {
		// Make wrong primary key value
		var pkValues = *GetPkValues(&row, &pks, &cols)
		for i := range pkValues {
			// rondb can convert int to string
			pkValues[i] = []byte(fmt.Sprintf("\"abc%d\"", i))
		}
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			1,
			pks,
			pkValues,
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.WRONG_DATA_TYPE.GetReason(), http.StatusUnsupportedMediaType)
	}
}

func Test_GetFeatureVector_Success_ComplexType(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_complex_type"
	var fvVersion = 1
	rows, pks, cols, err := GetSampleData(fsName, "sample_complex_type_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	mapCodec, err := goavro.NewCodec(`["null",{"type":"record","name":"r854762204","namespace":"struct","fields":[{"name":"int1","type":["null","long"]},{"name":"int2","type":["null","long"]}]}]`)
	if err != nil {
		t.Fatal(err.Error())
	}
	arrayCodec, err := goavro.NewCodec(`["null",{"type":"array","items":["null","long"]}]`)
	if err != nil {
		t.Fatal(err.Error())
	}
	mapDecoder := fsmetadata.AvroDecoder{Codec: mapCodec}
	arrayDecoder := fsmetadata.AvroDecoder{Codec: arrayCodec}

	if err != nil {
		t.Fatal(err.Error())
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			fsName,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
		fsResp := GetFeatureStoreResponse(t, fsReq)

		// convert data to object in json format
		arrayJson, err := ConvertBinaryToJsonMessage(row[2])
		if err != nil {
			t.Fatalf("Cannot convert to json with error %s ", err)
		}
		arrayPt, err := feature_store.DeserialiseComplexFeature(arrayJson, &arrayDecoder) // array
		row[2] = *arrayPt
		if err != nil {
			t.Fatalf("Cannot deserailize feature with error %s ", err)
		}
		// convert data to object in json format
		mapJson, err := ConvertBinaryToJsonMessage(row[3])
		if err != nil {
			t.Fatalf("Cannot convert to json with error %s ", err)
		}
		mapPt, err := feature_store.DeserialiseComplexFeature(mapJson, &mapDecoder) // map
		row[3] = *mapPt
		if err != nil {
			t.Fatalf("Cannot deserailize feature with error %s ", err)
		}
		// validate
		ValidateResponseWithData(t, &row, &cols, fsResp)
		ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
	}
}

func Test_PassedFeatures_Success(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		row[2] = []byte(`"999"`)
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"data1"},
			[]interface{}{[]byte(`"999"`)},
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_PassedFeatures_Join_FeatureNameCollision(t *testing.T) {
	rows, _, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "right_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n2_no_prefix",
			1,
			[]string{"id1"},
			[]interface{}{row[0]},
			[]string{"id1", "ts", "data1", "data2"},
			[]interface{}{[]byte(`1`), []byte(`123`), []byte(`999`), []byte(`245`)},
		)
		// Change the column name because it is conflicted with left fg
		cols[4] = "right_id1"
		cols[5] = "right_ts"
		cols[6] = "right_data1"
		cols[7] = "right_data2"
		// Should fail because "sample_2_1.data1" is string type and the provided type is number.
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.WRONG_DATA_TYPE.GetReason(), http.StatusBadRequest)
	}
}

func Test_PassedFeatures_Join_FeatureNameCollision_NoValidation(t *testing.T) {
	rows, _, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "right_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n2_no_prefix",
			1,
			[]string{"id1"},
			[]interface{}{row[0]},
			[]string{"id1", "ts", "data1", "data2"},
			[]interface{}{[]byte(`1`), []byte(`123`), []byte(`"999"`), []byte(`"000"`)},
		)
		var validatePassedFeatures = false
		fsReq.OptionsRequest = &api.OptionsRequest{ValidatePassedFeatures: &validatePassedFeatures}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Set the row to value of passed features
		row[0] = []byte(`1`)
		row[1] = []byte(`123`)
		row[2] = []byte(`"999"`)
		row[3] = []byte(`"000"`)
		row[4] = []byte(`1`)
		row[5] = []byte(`123`)
		row[6] = []byte(`"999"`)
		row[7] = []byte(`"000"`)
		// Change the column name because it is conflicted with left fg
		cols[4] = "right_id1"
		cols[5] = "right_ts"
		cols[6] = "right_data1"
		cols[7] = "right_data2"
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_PassedFeatures_Success_NoValidation(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for ind := range rows {
		var row = rows[ind]
		row[2] = []byte(`"999"`)
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"data1"},
			[]interface{}{[]byte(`"999"`)},
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		var validatePassedFeatures = false
		fsReq.OptionsRequest = &api.OptionsRequest{ValidatePassedFeatures: &validatePassedFeatures}
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_PassedFeatures_Success_AllTypes(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var passedFeatures = []interface{}{
			row[0],                 // id1
			row[1],                 // id2
			[]byte(`"992"`),        // ts
			[]byte(`993`),          // bigint
			[]byte(`"994"`),        // string
			[]byte(`"2022-01-01"`), // date
			[]byte(`true`),         // bool
			[]byte(`1.5`),          // float
			[]byte(`2.5`),          // double
			[]byte(fmt.Sprintf(`"%s"`, base64.StdEncoding.EncodeToString([]byte("EEFF")))), // binary
		}
		copy(row, passedFeatures)
		row[len(passedFeatures)-1] = []byte(`"EEFF"`)
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			2,
			pks,
			*GetPkValues(&row, &pks, &cols),
			cols,
			passedFeatures,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_PassedFeatures_WrongKey_FeatureNotExist(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		row[2] = []byte("999")
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"invalide_key"},
			[]interface{}{[]byte("999")},
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FEATURE_NOT_EXIST.GetReason(), http.StatusBadRequest)
	}
}

func Test_PassedFeatures_WrongKey_FeatureNotExist_NoValidation(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var originalRows = CopyRows(rows)
	for ind := range rows {
		var row = rows[ind]
		var originalRow = originalRows[ind]
		row[2] = []byte("999")
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"invalide_key"},
			[]interface{}{[]byte("999")},
		)
		var validatePassedFeatures = false
		fsReq.OptionsRequest = &api.OptionsRequest{ValidatePassedFeatures: &validatePassedFeatures}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &originalRow, &cols, fsResp)
	}
}

func Test_PassedFeatures_WrongType_NotString(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			2,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"string"},
			[]interface{}{[]byte(`999`)},
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.WRONG_DATA_TYPE.GetReason(), http.StatusBadRequest)
	}
}

func Test_PassedFeatures_WrongType_NotString_NoValidation(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			2,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"string"},
			[]interface{}{[]byte(`999`)},
		)
		var validatePassedFeatures = false
		fsReq.OptionsRequest = &api.OptionsRequest{ValidatePassedFeatures: &validatePassedFeatures}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		// Replace row with value of passed features
		row[4] = []byte("999")
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_PassedFeatures_WrongType_NotNumber(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			2,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"bigint"},
			[]interface{}{[]byte(`"int"`)},
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.WRONG_DATA_TYPE.GetReason(), http.StatusBadRequest)
	}
}

func Test_PassedFeatures_WrongType_NotNumber_NoValidation(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			2,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"bigint"},
			[]interface{}{[]byte(`"int"`)},
		)
		var validatePassedFeatures = false
		fsReq.OptionsRequest = &api.OptionsRequest{ValidatePassedFeatures: &validatePassedFeatures}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		row[3] = []byte(`"int"`)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_PassedFeatures_WrongType_NotBoolean(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			2,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"bool"},
			[]interface{}{[]byte(`"int"`)},
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.WRONG_DATA_TYPE.GetReason(), http.StatusBadRequest)
	}
}

func Test_PassedFeatures_WrongType_NotBoolean_NoValidation(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_3",
			2,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"bool"},
			[]interface{}{[]byte(`"int"`)},
		)
		var validatePassedFeatures = false
		fsReq.OptionsRequest = &api.OptionsRequest{ValidatePassedFeatures: &validatePassedFeatures}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		row[6] = []byte(`"int"`)
		ValidateResponseWithData(t, &row, &cols, fsResp)
	}
}

func Test_PassedFeatures_LabelShouldFail(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var exCols = make(map[string]bool)
	exCols["data1"] = true
	var fvName = "sample_1n2_label"
	var fvVersion = 1
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"data1"},
			[]interface{}{[]byte(`300`)},
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FEATURE_NOT_EXIST.GetReason(), http.StatusBadRequest)
	}
}

func Test_PassedFeatures_LabelShouldFail_NoValidation(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var exCols = make(map[string]bool)
	exCols["data1"] = true
	var fvName = "sample_1n2_label"
	var fvVersion = 1
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			fvName,
			fvVersion,
			pks,
			*GetPkValues(&row, &pks, &cols),
			[]string{"data1"},
			[]interface{}{[]byte(`300`)},
		)
		fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
		var validatePassedFeatures = false
		fsReq.OptionsRequest = &api.OptionsRequest{ValidatePassedFeatures: &validatePassedFeatures}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithDataExcludeCols(t, &row, &cols, &exCols, fsResp)
	}
}

func Test_IncludeDetailedStatus_SingleTable(t *testing.T) {
	rows, pks, cols, err := GetSampleData(testdbs.FSDB001, "sample_1_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		includeDetailedStatus := true
		fsReq.OptionsRequest = &api.OptionsRequest{IncludeDetailedStatus: &includeDetailedStatus}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
		if fsResp.DetailedStatus == nil {
			t.Fatalf("DetailedStatus should be set")
		}
		if len(fsResp.DetailedStatus) != 1 {
			t.Fatalf("DetailedStatus should have a single element")
		}
		if fsResp.DetailedStatus[0].HttpStatus != http.StatusOK {
			t.Fatalf("HttpStatus should be 200")
		}
		if fsResp.DetailedStatus[0].FeatureGroupId == -1 {
			t.Fatalf("FeatureGroupId should have been parsed correctly from OperationId")
		}
	}
}

func Test_IncludeDetailedStatus_JoinedTable(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n2",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		includeDetailedStatus := true
		fsReq.OptionsRequest = &api.OptionsRequest{IncludeDetailedStatus: &includeDetailedStatus}
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &row, &cols, fsResp)
		if fsResp.DetailedStatus == nil {
			t.Fatalf("DetailedStatus should be set")
		}
		if len(fsResp.DetailedStatus) != 2 {
			t.Fatalf("DetailedStatus should have two elements")
		}
		for _, ds := range fsResp.DetailedStatus {
			if ds.HttpStatus != http.StatusOK {
				t.Fatalf("HttpStatus should be 200")
			}
			if ds.FeatureGroupId == -1 {
				t.Fatalf("FeatureGroupId should have been parsed correctly from OperationId")
			}
		}
	}
}

func Test_IncludeDetailedStatus_JoinedTablePartialKey(t *testing.T) {
	rows, _, cols, err := GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_1_1", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB001,
			"sample_1n1_self",
			1,
			[]string{"fg1_id1"},
			[]interface{}{row[4]},
			nil,
			nil,
		)
		// Only features from right fg are not null because primary key of the right fg is provided
		for j := range row {
			if j < 4 {
				row[j] = nil
			}
		}
		includeDetailedStatus := true
		fsReq.OptionsRequest = &api.OptionsRequest{IncludeDetailedStatus: &includeDetailedStatus}
		fsResp := GetFeatureStoreResponse(t, fsReq)

		ValidateResponseWithData(t, &row, &cols, fsResp)
		if fsResp.Status != "MISSING" {
			t.Fatalf("Status should be MISSING")
		}
		if fsResp.DetailedStatus == nil {
			t.Fatalf("DetailedStatus should be set")
		}
		if len(fsResp.DetailedStatus) != 2 {
			t.Fatalf("DetailedStatus should have two elements")
		}
		for idx, ds := range fsResp.DetailedStatus {
			if ds.FeatureGroupId == -1 {
				t.Fatalf("FeatureGroupId should have been parsed correctly from OperationId")
			}
			if (idx == 1 && ds.HttpStatus != http.StatusOK) || (idx == 0 && ds.HttpStatus != http.StatusBadRequest) {
				t.Fatalf("HttpStatus should be 200 or 400")
			}
		}
	}
}

func Test_IncludeDetailedStatus_JoinedTablePartialKeyAndMissingRow(t *testing.T) {
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n3",
		1,
		[]string{"id1"},
		[]interface{}{[]byte(`"99999"`)},
		nil,
		nil,
	)
	includeDetailedStatus := true
	fsReq.OptionsRequest = &api.OptionsRequest{IncludeDetailedStatus: &includeDetailedStatus}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	if fsResp.Status != "MISSING" {
		t.Fatalf("Status should be MISSING")
	}
	if fsResp.DetailedStatus == nil {
		t.Fatalf("DetailedStatus should be set")
	}
	if len(fsResp.DetailedStatus) != 2 {
		t.Fatalf("DetailedStatus should have two elements")
	}
	for idx, ds := range fsResp.DetailedStatus {
		if ds.FeatureGroupId == -1 {
			t.Fatalf("FeatureGroupId should have been parsed correctly from OperationId")
		}
		if (idx == 0 && ds.HttpStatus != http.StatusNotFound) || (idx == 1 && ds.HttpStatus != http.StatusBadRequest) {
			t.Fatalf("HttpStatus should be 404 or 400")
		}
	}
}

func Test_GetFeatureVector_Success_ComplexType_With_Schema_Change(t *testing.T) {

	fsmetadata.DefaultExpiration = 1 * time.Second
	fsmetadata.CleanupInterval = 1 * time.Second

	doneCh := make(chan int)
	stop := false
	go work(t, &stop, doneCh)

	time.Sleep(2 * time.Second)

	log.Debug("Changing the schema for the test")
	err := testutils.RunQueriesOnDataCluster(testdbs.HopsworksUpdateScheme)
	if err != nil {
		t.Fatalf("failed to change schema. Error: %v", err)
	}
	log.Debug("Changed the schema for the test")

	time.Sleep(2 * time.Second)
	stop = true

	<-doneCh
}

func work(t *testing.T, stop *bool, done chan int) {
	defer func() { done <- 1 }()
	for !*stop {
		Test_GetFeatureVector_Success_ComplexType(t)
	}
}