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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/linkedin/goavro/v2"
	fsmetadata "hopsworks.ai/rdrs2/internal/feature_store"
	"hopsworks.ai/rdrs2/internal/handlers/feature_store"
	fshelper "hopsworks.ai/rdrs2/internal/integrationtests/feature_store"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

func Test_GetFeatureVector_CacheFG_5entries_Metadata_Success(t *testing.T) {
	var fsName = testdbs.FSDB001
	var fvName = "sample_cache"
	var fvVersion = 1
	rows, pks, cols, err := fshelper.GetNSampleData(fsName, fmt.Sprintf("%s_%d", fvName, fvVersion), 5)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		fsName,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
	fshelper.ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
}

func Test_GetFeatureVector_Success_1entries(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 1)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			testdbs.FSDB002,
			"sample_2",
			1,
			pks,
			[][]interface{}{*fshelper.GetPkValues(&row, &pks, &cols)},
			nil,
			nil,
		)
		fsResp := GetFeatureStoreResponse(t, fsReq)
		ValidateResponseWithData(t, &rows, &cols, fsResp)
	}
}

func Test_GetFeatureVector_Success_5entries(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 5)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_5entries_Metadata_Success(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_2"
	var fvVersion = 1
	rows, pks, cols, err := fshelper.GetNSampleData(fsName, fmt.Sprintf("%s_%d", fvName, fvVersion), 5)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		fsName,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
	fshelper.ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
}

func Test_GetFeatureVector_5entries_Metadata_Name_Success(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_2"
	var fvVersion = 1
	rows, pks, cols, err := fshelper.GetNSampleData(fsName, fmt.Sprintf("%s_%d", fvName, fvVersion), 5)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		fsName,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: false}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
	fshelper.ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
}

func Test_GetFeatureVector_Success_10entries(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 10)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_RequiredParametersAreNil(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.FeatureStoreName = nil
	fsReq.FeatureViewName = nil
	fsReq.FeatureViewVersion = nil
	fsReq.Entries = nil
	GetFeatureStoreResponseWithDetail(t, fsReq, "Error:Field validation", http.StatusBadRequest)

}

func Test_GetFeatureVector_OptionalParametersAreNil(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.PassedFeatures = nil
	fsReq.MetadataRequest = nil
	GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
}

func Test_GetFeatureVector_FsNotExist(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"NA",
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FS_NOT_EXIST.GetReason(), http.StatusBadRequest)
}

func Test_GetFeatureVector_FvNotExist(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"NA",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FV_NOT_EXIST.GetReason(), http.StatusBadRequest)

}

func Test_GetFeatureVector_ReadDeletedFg(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"test_deleted_fg",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)
}

func Test_GetFeatureVector_ReadDeletedJointFg(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData(testdbs.FSDB002, "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"test_deleted_joint_fg",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FG_NOT_EXIST.GetReason(), http.StatusBadRequest)
}

func Test_GetFeatureVector_CompositePrimaryKey(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns(testdbs.FSDB001, "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_ReturnMixedDataType(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData(testdbs.FSDB001, "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		2,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_Join(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(testdbs.FSDB002, "sample_1_1", testdbs.FSDB002, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_1n2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_Join_PkOnly(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "id1"
	rows, pks, cols, err := fshelper.GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_", joinKey,
		[]string{"id1"}, []string{"id1"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n2_pkonly",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)

}

func Test_GetFeatureVector_JoinItself(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_1_1", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n1_self",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)

}

func Test_GetFeatureVector_Join_Metadata(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_1n2"
	var fvVersion = 1
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(fsName, "sample_1_1", fsName, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		fsName,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
	fshelper.ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
}

func Test_GetFeatureVector_Join_Metadata_Name(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_1n2"
	var fvVersion = 1
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(fsName, "sample_1_1", fsName, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		fsName,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: false}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
	fshelper.ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)
}

func Test_GetFeatureVector_joinSameFg(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_1_2", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n1",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_TestServingKey_Join(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	// exclude entry 'fg2_id1' from the request
	var pkFiltered, pkValueFiltered = GetPkValuesExclude(&rows, &pks, &cols, []string{"fg2_id1"})
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
	ValidateResponseWithData(t, &rows, &cols, fsResp)

}

func Test_GetFeatureVector_TestServingKey_Join_ExtraKey(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pksExtraKey = make([][]interface{}, 0)
	for _, row := range rows {
		var pkValue = *fshelper.GetPkValues(&row, &pks, &cols)
		pksExtraKey = append(pksExtraKey, []interface{}{pkValue[0], []byte(fmt.Sprintf("%d", 999999))})
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n2",
		1,
		[]string{"id1", "fg2_id1"},
		pksExtraKey, // `fg2_id1` should be ignored
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)

}

func Test_GetFeatureVector_TestServingKey_SelfJoin(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_1_1", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	// exclude entry 'fg1_id1' from the request
	var pkFiltered, pkValueFiltered = GetPkValuesExclude(&rows, &pks, &cols, []string{"fg1_id1"})
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
	ValidateResponseWithData(t, &rows, &cols, fsResp)

}

func Test_GetFeatureVector_TestServingKey_Join_LeftColOnRightId(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["data1"] = "id1"
	rows, _, cols, err := fshelper.GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Exclude the primary key because they are not selected as features
	var exCols = make(map[string]bool)
	exCols["righId1"] = true
	exCols["id2"] = true

	var pkValues = make([][]interface{}, 0)
	for _, row := range rows {
		// All required primary key
		pkValues = append(pkValues, []interface{}{row[0], row[4], row[5]})
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n3_joinoncol",
		1,
		[]string{"id1", "0_id1", "id2"},
		pkValues,
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	// Change the column name because it is conflicted with col[0]
	cols[4] = "righId1"
	ValidateResponseWithDataExcludeCols(t, &rows, &cols, &exCols, fsResp)
}

func Test_GetFeatureVector_TestServingKey_Join_ColOnCol(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["data1"] = "bigint"
	rows, _, cols, err := fshelper.GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Exclude the primary key because they are not selected as features
	var exCols = make(map[string]bool)
	exCols["righId1"] = true
	exCols["id2"] = true

	var pkValues = make([][]interface{}, 0)
	for _, row := range rows {
		// All required primary key
		pkValues = append(pkValues, []interface{}{row[0], row[4], row[5]})
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n3_joincoloncol",
		1,
		[]string{"id1", "0_id1", "id2"},
		pkValues,
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	// Change the column name because it is conflicted with col[0]
	cols[4] = "righId1"
	ValidateResponseWithDataExcludeCols(t, &rows, &cols, &exCols, fsResp)

}

func Test_GetFeatureVector_TestServingKey_PrefixCollision(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "bigint"
	rows, _, cols, err := fshelper.GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Exclude the primary key because they are not selected as features
	var exCols = make(map[string]bool)
	exCols["righId1"] = true
	exCols["id2"] = true

	var pkValues = make([][]interface{}, 0)
	for _, row := range rows {
		// All required primary key
		pkValues = append(pkValues, []interface{}{row[0], row[4], row[5]})
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n3",
		1,
		[]string{"id1", "0_id1", "id2"},
		pkValues,
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	// Change the column name because it is conflicted with col[0]
	cols[4] = "righId1"
	ValidateResponseWithDataExcludeCols(t, &rows, &cols, &exCols, fsResp)
}

func Test_GetFeatureVector_excludeLabelColumn(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin("fsdb001", "sample_1_1", "fsdb001", "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var exCols = make(map[string]bool)
	exCols["data1"] = true
	var fvName = "sample_1n2_label"
	var fvVersion = 1
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: false, FeatureType: true}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithDataExcludeCols(t, &rows, &cols, &exCols, fsResp)
	fshelper.ValidateResponseMetadataExCol(t, &fsResp.Metadata, fsReq.MetadataRequest, &exCols, testdbs.FSDB001, fvName, fvVersion)
}

func Test_GetFeatureVector_excludeLabelFg(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin("fsdb001", "sample_1_1", "fsdb001", "sample_2_1", "fg2_")
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
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithDataExcludeCols(t, &rows, &cols, &exCols, fsResp)
	fshelper.ValidateResponseMetadataExCol(t, &fsResp.Metadata, fsReq.MetadataRequest, &exCols, testdbs.FSDB001, fvName, fvVersion)

}

func Test_GetFeatureVector_Shared(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB002, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_share_1n2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_NotShared(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns(testdbs.FSDB001, "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb_isolate",
		"sample_4",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FEATURE_STORE_NOT_SHARED.GetMessage(), http.StatusUnauthorized)
}

func Test_GetFeatureVector_WrongPrimaryKey_NotExist(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Make wrong primary key
	var wrongPks = make([]string, len(pks))
	for i, pk := range pks {
		wrongPks[i] = pk + "abcd"
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		wrongPks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	for i := range rows {
		rows[i] = nil
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)

}

func Test_GetFeatureVector_PrimaryKeyNoMatch(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = [][]interface{}{
		{[]byte(strconv.Itoa(9876543))},
		{[]byte(strconv.Itoa(9876544))},
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
	rows = [][]interface{}{
		{[]byte(strconv.Itoa(9876543)), nil, nil, nil},
		{[]byte(strconv.Itoa(9876544)), nil, nil, nil},
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_PrimaryKeyNoMatch_Partial(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 4)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = *GetPkValues(&rows, &pks, &cols)
	pkValues = [][]interface{}{
		pkValues[0],
		pkValues[1],
		{[]byte(strconv.Itoa(9876543))},
		{[]byte(strconv.Itoa(9876544))},
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
	rows = [][]interface{}{
		rows[0],
		rows[1],
		{[]byte(strconv.Itoa(9876543)), nil, nil, nil},
		{[]byte(strconv.Itoa(9876544)), nil, nil, nil},
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)

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
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.NO_PRIMARY_KEY_GIVEN.GetReason(), http.StatusBadRequest)
}

func Test_GetFeatureVector_IncompletePrimaryKey(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		1,
		// Remove one pk
		[]string{pks[0]},
		[][]interface{}{pkValues[0], pkValues[0]},
		nil,
		nil,
	)
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	rows = [][]interface{}{
		nil,
		nil,
	}
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_IncompletePrimaryKey_Join(t *testing.T) {
	var joinKey = make(map[string]string)
	joinKey["id1"] = "bigint"
	rows, _, cols, err := fshelper.GetNSampleDataWithJoinAndKey(
		2, testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_3_1", "", joinKey,
		[]string{"id1", "ts", "data1", "data2"}, []string{"id1", "id2", "bigint"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = make([][]interface{}, 0)
	for _, row := range rows {
		// All required primary key
		pkValues = append(pkValues, []interface{}{row[4], row[5]})
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_1n3",
		1,
		// Do not include primary key from left fg
		[]string{"0_id1", "id2"},
		pkValues,
		nil,
		nil,
	)
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	rows = [][]interface{}{
		nil,
		nil,
	}
	ValidateResponseWithData(t, &rows, &cols, fsResp)

}

func Test_GetFeatureVector_IncompletePrimaryKey_PartialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns(testdbs.FSDB001, "sample_3_1", 3, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		1,
		// Remove one pk
		pks,
		pkValues,
		nil,
		nil,
	)
	delete(*(*fsReq.Entries)[1], "id1")
	rows = [][]interface{}{
		rows[0],
		nil,
		rows[2],
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_WrongPrimaryKey_FeatureNotPk(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
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
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_WrongPrimaryKey_FeatureNotPk_PartialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns(testdbs.FSDB001, "sample_3_1", 3, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		1,
		pks,
		pkValues,
		nil,
		nil,
	)
	delete(*(*fsReq.Entries)[1], "id1")
	delete(*(*fsReq.Entries)[2], "id2")
	var ts = json.RawMessage([]byte(`"2022-01-01"`))
	(*(*fsReq.Entries)[1])["ts"] = &ts
	(*(*fsReq.Entries)[2])["ts"] = &ts
	rows = [][]interface{}{
		rows[0],
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_WrongPrimaryKey_TooManyPk(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
	pks = append(pks, "ts")
	for i := range pkValues {
		pkValues[i] = append(pkValues[i], []byte(`"2022-01-01"`))
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
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_WrongPkType_Int(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = make([][]interface{}, 0)
	for _, row := range rows {
		// Make wrong primary key value
		pkValue := *fshelper.GetPkValues(&row, &pks, &cols)
		for i, pkv := range pkValue {
			pkValue[i] = []byte("\"" + string(pkv.([]byte)) + "\"")
		}
		pkValues = append(pkValues, pkValue)
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
	rows[0][0] = pkValues[0][0]
	rows[1][0] = pkValues[1][0]

	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	// rondb can convert int in string back to int
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_WrongPkType_Str(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns(testdbs.FSDB001, "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = make([][]interface{}, 0)
	for _, row := range rows {
		// Make wrong primary key value
		pkValue := *fshelper.GetPkValues(&row, &pks, &cols)
		for i, pkv := range pkValue {
			pkValue[i] = []byte(string(pkv.([]byte)))
		}
		pkValues = append(pkValues, pkValue)
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
	rows[0][0] = pkValues[0][0]
	rows[1][0] = pkValues[1][0]
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	// rondb can convert int to string
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_WrongPkValue(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = [][]interface{}{
		{[]byte(`"abc1"`), []byte(`"abc2"`)},
		{[]byte(`"abc3"`), []byte(`"abc4"`)},
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
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_Success_ComplexType(t *testing.T) {
	var fsName = testdbs.FSDB002
	var fvName = "sample_complex_type"
	var fvVersion = 1
	rows, pks, cols, err := fshelper.GetSampleData(fsName, "sample_complex_type_1")
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
	var fsReq = CreateFeatureStoreRequest(
		fsName,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
	fsResp := GetFeatureStoreResponse(t, fsReq)
	for _, row := range rows {
		// convert data to object in json format
		arrayJson, err := fshelper.ConvertBinaryToJsonMessage(row[2])
		if err != nil {
			t.Fatalf("Cannot convert to json with error %s ", err)
		}
		arrayPt, err := feature_store.DeserialiseComplexFeature(arrayJson, &arrayDecoder) // array
		row[2] = *arrayPt
		if err != nil {
			t.Fatalf("Cannot deserailize feature with error %s ", err)
		}
		// convert data to object in json format
		mapJson, err := fshelper.ConvertBinaryToJsonMessage(row[3])
		if err != nil {
			t.Fatalf("Cannot convert to json with error %s ", err)
		}
		mapPt, err := feature_store.DeserialiseComplexFeature(mapJson, &mapDecoder) // map
		row[3] = *mapPt
		if err != nil {
			t.Fatalf("Cannot deserailize feature with error %s ", err)
		}
	}
	// validate
	ValidateResponseWithData(t, &rows, &cols, fsResp)
	fshelper.ValidateResponseMetadata(t, &fsResp.Metadata, fsReq.MetadataRequest, fsName, fvName, fvVersion)

}

func Test_GetFeatureVector_WrongPkValue_PartialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns(testdbs.FSDB001, "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = *GetPkValues(&rows, &pks, &cols)
	pkValues[1] = []interface{}{[]byte(`"abc3"`), []byte(`"abc4"`)}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		1,
		pks,
		pkValues,
		nil,
		nil,
	)
	rows = [][]interface{}{
		rows[0],
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_Success_AllTypes(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var passedFeatures = []interface{}{
		rows[0][0],             // id1
		rows[0][1],             // id2
		[]byte(`992`),          // ts
		[]byte(`993`),          // bigint
		[]byte(`"994"`),        // string
		[]byte(`"2022-01-01"`), // date
		[]byte(`true`),         // bool
		[]byte(`1.5`),          // float
		[]byte(`2.5`),          // double
		[]byte(fmt.Sprintf(`"%s"`, base64.StdEncoding.EncodeToString([]byte("EEFF")))), // binary
	}
	for _, row := range rows {

		copy(row, passedFeatures)
		row[len(passedFeatures)-1] = []byte(`"EEFF"`)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		2,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		cols,
		[][]interface{}{passedFeatures, passedFeatures},
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)

}

func Test_PassedFeatures_WrongKey_FeatureNotExist(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		row[2] = []byte("999")
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"invalide_key"},
		[][]interface{}{{[]byte("999")}, {[]byte("999")}},
	)
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_WrongKey_FeatureNotExist_PartialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"invalide_key"},
		[][]interface{}{{[]byte("999")}, {[]byte("999")}},
	)
	(*fsReq.PassedFeatures)[0] = nil
	rows = [][]interface{}{
		rows[0],
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_WrongType_NotString(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		2,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"string"},
		[][]interface{}{{[]byte(`999`)}, {[]byte(`999`)}},
	)
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_WrongType_NotString_PartialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		2,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"string"},
		[][]interface{}{{[]byte(`"abc"`)}, {[]byte(`999`)}},
	)
	rows[0][4] = []byte(`"abc"`)
	rows = [][]interface{}{
		rows[0],
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_WrongType_NotNumber(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		2,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"bigint"},
		[][]interface{}{{[]byte(`"int"`)}, {[]byte(`"int"`)}},
	)
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_WrongType_NotNumber_PartialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		2,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"bigint"},
		[][]interface{}{{[]byte(`"int"`)}, {[]byte(`999`)}},
	)
	rows[1][3] = []byte(`999`)
	rows = [][]interface{}{
		nil,
		rows[1],
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_WrongType_NotBoolean(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		2,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"bool"},
		[][]interface{}{{[]byte(`"int"`)}, {[]byte(`"int"`)}},
	)
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_WrongType_NotBoolean_PartialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_3",
		2,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"bool"},
		[][]interface{}{{[]byte(`true`)}, {[]byte(`"int"`)}},
	)
	rows[0][6] = []byte(`true`)
	rows = [][]interface{}{
		rows[0],
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeature_Success_1entries(t *testing.T) {
	var numEntries = 1
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", numEntries)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pfValues = make([][]interface{}, numEntries)
	for i, row := range rows {
		var pf = []byte(fmt.Sprintf(`"%d"`, i))
		row[2] = pf
		pfValues[i] = []interface{}{pf}
	}

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"data1"},
		pfValues,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeature_Success_5entries(t *testing.T) {
	var numEntries = 5
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB001, "sample_2_1", numEntries)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pfValues = make([][]interface{}, numEntries)
	for i, row := range rows {
		var pf = []byte(fmt.Sprintf(`"%d"`, i))
		row[2] = pf
		pfValues[i] = []interface{}{pf}
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"data1"},
		pfValues,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeature_Success_10entries(t *testing.T) {
	var numEntries = 10
	rows, pks, cols, err := fshelper.GetNSampleData(testdbs.FSDB002, "sample_2_1", numEntries)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pfValues = make([][]interface{}, numEntries)
	for i, row := range rows {
		var pf = []byte(fmt.Sprintf(`"%d"`, i))
		row[2] = pf
		pfValues[i] = []interface{}{pf}
	}
	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB002,
		"sample_2",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"data1"},
		pfValues,
	)
	fsResp := GetFeatureStoreResponse(t, fsReq)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_PassedFeatures_LabelShouldFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin(testdbs.FSDB001, "sample_1_1", testdbs.FSDB001, "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var exCols = make(map[string]bool)
	exCols["data1"] = true
	var fvName = "sample_1n2_label"
	var fvVersion = 1

	var fsReq = CreateFeatureStoreRequest(
		testdbs.FSDB001,
		fvName,
		fvVersion,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		[]string{"data1"},
		[][]interface{}{{[]byte(`300`)}, {[]byte(`300`)}},
	)
	fsReq.MetadataRequest = &api.MetadataRequest{FeatureName: true, FeatureType: true}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	rows = [][]interface{}{
		nil,
		nil,
	}
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}
