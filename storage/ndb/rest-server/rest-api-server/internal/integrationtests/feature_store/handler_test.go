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

	fsmetadata "hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/pkg/api"
)

func TestFeatureStoreMetaData(t *testing.T) {

	_, err := fsmetadata.GetFeatureViewMetadata("fsdb002", "sample_2", 1)
	if err != nil {
		t.Fatalf("Reading FS Metadata failed %v ", err)
	}
}

func TestMetadata_FsNotExist(t *testing.T) {
	_, err := fsmetadata.GetFeatureViewMetadata("NA", "sample_2", 1)
	if err == nil {
		t.Fatalf("This should fail.")
	}
	if !strings.Contains(err.Error().Error(), fsmetadata.FS_NOT_EXIST.GetReason()) {
		t.Fatalf("This should fail with error message: %s.", fsmetadata.FS_NOT_EXIST.GetReason())
	}
}

func TestMetadata_FvNotExist(t *testing.T) {
	_, err := fsmetadata.GetFeatureViewMetadata("fsdb002", "NA", 1)
	if err == nil {
		t.Fatalf("This should fail.")
	}
	if !strings.Contains(err.Error().Error(), fsmetadata.FV_NOT_EXIST.GetReason()) {
		t.Fatalf("This should fail with error message: %s.", fsmetadata.FV_NOT_EXIST.GetReason())
	}
}

func Test_GetFeatureVector_success(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb002", "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb002",
			"sample_2",
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

func Test_GetFeatureVector_withMetadata_All_success(t *testing.T) {
	var fsName = "fsdb002"
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

func Test_GetFeatureVector_withMetadata_Name_success(t *testing.T) {
	var fsName = "fsdb002"
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

func Test_GetFeatureVector_FsNotExist(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb002", "sample_2_1")
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
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FS_NOT_EXIST.GetReason(), http.StatusUnauthorized)

	}
}

func Test_GetFeatureVector_FvNotExist(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb002", "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb002",
			"NA",
			1,
			pks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FV_NOT_EXIST.GetReason(), http.StatusUnauthorized)
	}
}

func Test_GetFeatureVector_CompositePrimaryKey(t *testing.T) {
	rows, pks, cols, err := GetNSampleDataColumns("fsdb001", "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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

func Test_GetFeatureVector_join(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin("fsdb002", "sample_1_1", "fsdb002", "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb002",
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

func Test_GetFeatureVector_join_Metadata_all(t *testing.T) {
	var fsName = "fsdb002"
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

func Test_GetFeatureVector_join_Metadata_name(t *testing.T) {
	var fsName = "fsdb002"
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

func Test_GetFeatureVector_joinSameFg(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin("fsdb001", "sample_1_1", "fsdb001", "sample_1_2", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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

func Test_GetFeatureVector_shared(t *testing.T) {
	rows, pks, cols, err := GetSampleDataWithJoin("fsdb001", "sample_1_1", "fsdb002", "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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

func Test_GetFeatureVector_notShared(t *testing.T) {
	rows, pks, cols, err := GetNSampleDataColumns("fsdb001", "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})
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

func Test_GetFeatureVector_wrongPrimaryKey_notExist(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb002", "sample_2_1")

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
			"fsdb002",
			"sample_2",
			1,
			wrongPks,
			*GetPkValues(&row, &pks, &cols),
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FEATURE_NOT_EXIST.GetReason(), http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_primaryKeyNoMatch(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb002", "sample_2_1")

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
			"fsdb002",
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

func Test_GetFeatureVector_noPrimaryKey(t *testing.T) {
	var fsReq = CreateFeatureStoreRequest(
		"fsdb002",
		"sample_2",
		1,
		nil,
		nil,
		nil,
		nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.INCORRECT_PRIMARY_KEY.GetReason(), http.StatusBadRequest)
}

func Test_GetFeatureVector_incompletePrimaryKey(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	for _, row := range rows {
		var pkValues = *GetPkValues(&row, &pks, &cols)
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
			"sample_3",
			1,
			// Remove one pk
			[]string{pks[0]},
			[]interface{}{pkValues[0]},
			nil,
			nil,
		)
		GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.INCORRECT_PRIMARY_KEY.GetReason(), http.StatusBadRequest)
	}
}

func Test_GetFeatureVector_wrongPrimaryKey_featureNotPk(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	for _, row := range rows {
		var pkValues = *GetPkValues(&row, &pks, &cols)
		pks[0] = "ts"
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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

func Test_GetFeatureVector_wrongPkType_int(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb002", "sample_2_1")

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
			"fsdb002",
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

func Test_GetFeatureVector_wrongPkType_str(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")

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
			"fsdb001",
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

func Test_GetFeatureVector_wrongPkValue(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")

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
			"fsdb001",
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

func Test_PassedFeatures_success(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb002", "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		row[2] = []byte(`"999"`)
		var fsReq = CreateFeatureStoreRequest(
			"fsdb002",
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

func Test_PassedFeatures_success_allTypes(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var passedFeatures = []interface{}{
			row[0],                 // id1
			row[1],                 // id2
			[]byte(`992`),          // ts
			[]byte(`993`),          // bigint
			[]byte(`"994"`),        // string
			[]byte(`"2022-01-01"`), // date
			[]byte(`true`),         // bool
			[]byte(`1.5`),          // float
			[]byte(`2.5`),          // double
			[]byte(fmt.Sprintf(`"%s"`, base64.StdEncoding.EncodeToString([]byte("EEFF")))), // binary
		}
		for i, pf := range passedFeatures {
			row[i] = pf
		}
		row[len(passedFeatures)-1] = []byte(`"EEFF"`)
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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

func Test_PassedFeatures_wrongKey_featureNotExist(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb002", "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		row[2] = []byte("999")
		var fsReq = CreateFeatureStoreRequest(
			"fsdb002",
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

func Test_PassedFeatures_wrongType_notString(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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

func Test_PassedFeatures_wrongType_notNumber(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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

func Test_PassedFeatures_wrongType_notBoolean(t *testing.T) {
	rows, pks, cols, err := GetSampleData("fsdb001", "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb001",
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
