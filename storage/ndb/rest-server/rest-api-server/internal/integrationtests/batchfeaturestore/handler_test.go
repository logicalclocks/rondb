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

	fsmetadata "hopsworks.ai/rdrs/internal/feature_store"
	fshelper "hopsworks.ai/rdrs/internal/integrationtests/feature_store"
	"hopsworks.ai/rdrs/pkg/api"
)

func Test_GetFeatureVector_success_1entries(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 1)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		var fsReq = CreateFeatureStoreRequest(
			"fsdb002",
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

func Test_GetFeatureVector_success_5entries(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 5)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		"fsdb002",
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

func Test_GetFeatureVector_5entries_Metadata_success(t *testing.T) {
	var fsName = "fsdb002"
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

func Test_GetFeatureVector_5entries_Metadata_Name_success(t *testing.T) {
	var fsName = "fsdb002"
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

func Test_GetFeatureVector_success_10entries(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 10)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		"fsdb002",
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

func Test_GetFeatureVector_FsNotExist(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData("fsdb002", "sample_2_1")
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
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FS_NOT_EXIST.GetReason(), http.StatusUnauthorized)
}

func Test_GetFeatureVector_FvNotExist(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleData("fsdb002", "sample_2_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb002",
		"NA",
		1,
		pks,
		*GetPkValues(&rows, &pks, &cols),
		nil,
		nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.FV_NOT_EXIST.GetReason(), http.StatusUnauthorized)

}

func Test_GetFeatureVector_CompositePrimaryKey(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns("fsdb001", "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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
	rows, pks, cols, err := fshelper.GetSampleData("fsdb001", "sample_3_1")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_GetFeatureVector_join(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin("fsdb002", "sample_1_1", "fsdb002", "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb002",
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

func Test_GetFeatureVector_join_Metadata(t *testing.T) {
	var fsName = "fsdb002"
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

func Test_GetFeatureVector_join_Metadata_Name(t *testing.T) {
	var fsName = "fsdb002"
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
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin("fsdb001", "sample_1_1", "fsdb001", "sample_1_2", "fg1_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_GetFeatureVector_shared(t *testing.T) {
	rows, pks, cols, err := fshelper.GetSampleDataWithJoin("fsdb001", "sample_1_1", "fsdb002", "sample_2_1", "fg2_")
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_GetFeatureVector_notShared(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns("fsdb001", "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

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

func Test_GetFeatureVector_wrongPrimaryKey_notExist(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	// Make wrong primary key
	var wrongPks = make([]string, len(pks))
	for i, pk := range pks {
		wrongPks[i] = pk + "abcd"
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb002",
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

func Test_GetFeatureVector_primaryKeyNoMatch(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = [][]interface{}{
		{[]byte(strconv.Itoa(9876543))},
		{[]byte(strconv.Itoa(9876544))},
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
	rows = [][]interface{}{
		{[]byte(strconv.Itoa(9876543)), nil, nil, nil},
		{[]byte(strconv.Itoa(9876544)), nil, nil, nil},
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_primaryKeyNoMatch_partial(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 4)

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
		"fsdb002",
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
	GetFeatureStoreResponseWithDetail(t, fsReq, fsmetadata.NO_PRIMARY_KEY_GIVEN.GetReason(), http.StatusBadRequest)
}

func Test_GetFeatureVector_incompletePrimaryKey(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_GetFeatureVector_incompletePrimaryKey_partialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns("fsdb001", "sample_3_1", 3, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_GetFeatureVector_wrongPrimaryKey_featureNotPk(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
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
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_wrongPrimaryKey_featureNotPk_partialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns("fsdb001", "sample_3_1", 3, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var pkValues = *GetPkValues(&rows, &pks, &cols)
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_GetFeatureVector_wrongPkType_int(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 2)

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
		"fsdb002",
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

func Test_GetFeatureVector_wrongPkType_str(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns("fsdb001", "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

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
		"fsdb001",
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

func Test_GetFeatureVector_wrongPkValue(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = [][]interface{}{
		{[]byte(`"abc1"`), []byte(`"abc2"`)},
		{[]byte(`"abc3"`), []byte(`"abc4"`)},
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
	rows = [][]interface{}{
		nil,
		nil,
	}
	var fsResp = GetFeatureStoreResponseWithDetail(t, fsReq, "", http.StatusOK)
	ValidateResponseWithData(t, &rows, &cols, fsResp)
}

func Test_GetFeatureVector_wrongPkValue_partialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleDataColumns("fsdb001", "sample_3_1", 2, []string{"`id1`", "`id2`", "`ts`", "`bigint`"})

	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var pkValues = *GetPkValues(&rows, &pks, &cols)
	pkValues[1] = []interface{}{[]byte(`"abc3"`), []byte(`"abc4"`)}

	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_PassedFeatures_success_allTypes(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 1)
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

		for i, pf := range passedFeatures {
			row[i] = pf
		}
		row[len(passedFeatures)-1] = []byte(`"EEFF"`)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_PassedFeatures_wrongKey_featureNotExist(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	for _, row := range rows {
		row[2] = []byte("999")
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb002",
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

func Test_PassedFeatures_wrongKey_featureNotExist_partialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb002",
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

func Test_PassedFeatures_wrongType_notString(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_PassedFeatures_wrongType_notString_partialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_PassedFeatures_wrongType_notNumber(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_PassedFeatures_wrongType_notNumber_partialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}

	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_PassedFeatures_wrongType_notBoolean(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_PassedFeatures_wrongType_notBoolean_partialFail(t *testing.T) {
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_3_1", 2)
	if err != nil {
		t.Fatalf("Cannot get sample data with error %s ", err)
	}
	var fsReq = CreateFeatureStoreRequest(
		"fsdb001",
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

func Test_PassedFeature_success_1entries(t *testing.T) {
	var numEntries = 1
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", numEntries)
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
		"fsdb002",
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

func Test_PassedFeature_success_5entries(t *testing.T) {
	var numEntries = 5
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb001", "sample_2_1", numEntries)
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
		"fsdb001",
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

func Test_PassedFeature_success_10entries(t *testing.T) {
	var numEntries = 10
	rows, pks, cols, err := fshelper.GetNSampleData("fsdb002", "sample_2_1", numEntries)
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
		"fsdb002",
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
