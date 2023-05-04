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
	"fmt"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	fsmetadata "hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func createFeatureStoreRequest(
	fsName string,
	fvName string,
	fvVersion int,
	pk []string,
	values []interface{},
	passedFeaturesKey []string,
	passedFeaturesValue []interface{},
) *api.FeatureStoreRequest {
	var entries = make(map[string]*json.RawMessage)
	for i, key := range pk {
		val := json.RawMessage(values[i].([]byte))
		entries[key] = &val
	}
	var passedFeatures = make(map[string]*json.RawMessage)
	for i, key := range passedFeaturesKey {
		val := json.RawMessage(passedFeaturesValue[i].([]byte))
		passedFeatures[key] = &val
	}
	req := api.FeatureStoreRequest{
		FeatureStoreName:   &fsName,
		FeatureViewName:    &fvName,
		FeatureViewVersion: &fvVersion,
		Entries:            &entries,
		PassedFeatures:     &passedFeatures,
	}
	return &req
}

func TestFeatureStore(t *testing.T) {
	var fsName = "fsdb002"
	var fvName = "sample_1n2"
	var fvVersion = 1
	key := string("id1")
	value1 := json.RawMessage(`87`)
	value2 := json.RawMessage(`730`)
	var entries = make(map[string]*json.RawMessage)
	entries[key] = &value1
	entries[string("fg2_id1")] = &value2
	var passedFeatures = make(map[string]*json.RawMessage)
	pfValue := json.RawMessage(`999`)
	passedFeatures["data1"] = &pfValue
	req := api.FeatureStoreRequest{
		FeatureStoreName:   &fsName,
		FeatureViewName:    &fvName,
		FeatureViewVersion: &fvVersion,
		Entries:            &entries,
		PassedFeatures:     &passedFeatures}
	reqBody := fmt.Sprintf("%s", req)
	log.Debugf("Request body: %s", reqBody)
	_, respBody := testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, testutils.NewFeatureStoreURL(), reqBody, "", http.StatusOK)

	fsResp := api.FeatureStoreResponse{}
	err := json.Unmarshal([]byte(respBody), &fsResp)
	if err != nil {
		t.Fatalf("Unmarshal failed %s ", err)
	}

	log.Infof("Response data is %s", fsResp.String())
}

func TestFeatureStoreMetaData(t *testing.T) {

	md, err := fsmetadata.GetFeatureViewMetadata("fsdb002", "sample_2", 1)
	if err != nil {
		t.Fatalf("Reading FS Metadata failed %v ", err)
	}

	mdJson, _ := json.MarshalIndent(md, "", "  ")
	log.Infof("Feature store metadata is %s", mdJson)
}

func getFeatureStoreResponse(t *testing.T, req *api.FeatureStoreRequest) *api.FeatureStoreResponse {
	reqBody := fmt.Sprintf("%s", req)
	_, respBody := testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, testutils.NewFeatureStoreURL(), reqBody, "", http.StatusOK)
	fsResp := api.FeatureStoreResponse{}
	err := json.Unmarshal([]byte(respBody), &fsResp)
	if err != nil {
		t.Fatalf("Unmarshal failed %s ", err)
	}
	return &fsResp
}

func Metadata_success(t *testing.T) {
	// req := createFeatureStoreRequest(
	// 	"",
	// 	"",
	// 	0,
	// 	[]string{},
	// 	[]interface{}{},
	// 	nil,
	// 	nil,
	// )
	// rep := getFeatureStoreResponse(req)
}

func MetadataNotExist(t *testing.T) {

}

func PrimaryKey_success(t *testing.T) {
	// req := createFeatureStoreRequest(
	// 	"",
	// 	"",
	// 	0,
	// 	[]string{},
	// 	[]interface{}{},
	// 	nil,
	// 	nil,
	// )
	// rep := getFeatureStoreResponse(req)
}

func PrimaryKey_wrongKey(t *testing.T) {

}

func PrimaryKey_wrongValue(t *testing.T) {

}

func PrimaryKey_missingKey(t *testing.T) {

}

func PrimaryKey_wrongType(t *testing.T) {

}

func PassedFeatures_success(t *testing.T) {

}

func PassedFeatures_wrongKey(t *testing.T) {

}

func PassedFeatures_wrongType(t *testing.T) {

}

func GetFeatures_Join(t *testing.T) {

}

func GetFeatures_ReturnMixedDataType(t *testing.T) {

}
