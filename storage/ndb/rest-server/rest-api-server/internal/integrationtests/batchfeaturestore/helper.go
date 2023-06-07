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
	"fmt"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	fshelper "hopsworks.ai/rdrs/internal/integrationtests/feature_store"
)

func CreateFeatureStoreRequest(
	fsName string,
	fvName string,
	fvVersion int,
	pks []string,
	batchValues [][]interface{},
	passedFeaturesKey []string,
	batchPassedFeaturesValue [][]interface{},
) *api.BatchFeatureStoreRequest {
	var batchEntries = make([]*map[string]*json.RawMessage, 0)
	for _, value := range batchValues {
		var entries = make(map[string]*json.RawMessage)
		for j, key := range pks {
			val := json.RawMessage(value[j].([]byte))
			entries[key] = &val
		}
		batchEntries = append(batchEntries, &entries)
	}

	var batchPassedFeatures = make([]*map[string]*json.RawMessage, 0)
	for _, value := range batchPassedFeaturesValue {
		var entries = make(map[string]*json.RawMessage)
		for j, key := range passedFeaturesKey {
			val := json.RawMessage(value[j].([]byte))
			entries[key] = &val
		}
		batchPassedFeatures = append(batchPassedFeatures, &entries)
	}
	req := api.BatchFeatureStoreRequest{
		FeatureStoreName:   &fsName,
		FeatureViewName:    &fvName,
		FeatureViewVersion: &fvVersion,
		Entries:            &batchEntries,
		PassedFeatures:     &batchPassedFeatures,
	}
	return &req
}

func GetFeatureStoreResponse(t *testing.T, req *api.BatchFeatureStoreRequest) *api.BatchFeatureStoreResponse {
	return GetFeatureStoreResponseWithDetail(t, req, "", http.StatusOK)
}

func GetFeatureStoreResponseWithDetail(t *testing.T, req *api.BatchFeatureStoreRequest, message string, status int) *api.BatchFeatureStoreResponse {
	reqBody := req.String()
	log.Debugf("Request data is %s", reqBody)
	_, respBody := testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, testutils.NewBatchFeatureStoreURL(), reqBody, message, status)
	if int(status/100) == 2 {
		fsResp := api.BatchFeatureStoreResponse{}
		err := json.Unmarshal([]byte(respBody), &fsResp)
		if err != nil {
			t.Fatalf("Unmarshal failed %s ", err)
		}
		log.Debugf("Response data is %s", fsResp.String())
		return &fsResp
	} else {
		return nil
	}
}

func ValidateResponseWithData(t *testing.T, data *[][]interface{}, cols *[]string, resp *api.BatchFeatureStoreResponse) {
	for i, row := range *data {
		var fsResp = &api.FeatureStoreResponse{}
		fsResp.Metadata = resp.Metadata
		fsResp.Features = resp.Features[i]
		fsResp.Status = resp.Status[i]
		fshelper.ValidateResponseWithData(t, &row, cols, fsResp)
	}
}

func GetPkValues(rows *[][]interface{}, pks *[]string, cols *[]string) *[][]interface{} {
	var pkValues [][]interface{}
	for _, row := range *rows {
		pkValues = append(pkValues, *fshelper.GetPkValues(&row, pks, cols))
	}
	return &pkValues
}
