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
	"net/http"
	"fmt"
	"errors"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers/batchpkread"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct {
	apiKeyCache   apikey.Cache
	dbBatchReader batchpkread.Handler
}

func New(apiKeyCache apikey.Cache, batchPkReadHandler batchpkread.Handler) Handler {
	return Handler{apiKeyCache, batchPkReadHandler}
}

func (h *Handler) Validate(request interface{}) error {
	fsReq := request.(*api.FeatureStoreRequest)
	metadata := getMetadata()
	validatePrimaryKey(fsReq.Entries, metadata.PrefixPrimaryKeyLookup)
	err1 := validatePrimaryKey(fsReq.Entries, metadata.PrefixPrimaryKeyLookup)
	if err1 != nil {
		return err1
	}
	err2 := validatePassedFeatures(fsReq.Entries, metadata.PrefixFeaturesLookup)
	if err2 != nil {
		return err2
	}
	return nil

}

func validatePrimaryKey(entries *map[string]*json.RawMessage, primaryKey *map[string]*api.FeatureMetadata) error {
	// Data type check of primary key will be delegated to rondb.
	if (len(*entries) != len(*primaryKey)) {
		return errors.New(fmt.Sprintf("Size of given primary key is not correct, expecting  %d, but got %d", len(*primaryKey), len(*entries)))
	}
	for pk, _ := range *primaryKey {
		if _, ok := (*entries)[pk]; !ok {
			return errors.New(fmt.Sprintf("Require primary key `%s` does not exist.", pk))
		}
	}
	return nil
}

func validatePassedFeatures(passedFeatures *map[string]*json.RawMessage, features *map[string]*api.FeatureMetadata) error {
	for featureName, value := range *passedFeatures {
		feature, ok := (*features)[featureName]
		if (!ok) {
			return errors.New(fmt.Sprintf("Feature `%s` does not exist in the feature view.", featureName))
		}
		validateFeatureType(value, feature.Type)
	}
	return nil
}

func validateFeatureType(feature *json.RawMessage, featureType string) {
	// TODO: do simple number/string check?
}

func (h *Handler) Authenticate(apiKey *string, request interface{}) error {
	conf := config.GetAll()
	if !conf.Security.APIKey.UseHopsworksAPIKeys {
		return nil
	}
	return h.apiKeyCache.ValidateAPIKey(apiKey)
}

func (h *Handler) Execute(request interface{}, response interface{}) (int, error) {
	
	fsReq := request.(*api.FeatureStoreRequest)
	if log.IsDebug() {
		log.Debugf("Feature store request received %v", fsReq)
	}
	metadata := getMetadata()
	if log.IsDebug() {
		log.Debugf(metadata.String())
	}
	var readParams = getBatchPkReadParams(metadata, fsReq.Entries)
	err := h.dbBatchReader.Validate(readParams)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	var dbResponseIntf = getPkReadResponseJSON()
	code, err := h.dbBatchReader.Execute(readParams, *dbResponseIntf)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	fsResp := response.(*api.FeatureStoreResponse)
	features := getFeatureValues(dbResponseIntf, metadata)
	fillPassedFeatures(features, fsReq.PassedFeatures, metadata.PrefixFeaturesLookup, metadata.FeatureExPkIndexLookup)
	fsResp.Features = features

	return code, nil
}

func getMetadata() *api.FeatureViewMetadata {
	var f1 = api.FeatureMetadata{FeatureStoreName: "test2", FeatureGroupName: "sample_1_1", Name: "id1", Type: "int", PrimaryKey: true, Index: 1}
	var f2 = api.FeatureMetadata{FeatureStoreName: "test2", FeatureGroupName: "sample_1_1", Name: "data1", Type: "int", PrimaryKey: false, Index: 2}
	var f3 = api.FeatureMetadata{FeatureStoreName: "test2", FeatureGroupName: "sample_1_1", Name: "data2", Type: "int", PrimaryKey: false, Index: 3}
	var f4 = api.FeatureMetadata{FeatureStoreName: "test2", FeatureGroupName: "sample_3_1", Name: "id1", Type: "int", PrimaryKey: true, Prefix: "fg2_", Index: 4}
	var f5 = api.FeatureMetadata{FeatureStoreName: "test2", FeatureGroupName: "sample_3_1", Name: "data2", Type: "int", PrimaryKey: false, Prefix: "fg2_", Index: 0}

	var features = []*api.FeatureMetadata{&f1, &f2, &f3, &f4, &f5}
	return api.NewFeatureViewMetadata(nil, nil, nil, nil, nil, &features)
}

func getFeatureValues(batchResponse *api.BatchOpResponse, featureView *api.FeatureViewMetadata) *[]interface{} {
	jsonResponse := (*batchResponse).String()
	fsResp := api.BatchResponseJSON{}
	json.Unmarshal([]byte(jsonResponse), &fsResp)
	featureValues := make([]interface{}, *featureView.NumOfFeatureExPk, *featureView.NumOfFeatureExPk)
	for _, response:= range *fsResp.Result {
		for featureName, value := range *response.Body.Data {
			featureIndexKey := *response.Body.OperationID + "|" + featureName
			featureValues[(*featureView.FeatureExPkIndexLookup)[featureIndexKey]] = value
		}
	}
	return &featureValues
}

func getBatchPkReadParams(metadata *api.FeatureViewMetadata, entries *map[string]*json.RawMessage) *[]*api.PKReadParams {

	var batchReadParams = make([]*api.PKReadParams, 0, 0)
	for _, fgFeature := range *metadata.FeatureGroupFeatures {
		testDb := fgFeature.FeatureStoreName
		testTable := fgFeature.FeatureGroupName
		var filters = make([]api.Filter, 0, 0)
		var columns = make([]api.ReadColumn, 0, 0)
		for _, feature := range *fgFeature.Features {
			if (feature.PrimaryKey) {
				var filter = api.Filter{&feature.Name, (*entries)[feature.Prefix + feature.Name]}
				filters = append(filters, filter)
				if log.IsDebug() {
					log.Debugf("Add to filter: %s", feature.Name)
				}
			} else {
				var colName = feature.Name
				var colType = "default"
				readCol := api.ReadColumn{&colName, &colType}
				columns = append(columns, readCol)
				if log.IsDebug() {
					log.Debugf("Add to column: %s", feature.Name)
				}
			}
		}
		var opId = *fgFeature.FeatureStoreName + "|" + *fgFeature.FeatureGroupName
		param := api.PKReadParams{testDb, testTable, &filters, &columns, &opId}
		batchReadParams = append(batchReadParams, &param)
	}
	return &batchReadParams
}

func getPkReadResponseJSON() *api.BatchOpResponse {
	response := (api.BatchOpResponse)(&api.BatchResponseJSON{})
	response.Init()
	return &response
}

func fillPassedFeatures(features *[]interface{}, passedFeatures *map[string]*json.RawMessage, featureMetadata *map[string]*api.FeatureMetadata, indexLookup *map[string]int) {
	for featureName, passFeature := range *passedFeatures {
		var feature = (*featureMetadata)[featureName]
		var lookupKey = api.GetFeatureIndexKey(feature.FeatureStoreName, feature.FeatureGroupName, feature.Name)
		(*features)[(*indexLookup)[*lookupKey]] = passFeature	
	}

}
