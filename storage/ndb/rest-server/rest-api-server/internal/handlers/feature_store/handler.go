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
	"errors"
	"fmt"
	"net/http"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/feature_store"
	fsmetadata "hopsworks.ai/rdrs/internal/feature_store"
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
	metadata, err := fsmetadata.GetFeatureViewMetadata(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err
	}
	if log.IsDebug() {
		metadata, _ := json.MarshalIndent(metadata, "", "  ")
		log.Debugf("Feature store metadata is %s", metadata)
	}
	validatePrimaryKey(fsReq.Entries)
	err1 := validatePrimaryKey(fsReq.Entries)
	if err1 != nil {
		return err1
	}
	err2 := validatePassedFeatures(fsReq.Entries, metadata.PrefixFeaturesLookup)
	if err2 != nil {
		return err2
	}
	return nil

}

func validatePrimaryKey(entries *map[string]*json.RawMessage) error {
	// Data type check of primary key will be delegated to rondb.
	if len(*entries) == 0 {
		return errors.New(fmt.Sprintf("No primary key is given."))
	}
	return nil
}

func validatePassedFeatures(passedFeatures *map[string]*json.RawMessage, features *map[string]*feature_store.FeatureMetadata) error {
	for featureName, value := range *passedFeatures {
		feature, ok := (*features)[featureName]
		if !ok {
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
	metadata, err := fsmetadata.GetFeatureViewMetadata(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	var readParams = getBatchPkReadParams(metadata, fsReq.Entries)
	err = h.dbBatchReader.Validate(readParams)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	var dbResponseIntf = getPkReadResponseJSON()
	code, err := h.dbBatchReader.Execute(readParams, *dbResponseIntf)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	fsResp := response.(*api.FeatureStoreResponse)
	features := getFeatureValues(dbResponseIntf, fsReq.Entries, metadata)
	fillPassedFeatures(features, fsReq.PassedFeatures, metadata.PrefixFeaturesLookup, metadata.FeatureIndexLookup)
	fsResp.Features = features

	return code, nil
}

func getFeatureValues(batchResponse *api.BatchOpResponse, entries *map[string]*json.RawMessage, featureView *feature_store.FeatureViewMetadata) *[]interface{} {
	jsonResponse := (*batchResponse).String()
	fsResp := api.BatchResponseJSON{}
	json.Unmarshal([]byte(jsonResponse), &fsResp)
	featureValues := make([]interface{}, *featureView.NumOfFeatures, *featureView.NumOfFeatures)
	for _, response := range *fsResp.Result {
		for featureName, value := range *response.Body.Data {
			featureIndexKey := *response.Body.OperationID + "|" + featureName
			featureValues[(*featureView.FeatureIndexLookup)[featureIndexKey]] = value
		}
	}
	// Fill in primary key into the vector
	for featureName, value := range *entries {
		indexKey := feature_store.GetFeatureIndexKeyByFeature((*featureView.PrefixFeaturesLookup)[featureName])
		if index, ok := (*featureView.FeatureIndexLookup)[*indexKey]; ok {
			featureValues[index] = value
		}
	}
	return &featureValues
}

func getBatchPkReadParams(metadata *feature_store.FeatureViewMetadata, entries *map[string]*json.RawMessage) *[]*api.PKReadParams {

	var batchReadParams = make([]*api.PKReadParams, 0, 0)
	for _, fgFeature := range *metadata.FeatureGroupFeatures {
		testDb := fgFeature.FeatureStoreName
		testTable := fmt.Sprintf("%s_%d", *fgFeature.FeatureGroupName, *fgFeature.FeatureGroupVersion)
		var filters = make([]api.Filter, 0, 0)
		var columns = make([]api.ReadColumn, 0, 0)
		for _, feature := range *fgFeature.Features {
			if value, ok := (*entries)[feature.Prefix+feature.Name]; ok {
				var filter = api.Filter{&feature.Name, value}
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
		param := api.PKReadParams{testDb, &testTable, &filters, &columns, &opId}
		batchReadParams = append(batchReadParams, &param)
	}
	return &batchReadParams
}

func getPkReadResponseJSON() *api.BatchOpResponse {
	response := (api.BatchOpResponse)(&api.BatchResponseJSON{})
	response.Init()
	return &response
}

func fillPassedFeatures(features *[]interface{}, passedFeatures *map[string]*json.RawMessage, featureMetadata *map[string]*feature_store.FeatureMetadata, indexLookup *map[string]int) {
	for featureName, passFeature := range *passedFeatures {
		var feature = (*featureMetadata)[featureName]
		var lookupKey = feature_store.GetFeatureIndexKey(feature.FeatureStoreName, feature.FeatureGroupName, feature.Name)
		(*features)[(*indexLookup)[*lookupKey]] = passFeature
	}

}
