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
	"regexp"
	"strings"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/handlers/batchpkread"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

const (
	WRONG_DATA_TYPE          = "Wrong data type."
	FEATURE_NOT_EXIST        = "Feature does not exist."
	INCORRECT_PRIMARY_KEY    = "Incorrect primary key."
	INCORRECT_PASSED_FEATURE = "Incorrect passed feature."
)

const (
	JSON_NUMBER  = "NUMBER"
	JSON_STRING  = "STRING"
	JSON_BOOLEAN = "BOOLEAN"
	JSON_NIL     = "NIL"
	JSON_OTHER   = "OTHER"
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
	metadata, err := feature_store.GetFeatureViewMetadata(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err
	}
	if log.IsDebug() {
		metadata, _ := json.MarshalIndent(metadata, "", "  ")
		log.Debugf("Feature store request is %s", fsReq.String())
		log.Debugf("Feature store metadata is %s", metadata)
	}
	err1 := validatePrimaryKey(fsReq.Entries, &metadata.PrefixFeaturesLookup)
	if err1 != nil {
		return err1
	}
	err2 := validatePassedFeatures(fsReq.PassedFeatures, &metadata.PrefixFeaturesLookup)
	if err2 != nil {
		return err2
	}
	return nil

}

func validatePrimaryKey(entries *map[string]*json.RawMessage, features *map[string]*feature_store.FeatureMetadata) error {
	// Data type check of primary key will be delegated to rondb.
	if len(*entries) == 0 {
		return fmt.Errorf("%s Error: No primary key is given.", INCORRECT_PRIMARY_KEY)
	}
	for featureName := range *entries {
		_, ok := (*features)[featureName]
		if !ok {
			return fmt.Errorf("%s. Provided primary key `%s` does not exist in the feature view.", FEATURE_NOT_EXIST, featureName)
		}
	}
	return nil
}

func validatePassedFeatures(passedFeatures *map[string]*json.RawMessage, features *map[string]*feature_store.FeatureMetadata) error {
	for featureName, value := range *passedFeatures {
		feature, ok := (*features)[featureName]
		if !ok {
			return fmt.Errorf("%s. Feature `%s` does not exist in the feature view.", FEATURE_NOT_EXIST, featureName)
		}
		err := validateFeatureType(value, feature.Type)
		if err != nil {
			return fmt.Errorf("%s feature: %s (value: %s); Error:  %s", INCORRECT_PASSED_FEATURE, feature.Name, *value, err.Error())
		}
	}
	return nil
}

func validateFeatureType(feature *json.RawMessage, featureType string) error {
	var got, err = getJsonType(feature)

	if err != nil {
		return fmt.Errorf("Provided value %s is not in correct JSON format. %s", feature, err)
	}
	var expected = mapFeatureTypeToJsonType(featureType)
	if got != expected {
		return fmt.Errorf("%s Got: '%s', expected: '%s' (offline type: %s)", WRONG_DATA_TYPE, got, expected, featureType)
	}
	return nil
}

func mapFeatureTypeToJsonType(featureType string) string {
	switch featureType {
	case "boolean":
		return JSON_BOOLEAN
	case "tinyint":
		return JSON_NUMBER
	case "int":
		return JSON_NUMBER
	case "smallint":
		return JSON_NUMBER
	case "bigint":
		return JSON_NUMBER
	case "float":
		return JSON_NUMBER
	case "double":
		return JSON_NUMBER
	case "decimal":
		return JSON_NUMBER
	case "timestamp":
		return JSON_NUMBER
	case "date":
		return JSON_STRING
	case "string":
		return JSON_STRING
	case "binary":
		return JSON_STRING
	default:
		return JSON_OTHER
	}
}

func getJsonType(jsonString *json.RawMessage) (string, error) {
	var value interface{}
	if err := json.Unmarshal(*jsonString, &value); err != nil {
		return "", err
	}

	switch value.(type) {
	case float64:
		return JSON_NUMBER, nil
	case string:
		return JSON_STRING, nil
	case bool:
		return JSON_BOOLEAN, nil
	case nil:
		return JSON_NIL, nil
	default:
		return JSON_OTHER, nil
	}
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
	metadata, err := feature_store.GetFeatureViewMetadata(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	var readParams = getBatchPkReadParams(metadata, fsReq.Entries)
	err = h.dbBatchReader.Validate(readParams)
	if err != nil {
		return http.StatusBadRequest, err
	}
	var dbResponseIntf = getPkReadResponseJSON()
	code, err := h.dbBatchReader.Execute(readParams, *dbResponseIntf)
	if err != nil {
		return translateRonDbError(code, err)
	}
	fsResp := response.(*api.FeatureStoreResponse)
	features := getFeatureValues(dbResponseIntf, fsReq.Entries, metadata)
	fillPassedFeatures(features, fsReq.PassedFeatures, &metadata.PrefixFeaturesLookup, &metadata.FeatureIndexLookup)
	fsResp.Features = *features
	featureMetadatas := make([]*api.FeatureMeatadata, metadata.NumOfFeatures, metadata.NumOfFeatures)
	for featureKey, metadata := range metadata.PrefixFeaturesLookup {
		featureMetadata := api.FeatureMeatadata{}
		featureMetadata.Name = featureKey
		featureMetadata.Type = metadata.Type
		featureMetadatas[metadata.Index] = &featureMetadata
	}
	fsResp.Metadata = featureMetadatas
	return code, nil
}

func translateRonDbError(code int, err error) (fsCode int, fsError error) {
	if strings.Contains(err.Error(), "Wrong data type.") {
		regex := regexp.MustCompile(`Expecting (\w+)\. Column: (\w+)`)
		match := regex.FindStringSubmatch(err.Error())
		var errorMessage string
		if match != nil {
			dataType := match[1]
			columnName := match[2]
			errorMessage = fmt.Sprintf("%s Primary key '%s' should be in '%s' format.", WRONG_DATA_TYPE, columnName, dataType)
		} else {
			errorMessage = WRONG_DATA_TYPE
		}
		return http.StatusBadRequest, fmt.Errorf(errorMessage)
	}
	if strings.Contains(err.Error(), "Wrong number of primary-key columns.") ||
		strings.Contains(err.Error(), "Wrong primay-key column.") {
		return http.StatusBadRequest, fmt.Errorf("%s Error: %s", INCORRECT_PRIMARY_KEY, err.Error())
	}
	return code, err
}

func getFeatureValues(batchResponse *api.BatchOpResponse, entries *map[string]*json.RawMessage, featureView *feature_store.FeatureViewMetadata) *[]interface{} {
	jsonResponse := (*batchResponse).String()
	fsResp := api.BatchResponseJSON{}
	json.Unmarshal([]byte(jsonResponse), &fsResp)
	featureValues := make([]interface{}, featureView.NumOfFeatures, featureView.NumOfFeatures)
	for _, response := range *fsResp.Result {
		for featureName, value := range *response.Body.Data {
			featureIndexKey := *response.Body.OperationID + "|" + featureName
			featureValues[(featureView.FeatureIndexLookup)[featureIndexKey]] = value
		}
	}
	// Fill in primary key into the vector
	for featureName, value := range *entries {
		indexKey := feature_store.GetFeatureIndexKeyByFeature((featureView.PrefixFeaturesLookup)[featureName])
		if index, ok := (featureView.FeatureIndexLookup)[*indexKey]; ok {
			featureValues[index] = value
		}
	}
	return &featureValues
}

func getBatchPkReadParams(metadata *feature_store.FeatureViewMetadata, entries *map[string]*json.RawMessage) *[]*api.PKReadParams {

	var batchReadParams = make([]*api.PKReadParams, 0, 0)
	for _, fgFeature := range metadata.FeatureGroupFeatures {
		testDb := fgFeature.FeatureStoreName
		testTable := fmt.Sprintf("%s_%d", fgFeature.FeatureGroupName, fgFeature.FeatureGroupVersion)
		var filters = make([]api.Filter, 0, 0)
		var columns = make([]api.ReadColumn, 0, 0)
		for _, feature := range fgFeature.Features {
			if value, ok := (*entries)[feature.Prefix+feature.Name]; ok {
				var filter = api.Filter{Column: &feature.Name, Value: value}
				filters = append(filters, filter)
				if log.IsDebug() {
					log.Debugf("Add to filter: %s", feature.Name)
				}
			} else {
				var colName = feature.Name
				var colType = "default"
				readCol := api.ReadColumn{Column: &colName, DataReturnType: &colType}
				columns = append(columns, readCol)
				if log.IsDebug() {
					log.Debugf("Add to column: %s", feature.Name)
				}
			}
		}
		var opId = fgFeature.FeatureStoreName + "|" + fgFeature.FeatureGroupName
		param := api.PKReadParams{DB: &testDb, Table: &testTable, Filters: &filters, ReadColumns: &columns, OperationID: &opId}
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
