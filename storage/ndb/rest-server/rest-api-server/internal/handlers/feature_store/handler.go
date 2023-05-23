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
	JSON_NUMBER  = "NUMBER"
	JSON_STRING  = "STRING"
	JSON_BOOLEAN = "BOOLEAN"
	JSON_NIL     = "NIL"
	JSON_OTHER   = "OTHER"
)

type Handler struct {
	fvMetaCache   *feature_store.FeatureViewMetaDataCache
	apiKeyCache   apikey.Cache
	dbBatchReader batchpkread.Handler
}

func New(fvMetaCache *feature_store.FeatureViewMetaDataCache, apiKeyCache apikey.Cache, batchPkReadHandler batchpkread.Handler) Handler {
	return Handler{fvMetaCache, apiKeyCache, batchPkReadHandler}
}

func (h *Handler) Validate(request interface{}) error {
	fsReq := request.(*api.FeatureStoreRequest)
	metadata, err := h.fvMetaCache.Get(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err.Error()
	}
	if log.IsDebug() {
		log.Debugf("Feature store request is %s", fsReq.String())
	}
	err1 := validatePrimaryKey(fsReq.Entries, &metadata.PrefixFeaturesLookup)
	if err1 != nil {
		return err1.Error()
	}
	err2 := validatePassedFeatures(fsReq.PassedFeatures, &metadata.PrefixFeaturesLookup)
	if err2 != nil {
		return err2.Error()
	}
	return nil

}

func validatePrimaryKey(entries *map[string]*json.RawMessage, features *map[string]*feature_store.FeatureMetadata) *feature_store.RestErrorCode {
	// Data type check of primary key will be delegated to rondb.
	if len(*entries) == 0 {
		return feature_store.INCORRECT_PRIMARY_KEY
	}
	for featureName := range *entries {
		_, ok := (*features)[featureName]
		if !ok {
			return feature_store.FEATURE_NOT_EXIST.NewMessage(fmt.Sprintf("Provided primary key `%s` does not exist in the feature view.", featureName))
		}
	}
	return nil
}

func validatePassedFeatures(passedFeatures *map[string]*json.RawMessage, features *map[string]*feature_store.FeatureMetadata) *feature_store.RestErrorCode {
	for featureName, value := range *passedFeatures {
		feature, ok := (*features)[featureName]
		if !ok {
			return feature_store.FEATURE_NOT_EXIST.NewMessage(fmt.Sprintf("Feature `%s` does not exist in the feature view.", featureName))
		}
		err := validateFeatureType(value, feature.Type)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateFeatureType(feature *json.RawMessage, featureType string) *feature_store.RestErrorCode {
	var got, err = getJsonType(feature)

	if err != nil {
		return feature_store.INCORRECT_FEATURE_VALUE.NewMessage(fmt.Sprintf("Provided value %s is not in correct JSON format. %s", feature, err))
	}
	var expected = mapFeatureTypeToJsonType(featureType)
	if got != expected {
		return feature_store.WRONG_DATA_TYPE.NewMessage(fmt.Sprintf("Got: '%s', expected: '%s' (offline type: %s)", got, expected, featureType))
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
	fsReq := request.(*api.FeatureStoreRequest)
	metadata, err := h.fvMetaCache.Get(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err.Error()
	}
	valErr := h.apiKeyCache.ValidateAPIKey(apiKey, metadata.FeatureStoreNames...)
	if valErr != nil {
		return feature_store.FEATURE_STORE_NOT_SHARED.Error()
	}
	return nil
}

func (h *Handler) Execute(request interface{}, response interface{}) (int, error) {

	fsReq := request.(*api.FeatureStoreRequest)
	metadata, err := h.fvMetaCache.Get(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err.GetStatus(), err.Error()
	}
	var readParams = getBatchPkReadParams(metadata, fsReq.Entries)
	ronDbErr := h.dbBatchReader.Validate(readParams)
	if ronDbErr != nil {
		var fsError = translateRonDbError(http.StatusBadRequest, ronDbErr)
		return fsError.GetStatus(), fsError.Error()
	}
	var dbResponseIntf = getPkReadResponseJSON()
	code, ronDbErr := h.dbBatchReader.Execute(readParams, *dbResponseIntf)
	if ronDbErr != nil {
		var fsError = translateRonDbError(code, ronDbErr)
		return fsError.GetStatus(), fsError.Error()
	}
	fsResp := response.(*api.FeatureStoreResponse)
	features, status := getFeatureValues(dbResponseIntf, fsReq.Entries, metadata)
	fsResp.Status = status
	fillPassedFeatures(features, fsReq.PassedFeatures, &metadata.PrefixFeaturesLookup, &metadata.FeatureIndexLookup)
	fsResp.Features = *features
	if fsReq.MetadataRequest != nil {
		featureMetadatas := make([]*api.FeatureMeatadata, metadata.NumOfFeatures)
		for featureKey, metadata := range metadata.PrefixFeaturesLookup {
			featureMetadata := api.FeatureMeatadata{}
			if fsReq.MetadataRequest.FeatureName {
				var fk = featureKey
				featureMetadata.Name = &fk
			}
			if fsReq.MetadataRequest.FeatureType {
				var ft = metadata.Type
				featureMetadata.Type = &ft
			}
			featureMetadatas[metadata.Index] = &featureMetadata
		}
		fsResp.Metadata = featureMetadatas
	}
	return http.StatusOK, nil
}

func translateRonDbError(code int, err error) *feature_store.RestErrorCode {
	var fsError *feature_store.RestErrorCode
	if strings.Contains(err.Error(), "Wrong data type.") {
		regex := regexp.MustCompile(`Expecting (\w+)\. Column: (\w+)`)
		match := regex.FindStringSubmatch(err.Error())
		if match != nil {
			dataType := match[1]
			columnName := match[2]
			fsError = feature_store.WRONG_DATA_TYPE.NewMessage(
				fmt.Sprintf("Primary key '%s' should be in '%s' format.", columnName, dataType),
			)
		} else {
			fsError = feature_store.WRONG_DATA_TYPE
		}

	} else if strings.Contains(err.Error(), "Wrong number of primary-key columns.") ||
		strings.Contains(err.Error(), "Wrong primay-key column.") {
		fsError = feature_store.INCORRECT_PRIMARY_KEY.NewMessage(err.Error())
	} else {
		fsError = feature_store.READ_FROM_DB_FAIL
	}
	return fsError
}

func getFeatureValues(batchResponse *api.BatchOpResponse, entries *map[string]*json.RawMessage, featureView *feature_store.FeatureViewMetadata) (*[]interface{}, api.FeatureStatus) {
	jsonResponse := (*batchResponse).String()
	if log.IsDebug() {
		log.Debugf("Received response from rondb: %s", jsonResponse)
	}
	rondbResp := api.BatchResponseJSON{}
	json.Unmarshal([]byte(jsonResponse), &rondbResp)
	featureValues := make([]interface{}, featureView.NumOfFeatures)
	var status = api.FEATURE_STATUS_COMPLETE
	for _, response := range *rondbResp.Result {
		if *response.Code == 404 {
			status = api.FEATURE_STATUS_MISSING
		} else if *response.Code != 200 {
			status = api.FEATURE_STATUS_ERROR
		}
		for featureName, value := range *response.Body.Data {
			featureIndexKey := feature_store.GetFeatureIndexKeyByFgIndexKey(*response.Body.OperationID, featureName)
			if index, ok := (featureView.FeatureIndexLookup)[featureIndexKey]; ok {
				featureValues[index] = value
			} else {
				panic(fmt.Sprintf("Index cannot be found by the key '%s'", featureIndexKey))
			}
		}
	}
	// Fill in primary key value from request into the vector
	for featureName, value := range *entries {
		indexKey := feature_store.GetFeatureIndexKeyByFeature((featureView.PrefixFeaturesLookup)[featureName])
		if index, ok := (featureView.FeatureIndexLookup)[indexKey]; ok {
			featureValues[index] = value
		}
	}
	return &featureValues, status
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
		var opId = feature_store.GetFeatureGroupKeyByTDFeature(fgFeature)
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
		var lookupKey = feature_store.GetFeatureIndexKeyByFeature(feature)
		(*features)[(*indexLookup)[lookupKey]] = passFeature
	}

}
