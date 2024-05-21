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
	"strconv"
	"strings"

	"hopsworks.ai/rdrs/internal/common"
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
		return err.GetError()
	}
	if log.IsDebug() {
		log.Debugf("Feature store request is %s", fsReq.String())
	}
	err1 := ValidatePrimaryKey(fsReq.Entries, &metadata.ValidPrimaryKeys)
	if err1 != nil {
		return err1.GetError()
	}
	if fsReq.GetOptions().ValidatePassedFeatures {
		err2 := ValidatePassedFeatures(fsReq.PassedFeatures, &metadata.PrefixFeaturesLookup)
		if err2 != nil {
			return err2.GetError()
		}
	}
	return nil

}

func ValidatePrimaryKey(entries *map[string]*json.RawMessage, validPrimaryKeys *map[string]bool) *feature_store.RestErrorCode {
	// Data type check of primary key will be delegated to rondb.
	if len(*entries) == 0 {
		return feature_store.INCORRECT_PRIMARY_KEY.NewMessage("No entries found")
	}

	for featureName := range *entries {
		_, ok := (*validPrimaryKeys)[featureName]
		if !ok {
			return feature_store.INCORRECT_PRIMARY_KEY.NewMessage(fmt.Sprintf("Provided primary key `%s` does not belong to the set of primary key.", featureName))
		}
	}
	return nil
}

func ValidatePassedFeatures(passedFeatures *map[string]*json.RawMessage, features *map[string][]*feature_store.FeatureMetadata) *feature_store.RestErrorCode {
	if passedFeatures == nil {
		return nil
	}
	for featureName, value := range *passedFeatures {
		features, ok := (*features)[featureName]
		if !ok {
			return feature_store.FEATURE_NOT_EXIST.NewMessage(
				fmt.Sprintf("Feature `%s` does not exist in the feature view or it is a label which cannot be a passed feature.", featureName))
		}
		for _, feature := range features {
			err := ValidateFeatureType(value, feature.Type)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ValidateFeatureType(feature *json.RawMessage, featureType string) *feature_store.RestErrorCode {
	var got, err = getJsonType(feature)

	if err != nil {
		return feature_store.INCORRECT_FEATURE_VALUE.NewMessage(fmt.Sprintf("Provided value %v is not in correct JSON format. %s", feature, err))
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
		return JSON_STRING
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
		return err.GetError()
	}
	// Validate access right to ALL feature stores including shared feature
	valErr := h.apiKeyCache.ValidateAPIKey(apiKey, metadata.FeatureStoreNames...)
	if valErr != nil {
		return feature_store.FEATURE_STORE_NOT_SHARED.GetError()
	}
	return nil
}

func (h *Handler) Execute(request interface{}, response interface{}) (int, error) {

	fsReq := request.(*api.FeatureStoreRequest)
	metadata, err := h.fvMetaCache.Get(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err.GetStatus(), err.GetError()
	}
	var readParams = GetBatchPkReadParams(metadata, fsReq.Entries)
	ronDbErr := h.dbBatchReader.Validate(readParams)
	if ronDbErr != nil {
		var fsError = TranslateRonDbError(http.StatusBadRequest, ronDbErr.Error())
		return fsError.GetStatus(), fsError.GetError()
	}
	var dbResponseIntf = getPkReadResponseJSON(*metadata)
	code, ronDbErr := h.dbBatchReader.Execute(readParams, *dbResponseIntf)
	if ronDbErr != nil {
		var fsError = TranslateRonDbError(code, ronDbErr.Error())
		return fsError.GetStatus(), fsError.GetError()
	}
	if log.IsDebug() {
		jsonResponse := (*dbResponseIntf).String()
		log.Debugf("Rondb response: code: %d, error: %s, body: %s", code, ronDbErr, jsonResponse)
	}
	rondbResp := (*dbResponseIntf).(*api.BatchResponseJSON)
	fsError := checkRondbResponse(rondbResp)
	if fsError != nil {
		return fsError.GetStatus(), fsError.GetError()
	}
	features, status, detailedStatus, fsError := GetFeatureValues(rondbResp.Result, fsReq.Entries, metadata, *fsReq.OptionsRequest.IncludeDetailedStatus)
	if fsError != nil {
		return fsError.GetStatus(), fsError.GetError()
	}
	if log.IsDebug() {
		log.Debugf("Detailed Status : %s", detailedStatus)
	}
	fsResp := response.(*api.FeatureStoreResponse)
	fsResp.Status = status
	FillPassedFeatures(features, fsReq.PassedFeatures, &metadata.PrefixFeaturesLookup, &metadata.FeatureIndexLookup)
	fsResp.Features = *features
	if fsReq.MetadataRequest != nil {
		fsResp.Metadata = *GetFeatureMetadata(metadata, fsReq.MetadataRequest)
	}
	if *fsReq.OptionsRequest.IncludeDetailedStatus {
		fsResp.DetailedStatus = detailedStatus
	}
	return http.StatusOK, nil
}

func checkRondbResponse(rondbResp *api.BatchResponseJSON) *feature_store.RestErrorCode {
	for _, result := range *rondbResp.Result {
		if *result.Code != http.StatusOK && *result.Code != http.StatusNotFound {
			return TranslateRonDbError(int(*result.Code), *result.Message)
		}
	}
	return nil
}

func GetFeatureMetadata(metadata *feature_store.FeatureViewMetadata, metaRequest *api.MetadataRequest) *[]*api.FeatureMetadata {
	featureMetadataArray := make([]*api.FeatureMetadata, metadata.NumOfFeatures)
	for featureKey, prefixFeaturesLookup := range metadata.PrefixFeaturesLookup {
		for _, featureMetadata := range prefixFeaturesLookup {
			if featureIndex, ok := metadata.FeatureIndexLookup[feature_store.GetFeatureIndexKeyByFeature(featureMetadata)]; ok {
				featureMetadataResp := api.FeatureMetadata{}
				if metaRequest.FeatureName {
					var fk = featureKey
					featureMetadataResp.Name = &fk
				}
				if metaRequest.FeatureType {
					var ft = featureMetadata.Type
					featureMetadataResp.Type = &ft
				}
				featureMetadataArray[featureIndex] = &featureMetadataResp
			}
		}
	}
	return &featureMetadataArray
}

func TranslateRonDbError(code int, err string) *feature_store.RestErrorCode {
	var fsError *feature_store.RestErrorCode
	if strings.Contains(err, common.ERROR_015()) { // Wrong data type.
		regex := regexp.MustCompile(`Expecting (\w+)\. Column: (\w+)`)
		match := regex.FindStringSubmatch(err)
		if match != nil {
			dataType := match[1]
			columnName := match[2]
			fsError = feature_store.WRONG_DATA_TYPE.NewMessage(
				fmt.Sprintf("Primary key '%s' should be in '%s' format.", columnName, dataType),
			)
		} else {
			fsError = feature_store.WRONG_DATA_TYPE
		}

	} else if strings.Contains(err, common.ERROR_014()) || // "Wrong primay-key column."
		strings.Contains(err, common.ERROR_012()) { // "Column does not exist."
		fsError = feature_store.INCORRECT_PRIMARY_KEY.NewMessage(err)
	} else if strings.Contains(err, common.ERROR_013()) { // "Wrong number of primary-key columns."
		fsError = nil // Missing entry can happen and users can fill up missing features by passed featues
	} else {
		if code == http.StatusBadRequest {
			fsError = feature_store.READ_FROM_DB_FAIL_BAD_INPUT.NewMessage(err)
		} else {
			fsError = feature_store.READ_FROM_DB_FAIL.NewMessage(err)
		}
	}
	return fsError
}

func GetFeatureValues(ronDbResult *[]*api.PKReadResponseWithCodeJSON, entries *map[string]*json.RawMessage, featureView *feature_store.FeatureViewMetadata, includeDetailedStatus bool) (*[]interface{}, api.FeatureStatus, []*api.DetailedStatus, *feature_store.RestErrorCode) {
	featureValues := make([]interface{}, featureView.NumOfFeatures)
	var status = api.FEATURE_STATUS_COMPLETE
	var arrDetailedStatus = make([]*api.DetailedStatus, 0, len(*ronDbResult))
	var err *feature_store.RestErrorCode
	for _, response := range *ronDbResult {
		if includeDetailedStatus {
			fgInt, err := strconv.Atoi(strings.Split(*response.Body.OperationID, "|")[1])
			if err != nil {
				log.Errorf("Failed to convert feature group id to int: %s", *response.Body.OperationID)
				fgInt = -1
			}
			arrDetailedStatus = append(arrDetailedStatus, &api.DetailedStatus{
				FeatureGroupId: fgInt,
				HttpStatus:     *response.Code,
				Message:        response.Message,
				OperationId:    response.Body.OperationID,
			})
		}
		if *response.Code == http.StatusNotFound {
			status = api.FEATURE_STATUS_MISSING
		} else if *response.Code == http.StatusBadRequest {
			if strings.Contains(*response.Message, common.ERROR_013()) { // "Wrong number of primary-key columns."
				status = api.FEATURE_STATUS_MISSING // Missing entry can happen and users can fill up missing features by passed featues
			} else {
				status = api.FEATURE_STATUS_ERROR
			}
		} else if *response.Code != http.StatusOK {
			status = api.FEATURE_STATUS_ERROR
		}
		for featureName, value := range *response.Body.Data {
			featureIndexKey := feature_store.GetFeatureIndexKeyByFgIndexKey(*response.Body.OperationID, featureName)
			// When only primary key is selected, Rondb will return all columns, so not all value from response are needed.
			if index, ok := (featureView.FeatureIndexLookup)[featureIndexKey]; ok {
				if decoder, ok := (featureView.ComplexFeatures)[featureIndexKey]; ok {
					var deser, err1 = DeserialiseComplexFeature(value, decoder)
					if err1 != nil {
						status = api.FEATURE_STATUS_ERROR
						err = feature_store.DESERIALISE_FEATURE_FAIL.NewMessage(fmt.Sprintf("Feature name: %s; %s", featureName, err1.Error()))
					} else {
						featureValues[index] = deser
					}
				} else {
					featureValues[index] = value
				}
			}
		}
	}
	// Fill in primary key value from request into the vector
	// If multiple matched entries are found, the priority of the entry follows the order in `GetBatchPkReadParams`
	// i.e required entry > entry with prefix > entry without prefix
	// Reason why it has to loop all entries for each `*JoinKeyMap` is to make sure the value are filled in according to the priority.
	// Otherwise the lower priority one can overwrite the previous assigned entry if the later entry exists only in the lower priority map.
	// Check `Test_GetFeatureVector_TestCorrectPkValue*` for more detail.
	for featureName, value := range *entries {
		// Get all join key linked to the provided feature name without prefix
		if joinKeysWithoutPrefix, ok := featureView.JoinKeyMap[featureName]; ok {
			if log.IsDebug() {
				log.Debugf("Filled primary key by JoinKeyMap")
			}
			FillPrimaryKey(featureView, &featureValues, joinKeysWithoutPrefix, value)
		}
	}
	for featureName, value := range *entries {
		// Get all join key linked to the provided feature name with prefix.
		// Entry with prefix is prioritised and the value overwrites the previous assignment if available.
		if joinKeysWithPrefix, ok := featureView.PrefixJoinKeyMap[featureName]; ok {
			if log.IsDebug() {
				log.Debugf("Filled primary key by PrefixJoinKeyMap")
			}
			FillPrimaryKey(featureView, &featureValues, joinKeysWithPrefix, value)
		}
	}
	for featureName, value := range *entries {
		// Get all join key linked to the provided feature name with prefix.
		// Entry in RequiredJoinKeyMap is prioritised and the value overwrites the previous assignment if available.
		if joinKeysRequired, ok := featureView.RequiredJoinKeyMap[featureName]; ok {
			if log.IsDebug() {
				log.Debugf("Filled primary key by RequiredJoinKeyMap")
			}
			FillPrimaryKey(featureView, &featureValues, joinKeysRequired, value)
		}
	}
	return &featureValues, status, arrDetailedStatus, err
}

func FillPrimaryKey(featureView *feature_store.FeatureViewMetadata, featureValues *[]interface{}, joinKeys []string, value *json.RawMessage) {
	var indexKey string
	// Get all join key linked to the provided feature name
	if log.IsDebug() {
		log.Debugf("Join keys are: %s", strings.Join(joinKeys, ", "))
	}
	for _, joinKey := range joinKeys {
		// Check if the join key are valid feature
		if prefixFeaturesLookups, ok := featureView.PrefixFeaturesLookup[joinKey]; ok {
			for _, prefixFeaturesLookup := range prefixFeaturesLookups {
				indexKey = feature_store.GetFeatureIndexKeyByFeature(prefixFeaturesLookup)
				// Get the index of the feature
				if index, ok := (featureView.FeatureIndexLookup)[indexKey]; ok {
					(*featureValues)[index] = value
					if log.IsDebug() {
						log.Debugf("Filled primary key %s with value %s", joinKey, *value)
					}
				}
			}
		}
	}
}

func GetBatchPkReadParams(metadata *feature_store.FeatureViewMetadata, entries *map[string]*json.RawMessage) *[]*api.PKReadParams {

	var batchReadParams = make([]*api.PKReadParams, 0, len(metadata.FeatureGroupFeatures))
	for _, fgFeature := range metadata.FeatureGroupFeatures {
		testDb := fgFeature.FeatureStoreName
		testTable := fmt.Sprintf("%s_%d", fgFeature.FeatureGroupName, fgFeature.FeatureGroupVersion)
		var filters = make([]api.Filter, 0, len(fgFeature.Features))
		var columns = make([]api.ReadColumn, 0, len(fgFeature.Features))
		for _, feature := range fgFeature.Features {
			if _, ok := metadata.PrimaryKeyMap[feature_store.GetServingKey(feature.JoinIndex, feature.Name)]; !ok {
				var colName = feature.Name
				var colType = api.DRT_DEFAULT
				readCol := api.ReadColumn{Column: &colName, DataReturnType: &colType}
				columns = append(columns, readCol)
				if log.IsDebug() {
					log.Debugf("Add to column: %s", feature.Name)
				}
			}
		}
		for _, servingKey := range fgFeature.PrimaryKeyMap {
			var pkCol = servingKey.FeatureName
			if (*entries)[servingKey.RequiredEntry] != nil {
				// Fill in value of required entry as original entry may not be required.
				var filter = api.Filter{Column: &pkCol, Value: (*entries)[servingKey.RequiredEntry]}
				filters = append(filters, filter)
				if log.IsDebug() {
					var entryValue interface{}
					err := json.Unmarshal(*(*entries)[servingKey.RequiredEntry], &entryValue)
					if err == nil {
						log.Debugf("Add to filter: %s=%v", pkCol, entryValue)
					} else {
						log.Debugf("Add to filter: %s", pkCol)
					}
				}
			} else if (*entries)[servingKey.Prefix+servingKey.FeatureName] != nil {
				// Also Fallback and use feature name with prefix.
				var filter = api.Filter{Column: &pkCol, Value: (*entries)[servingKey.Prefix+servingKey.FeatureName]}
				filters = append(filters, filter)
				if log.IsDebug() {
					var entryValue interface{}
					err := json.Unmarshal(*(*entries)[servingKey.Prefix+servingKey.FeatureName], &entryValue)
					if err == nil {
						log.Debugf("Add to filter: %s=%v", pkCol, entryValue)
					} else {
						log.Debugf("Add to filter: %s", pkCol)
					}
				}
			} else if (*entries)[servingKey.FeatureName] != nil {
				// Fallback and use the raw feature name so as to be consistent with python client.
				// Also add feature name with prefix.
				var filter = api.Filter{Column: &pkCol, Value: (*entries)[servingKey.FeatureName]}
				filters = append(filters, filter)

				if log.IsDebug() {
					var entryValue interface{}
					err := json.Unmarshal(*(*entries)[servingKey.FeatureName], &entryValue)
					if err == nil {
						log.Debugf("Add to filter: %s=%v", pkCol, entryValue)
					} else {
						log.Debugf("Add to filter: %s", pkCol)
					}
				}
			}
		}

		var opId = feature_store.GetFeatureGroupKeyByTDFeature(fgFeature)
		param := api.PKReadParams{DB: &testDb, Table: &testTable, Filters: &filters, ReadColumns: &columns, OperationID: &opId}
		batchReadParams = append(batchReadParams, &param)
	}
	return &batchReadParams
}

func getPkReadResponseJSON(metadata feature_store.FeatureViewMetadata) *api.BatchOpResponse {
	response := (api.BatchOpResponse)(&api.BatchResponseJSON{})
	response.Init(len(metadata.FeatureGroupFeatures))
	return &response
}

func FillPassedFeatures(features *[]interface{}, passedFeatures *map[string]*json.RawMessage, featureMetadataMap *map[string][]*feature_store.FeatureMetadata, indexLookup *map[string]int) {
	if passedFeatures != nil {
		for featureName, passFeature := range *passedFeatures {

			var featureMetadatas = (*featureMetadataMap)[featureName]
			for _, featureMetadata := range featureMetadatas {
				if featureMetadata != nil {
					var lookupKey = feature_store.GetFeatureIndexKeyByFeature(featureMetadata)
					if index, ok := (*indexLookup)[lookupKey]; ok {
						(*features)[index] = passFeature
					}
				}
			}
		}
	}
}
