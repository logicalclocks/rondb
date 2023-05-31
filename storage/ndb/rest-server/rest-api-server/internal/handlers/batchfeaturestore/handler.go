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
	"regexp"
	"strconv"
	"strings"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/handlers/batchpkread"
	fshanlder "hopsworks.ai/rdrs/internal/handlers/feature_store"
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

const (
	SEQUENCE_SEPARATOR = "#"
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
	// Complete validation is delegated to Execute()
	fsReq := request.(*api.BatchFeatureStoreRequest)
	if log.IsDebug() {
		log.Debugf("Feature store request is %s", fsReq.String())
	}
	// check if requested fv and fs exist
	_, err := h.fvMetaCache.Get(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err.Error()
	}
	if len(*fsReq.Entries) == 0 {
		return feature_store.NO_PRIMARY_KEY_GIVEN.Error()
	}
	if len(*fsReq.PassedFeatures) != 0 && len(*fsReq.Entries) != len(*fsReq.PassedFeatures) {
		return feature_store.INCORRECT_PASSED_FEATURE.Error()
	}
	return nil
}

func checkStatus(fsReq *api.BatchFeatureStoreRequest, metadata *feature_store.FeatureViewMetadata, status *[]api.FeatureStatus) int {
	var cnt = make(map[int]bool)
	for i, entry := range *fsReq.Entries {
		if fshanlder.ValidatePrimaryKey(entry, &metadata.PrefixFeaturesLookup) != nil {
			(*status)[i] = api.FEATURE_STATUS_ERROR
			cnt[i] = true
		}
	}
	for i, passedFeature := range *fsReq.PassedFeatures {
		if fshanlder.ValidatePassedFeatures(passedFeature, &metadata.PrefixFeaturesLookup) != nil {
			(*status)[i] = api.FEATURE_STATUS_ERROR
			cnt[i] = true
		}
	}
	return len(*status) - len(cnt)
}

func (h *Handler) Authenticate(apiKey *string, request interface{}) error {
	conf := config.GetAll()
	if !conf.Security.APIKey.UseHopsworksAPIKeys {
		return nil
	}
	fsReq := request.(*api.BatchFeatureStoreRequest)
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
	fsReq := request.(*api.BatchFeatureStoreRequest)
	metadata, err := h.fvMetaCache.Get(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err.GetStatus(), err.Error()
	}
	var featureStatus = make([]api.FeatureStatus, len(*fsReq.Entries))
	var numPassed = checkStatus(fsReq, metadata, &featureStatus)
	var readParams = getBatchPkReadParamsMutipleEntries(metadata, fsReq.Entries, &featureStatus)
	ronDbErr := h.dbBatchReader.Validate(readParams)
	if ronDbErr != nil {
		var fsError = translateRonDbError(http.StatusBadRequest, ronDbErr)
		return fsError.GetStatus(), fsError.Error()
	}
	var dbResponseIntf = getPkReadResponseJSON(numPassed, *metadata)
	code, ronDbErr := h.dbBatchReader.Execute(readParams, *dbResponseIntf)
	if log.IsDebug() {
		log.Debugf("Rondb response: %s", (*dbResponseIntf).String())
	}
	// FIXME: depends on rondb response
	if ronDbErr != nil {
		var fsError = translateRonDbError(code, ronDbErr)
		return fsError.GetStatus(), fsError.Error()
	}
	fsResp := response.(*api.BatchFeatureStoreResponse)
	features := getFeatureValuesMultipleEntries(dbResponseIntf, fsReq.Entries, metadata, &featureStatus)
	fsResp.Status = featureStatus
	fillPassedFeaturesMultipleEntries(features, fsReq.PassedFeatures, &metadata.PrefixFeaturesLookup, &metadata.FeatureIndexLookup, &featureStatus)
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

func getFeatureValuesMultipleEntries(batchResponse *api.BatchOpResponse, entries *[]*map[string]*json.RawMessage, featureView *feature_store.FeatureViewMetadata, batchStatus *[]api.FeatureStatus) *[][]interface{} {
	jsonResponse := (*batchResponse).String()
	if log.IsDebug() {
		log.Debugf("Received response from rondb: %s", jsonResponse)
	}
	rondbResp := api.BatchResponseJSON{}
	json.Unmarshal([]byte(jsonResponse), &rondbResp)
	ronDbBatchResult := make([][]*api.PKReadResponseWithCodeJSON, len(*batchStatus))
	batchResult := make([][]interface{}, len(*batchStatus))
	for _, response := range *rondbResp.Result {
		splitOperationId := strings.Split(*response.Body.OperationID, SEQUENCE_SEPARATOR)
		seqNum, _ := strconv.Atoi(splitOperationId[0])
		*response.Body.OperationID = splitOperationId[1]
		ronDbBatchResult[seqNum] = append(ronDbBatchResult[seqNum], response)
	}
	for i, ronDbResult := range ronDbBatchResult {
		if len(ronDbResult) != 0 {
			result, status := fshanlder.GetFeatureValues(&ronDbResult, (*entries)[i], featureView)
			batchResult[i] = *result
			(*batchStatus)[i] = status
		}
	}
	return &batchResult
}

func getBatchPkReadParamsMutipleEntries(metadata *feature_store.FeatureViewMetadata, entries *[]*map[string]*json.RawMessage, status *[]api.FeatureStatus) *[]*api.PKReadParams {
	var batchReadParams = make([]*api.PKReadParams, 0, metadata.NumOfFeatures*len(*entries))
	for i, entry := range *entries {
		if (*status)[i] != api.FEATURE_STATUS_ERROR {
			batchReadParams = append(batchReadParams, *getBatchPkReadParams(metadata, entry, i)...)
		}
	}
	return &batchReadParams
}

func getBatchPkReadParams(metadata *feature_store.FeatureViewMetadata, entries *map[string]*json.RawMessage, seqNum int) *[]*api.PKReadParams {

	var batchReadParams = make([]*api.PKReadParams, 0, metadata.NumOfFeatures)
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
		var opId = fmt.Sprintf("%d#%s", seqNum, feature_store.GetFeatureGroupKeyByTDFeature(fgFeature))
		param := api.PKReadParams{DB: &testDb, Table: &testTable, Filters: &filters, ReadColumns: &columns, OperationID: &opId}
		if log.IsDebug() {
			strBytes, err := json.MarshalIndent(param, "", "\t")
			if err != nil {
				log.Debugf("Failed to marshal PKReadParams. Error: %v", err)
			} else {
				log.Debugf(string(strBytes))
			}

		}
		batchReadParams = append(batchReadParams, &param)
	}
	return &batchReadParams
}

func getPkReadResponseJSON(numEntries int, metadata feature_store.FeatureViewMetadata) *api.BatchOpResponse {
	response := (api.BatchOpResponse)(&api.BatchResponseJSON{})
	response.Init(len(metadata.FeatureGroupFeatures) * numEntries)
	log.Debugf("total number of entries: %d", len(metadata.FeatureGroupFeatures)*numEntries)
	return &response
}

func fillPassedFeaturesMultipleEntries(features *[][]interface{}, passedFeatures *[]*map[string]*json.RawMessage, featureMetadata *map[string]*feature_store.FeatureMetadata, indexLookup *map[string]int, status *[]api.FeatureStatus) {
	if len(*passedFeatures) != 0 {
		for i, feature := range *features {
			// TODO: validate length of pass features
			if (*status)[i] != api.FEATURE_STATUS_ERROR {
				fillPassedFeatures(&feature, (*passedFeatures)[i], featureMetadata, indexLookup)
			}
		}
	}
}

func fillPassedFeatures(features *[]interface{}, passedFeatures *map[string]*json.RawMessage, featureMetadata *map[string]*feature_store.FeatureMetadata, indexLookup *map[string]int) {
	for featureName, passFeature := range *passedFeatures {
		var feature = (*featureMetadata)[featureName]
		var lookupKey = feature_store.GetFeatureIndexKeyByFeature(feature)
		(*features)[(*indexLookup)[lookupKey]] = passFeature
	}

}
