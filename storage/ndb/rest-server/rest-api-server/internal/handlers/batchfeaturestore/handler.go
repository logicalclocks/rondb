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
	"strconv"
	"strings"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/handlers/batchpkread"
	fshandler "hopsworks.ai/rdrs/internal/handlers/feature_store"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

const (
	SEQUENCE_SEPARATOR = "#"
)

type Handler struct {
	fvMetaCache    *feature_store.FeatureViewMetaDataCache
	apiKeyCache    apikey.Cache
	dbBatchHandler batchpkread.Handler
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
		return err.GetError()
	}
	if len(*fsReq.Entries) == 0 {
		return feature_store.NO_PRIMARY_KEY_GIVEN.GetError()
	}
	if fsReq.PassedFeatures != nil && len(*fsReq.PassedFeatures) != 0 && len(*fsReq.Entries) != len(*fsReq.PassedFeatures) {
		return feature_store.INCORRECT_PASSED_FEATURE.NewMessage("Length of passed feature does not equal to that of the entries provided in the request.").GetError()
	}
	return nil
}

func checkFeatureStatus(fsReq *api.BatchFeatureStoreRequest, metadata *feature_store.FeatureViewMetadata, status *[]api.FeatureStatus) int {
	var cnt = make(map[int]bool)
	for i, entry := range *fsReq.Entries {
		if fshandler.ValidatePrimaryKey(entry, &metadata.PrefixPrimaryKeyMap) != nil {
			(*status)[i] = api.FEATURE_STATUS_ERROR
			cnt[i] = true
		}
	}
	if fsReq.PassedFeatures != nil {
		for i, passedFeature := range *fsReq.PassedFeatures {
			if fshandler.ValidatePassedFeatures(passedFeature, &metadata.PrefixFeaturesLookup) != nil {
				(*status)[i] = api.FEATURE_STATUS_ERROR
				cnt[i] = true
			}
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
		return err.GetError()
	}
	valErr := h.apiKeyCache.ValidateAPIKey(apiKey, metadata.FeatureStoreNames...)
	if valErr != nil {
		return feature_store.FEATURE_STORE_NOT_SHARED.GetError()
	}
	return nil
}

func (h *Handler) Execute(request interface{}, response interface{}) (int, error) {
	fsReq := request.(*api.BatchFeatureStoreRequest)
	metadata, err := h.fvMetaCache.Get(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err.GetStatus(), err.GetError()
	}
	var featureStatus = make([]api.FeatureStatus, len(*fsReq.Entries))
	var numPassed = checkFeatureStatus(fsReq, metadata, &featureStatus)
	var readParams = getBatchPkReadParamsMutipleEntries(metadata, fsReq.Entries, &featureStatus)
	fsResp := response.(*api.BatchFeatureStoreResponse)
	var features *[][]interface{}
	if len(*readParams) > 0 {
		ronDbErr := h.dbBatchHandler.Validate(readParams)
		if ronDbErr != nil {
			if log.IsDebug() {
				log.Debugf("RonDB validation failed: %s", ronDbErr.Error())
			}
			var fsError = fshandler.TranslateRonDbError(http.StatusBadRequest, ronDbErr.Error())
			return fsError.GetStatus(), fsError.GetError()
		}
		var dbResponseIntf = getPkReadResponseJSON(numPassed, *metadata)
		code, ronDbErr := h.dbBatchHandler.Execute(readParams, *dbResponseIntf)
		if log.IsDebug() {
			log.Debugf("RonDB response: code: %d, error: %s, body: %s", code, ronDbErr, (*dbResponseIntf).String())
		}
		if ronDbErr != nil {
			var fsError = fshandler.TranslateRonDbError(code, ronDbErr.Error())
			return fsError.GetStatus(), fsError.GetError()
		}
		features, err = getFeatureValuesMultipleEntries(dbResponseIntf, fsReq.Entries, metadata, &featureStatus)
		if err != nil {
			return err.GetStatus(), err.GetError()
		}
	} else {
		var emptyFeatures = make([][]interface{}, len(*fsReq.Entries))
		for i := range emptyFeatures {
			emptyFeatures[i] = nil
		}
		features = &emptyFeatures
	}
	fsResp.Status = featureStatus
	fillPassedFeaturesMultipleEntries(features, fsReq.PassedFeatures, &metadata.PrefixFeaturesLookup, &metadata.FeatureIndexLookup, &featureStatus)
	fsResp.Features = *features
	if fsReq.MetadataRequest != nil {
		fsResp.Metadata = *fshandler.GetFeatureMetadata(metadata, fsReq.MetadataRequest)
	}
	return http.StatusOK, nil
}

func getFeatureValuesMultipleEntries(batchResponse *api.BatchOpResponse, entries *[]*map[string]*json.RawMessage, featureView *feature_store.FeatureViewMetadata, batchStatus *[]api.FeatureStatus) (*[][]interface{}, *feature_store.RestErrorCode) {
	rondbResp := (*batchResponse).(*api.BatchResponseJSON)
	ronDbBatchResult := make([][]*api.PKReadResponseWithCodeJSON, len(*batchStatus))
	batchResult := make([][]interface{}, len(*batchStatus))
	for _, response := range *rondbResp.Result {
		splitOperationId := strings.Split(*response.Body.OperationID, SEQUENCE_SEPARATOR)
		seqNum, err := strconv.Atoi(splitOperationId[0])
		if err != nil {
			return nil, feature_store.READ_FROM_DB_FAIL
		}
		*response.Body.OperationID = splitOperationId[1]
		ronDbBatchResult[seqNum] = append(ronDbBatchResult[seqNum], response)
	}
	for i, ronDbResult := range ronDbBatchResult {
		if len(ronDbResult) != 0 {
			result, status, _ := fshandler.GetFeatureValues(&ronDbResult, (*entries)[i], featureView)
			batchResult[i] = *result
			(*batchStatus)[i] = status
		}
	}
	return &batchResult, nil
}

func getBatchPkReadParamsMutipleEntries(metadata *feature_store.FeatureViewMetadata, entries *[]*map[string]*json.RawMessage, status *[]api.FeatureStatus) *[]*api.PKReadParams {
	var batchReadParams = make([]*api.PKReadParams, 0, metadata.NumOfFeatures*len(*entries))
	for i, entry := range *entries {
		if (*status)[i] != api.FEATURE_STATUS_ERROR {
			for _, param := range *fshandler.GetBatchPkReadParams(metadata, entry) {
				var oid = fmt.Sprintf("%d%s%s", i, SEQUENCE_SEPARATOR, *(*param).OperationID)
				(*param).OperationID = &oid
				batchReadParams = append(batchReadParams, param)
			}
		}
	}
	return &batchReadParams
}

func getPkReadResponseJSON(numEntries int, metadata feature_store.FeatureViewMetadata) *api.BatchOpResponse {
	response := (api.BatchOpResponse)(&api.BatchResponseJSON{})
	response.Init(len(metadata.FeatureGroupFeatures) * numEntries)
	return &response
}

func fillPassedFeaturesMultipleEntries(features *[][]interface{}, passedFeatures *[]*map[string]*json.RawMessage, featureMetadata *map[string]*feature_store.FeatureMetadata, indexLookup *map[string]int, status *[]api.FeatureStatus) {
	if passedFeatures != nil && len(*passedFeatures) != 0 {
		for i, feature := range *features {
			if (*status)[i] != api.FEATURE_STATUS_ERROR {
				fshandler.FillPassedFeatures(&feature, (*passedFeatures)[i], featureMetadata, indexLookup)
			}
		}
	}
}
