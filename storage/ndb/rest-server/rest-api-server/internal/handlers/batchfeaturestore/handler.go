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
	"net/http"
	"sync"

	"hopsworks.ai/rdrs/internal/config"
	fs "hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/handlers/feature_store"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct {
	apiKeyCache apikey.Cache
	fvMetaCache *fs.FeatureViewMetaDataCache
	fshandler   *feature_store.Handler
}

type FeatureStoreResponseWithOrder struct {
	FsResponse *api.FeatureStoreResponse
	Order      int
	Status     int
	Error      error
}

type FeatureStoreRequestWithOrder struct {
	FsResponse *api.FeatureStoreRequest
	Order      int
}

type FeatureStoreBatchResponseWithStatus struct {
	FsResponse *api.BatchFeatureStoreResponse
	Status     int
	Error      error
}

func New(fvMeta *fs.FeatureViewMetaDataCache, apiKeyCache apikey.Cache, fshandler *feature_store.Handler) Handler {
	return Handler{apiKeyCache, fvMeta, fshandler}
}

func (h *Handler) Validate(request interface{}) error {
	// Complete validation is delegated to feature store single read handler
	fsReq := request.(*api.BatchFeatureStoreRequest)
	// check if requested fv and fs exist
	_, err := h.fvMetaCache.Get(
		*fsReq.FeatureStoreName, *fsReq.FeatureViewName, *fsReq.FeatureViewVersion)
	if err != nil {
		return err.Error()
	}
	if len(*fsReq.Entries) == 0 {
		return fs.NO_PRIMARY_KEY_GIVEN.Error()
	}
	return nil
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
	return h.apiKeyCache.ValidateAPIKey(apiKey, metadata.FeatureStoreNames...)
}

func (h *Handler) Execute(request interface{}, response interface{}) (int, error) {
	// FIXME: make this configurable
	numThread := 3
	batchFsReq := request.(*api.BatchFeatureStoreRequest)
	fvReqs := make(chan *FeatureStoreRequestWithOrder)
	fvResps := make(chan *FeatureStoreResponseWithOrder)

	var wg sync.WaitGroup
	var wgResp sync.WaitGroup

	wg.Add(numThread)
	wgResp.Add(1)

	for i := 0; i < numThread; i++ {
		go processFsRequest(*h.fshandler, fvReqs, fvResps, &wg)
	}
	var batchResponse = response.(*api.BatchFeatureStoreResponse)
	var batchResponseWithStatus = FeatureStoreBatchResponseWithStatus{}
	batchResponseWithStatus.FsResponse = batchResponse
	go processFsResp(len(*batchFsReq.Entries), &batchResponseWithStatus, fvResps, &wgResp)

	for i, entry := range *batchFsReq.Entries {
		var pf map[string]*json.RawMessage
		if len(*batchFsReq.PassedFeatures) > 0 {
			pf = *(*batchFsReq.PassedFeatures)[i]
		}
		var fsReq = api.FeatureStoreRequest{}
		fsReq.FeatureStoreName = batchFsReq.FeatureStoreName
		fsReq.FeatureViewName = batchFsReq.FeatureViewName
		fsReq.FeatureViewVersion = batchFsReq.FeatureViewVersion
		fsReq.Entries = entry
		fsReq.PassedFeatures = &pf
		fvReqs <- &FeatureStoreRequestWithOrder{&fsReq, i}
	}
	close(fvReqs)
	wg.Wait()
	wgResp.Wait()
	defer close(fvResps)
	if batchResponseWithStatus.Error != nil {
		batchResponse = nil
		return batchResponseWithStatus.Status, batchResponseWithStatus.Error
	}
	return http.StatusOK, nil
}

func processFsRequest(
	fshandler feature_store.Handler,
	requests chan *FeatureStoreRequestWithOrder,
	responses chan *FeatureStoreResponseWithOrder,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for req := range requests {
		response := api.FeatureStoreResponse{}
		var valErr = fshandler.Validate(req.FsResponse)
		if valErr != nil {
			responses <- &FeatureStoreResponseWithOrder{
				&response, req.Order, http.StatusBadRequest, valErr,
			}
			continue
		}
		var status, err = fshandler.Execute(req.FsResponse, &response)
		responses <- &FeatureStoreResponseWithOrder{
			&response, req.Order, status, err,
		}
	}
}

func processFsResp(
	numReq int,
	batchResponse *FeatureStoreBatchResponseWithStatus,
	responses chan *FeatureStoreResponseWithOrder,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	batchResponse.FsResponse.Features = make([][]interface{}, numReq)
	for i := 0; i < numReq; i++ {
		var fvResp = <-responses
		if fvResp.Error != nil {
			batchResponse.Status = fvResp.Status
			batchResponse.Error = fvResp.Error
			continue
		}
		if i == 0 {
			batchResponse.FsResponse.Metadata = fvResp.FsResponse.Metadata
		}
		batchResponse.FsResponse.Features[fvResp.Order] = fvResp.FsResponse.Features
	}
}
