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
	"hopsworks.ai/rdrs/internal/handlers/feature_store"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct {
	apiKeyCache apikey.Cache
	fshandler   feature_store.Handler
}

type FeatureStoreResponseWithOrder struct {
	FsResponse *api.FeatureStoreResponse
	Order int
}

type FeatureStoreRequestWithOrder struct {
	FsResponse *api.FeatureStoreRequest
	Order int
}

func New(apiKeyCache apikey.Cache, fshandler feature_store.Handler) Handler {
	return Handler{apiKeyCache, fshandler}
}

func (h *Handler) Validate(request interface{}) error {
	return nil
}

func (h *Handler) Authenticate(apiKey *string, request interface{}) error {
	conf := config.GetAll()
	if !conf.Security.APIKey.UseHopsworksAPIKeys {
		return nil
	}
	return h.apiKeyCache.ValidateAPIKey(apiKey)
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
		go processFsRequest(h.fshandler, fvReqs, fvResps, &wg)
	}
	var batchResponse = response.(*api.BatchFeatureStoreResponse)
	go processFsResp(len(*batchFsReq.Entries), batchResponse, fvResps, &wgResp)

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
		fshandler.Execute(req.FsResponse, &response)
		responses <- &FeatureStoreResponseWithOrder{&response, req.Order}
	}
}

func processFsResp(
	numFeatures int,
	batchResponse *api.BatchFeatureStoreResponse,
	responses chan *FeatureStoreResponseWithOrder,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	batchResponse.Features = make([][]interface{}, numFeatures)
	for i := 0; i < numFeatures; i++ {
		var fvResp = <-responses
		if i == 0 {
			batchResponse.Metadata = fvResp.FsResponse.Metadata
		}
		batchResponse.Features[fvResp.Order] = fvResp.FsResponse.Features
	}
}
