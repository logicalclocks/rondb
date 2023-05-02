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

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers/batchpkread"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

type Handler struct {
	apiKeyCache   apikey.Cache
	dbBatchReader batchpkread.Handler
}

func New(apiKeyCache apikey.Cache, batchPkReadHandler batchpkread.Handler) Handler {
	return Handler{apiKeyCache, batchPkReadHandler}
}

func (h *Handler) Validate(request interface{}) error {
	// TODO check you request is valid
	return nil
}

func (h *Handler) Authenticate(apiKey *string, request interface{}) error {
	conf := config.GetAll()
	if !conf.Security.APIKey.UseHopsworksAPIKeys {
		return nil
	}
	return h.apiKeyCache.ValidateAPIKey(apiKey)
}

func (h *Handler) Execute2(request interface{}, response interface{}) (int, error) {
	fsReq := request.(*api.FeatureStoreRequest)
	if log.IsDebug() {
		log.Debugf("Feature store request received %v", fsReq)
	}

	fsResp := response.(*api.FeatureStoreResponse)
	fsResp.RespData = fmt.Sprintf("You sent me \"%s\". I do not know what to do with it", fsReq.ReqData)

	// TODO something
	return http.StatusOK, nil
}

/*
* Just testing. forwarding the request to DB
 */
func (h *Handler) Execute(request interface{}, response interface{}) (int, error) {
	fsReq := request.(*api.FeatureStoreRequest)
	if log.IsDebug() {
		log.Debugf("Feature store request received %v", fsReq)
	}

	// what ever the request is I will read some dummy data from the DB
	//
	testDb := testdbs.FSDB001
	testTable := "sample_1_1"
	candidateKey1Name := string("id1")
	candidateKey1Value := json.RawMessage([]byte("80"))
	filters := []api.Filter{
		{Column: &candidateKey1Name, Value: &candidateKey1Value},
	}
	opID := "some_op_id"

	pkReq := api.PKReadParams{
		DB:          &testDb,
		Table:       &testTable,
		Filters:     &filters,
		OperationID: &opID,
	}

	pkReqs := []*api.PKReadParams{
		&pkReq,
	}

	err := h.dbBatchReader.Validate(&pkReqs)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	var dbResponseIntf api.BatchOpResponse = (api.BatchOpResponse)(&api.BatchResponseJSON{})
	dbResponseIntf.Init()

	code, err := h.dbBatchReader.Execute(&pkReqs, dbResponseIntf)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	// convert the response to string as api.FeatureStoreResponse.RespData is string
	fsResp := response.(*api.FeatureStoreResponse)
	fsResp.RespData = fmt.Sprintf("You sent me \"%s\". And I read some thing from DB %s", fsReq.ReqData, dbResponseIntf)

	return code, nil
}
