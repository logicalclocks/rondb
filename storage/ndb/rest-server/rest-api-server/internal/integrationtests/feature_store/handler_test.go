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
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func TestFeatureStore(t *testing.T) {
	req := api.FeatureStoreRequest{ReqData: "data sent by client"}
	reqBody := fmt.Sprintf("%s", req)

	_, respBody := testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB, testutils.NewFeatureStoreURL(), reqBody, "", http.StatusOK)

	fsResp := api.FeatureStoreResponse{}
	err := json.Unmarshal([]byte(respBody), &fsResp)
	if err != nil {
		t.Fatalf("Unmarshal failed %s ", err)
	}

	log.Infof("Response data is %s", fsResp.RespData)
}

func TestFeatureStoreMetaData(t *testing.T) {
	projID, err := dal.GetProjectID("test2")
	if err != nil {
		t.Fatalf("Reading Project ID failed %s ", err)
	}

	log.Infof("Project ID %d \n", projID)

	fsID, err := dal.GetFeatureStorID("test2")
	if err != nil {
		t.Fatalf("Reading Feature Store ID failed %s ", err)
	}

	log.Infof("Feature store ID %d \n", fsID)

	fvID, err := dal.GetFeatureViewID(fsID, "sample_2", 1)
	if err != nil {
		t.Fatalf("Reading Feature View ID failed %s ", err.VerboseError())
	}

	log.Infof("Feature View ID %d \n", fvID)

	tdJoinID, featureGroupID, prefix, err := dal.GetTrainingDatasetJoinData(fvID)
	if err != nil {
		t.Fatalf("Reading Training Dataset Join failed %s ", err.VerboseError())
	}

	log.Infof("Training Dataset Join, td_id: %d, feature_group_id: %d, prefix %s \n", tdJoinID, featureGroupID, prefix)

	name, onlineEnabled, featureGroupID, err := dal.GetFeatureGroupData(featureGroupID)
	if err != nil {
		t.Fatalf("Reading feature group failed %s ", err.VerboseError())
	}

	log.Infof("Freature group. Name: %s, onlineEnabled: %t, featureGroupID: %d\n", name, onlineEnabled, featureGroupID)

}
