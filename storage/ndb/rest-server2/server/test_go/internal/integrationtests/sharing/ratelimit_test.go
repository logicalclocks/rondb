/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

package sharing

import (
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// userb is a member of userb_project only; usera_project's feature store is
// shared entirely with userb_project (fine_grained_sharing_data.sql, share
// row 100001: feature_store 100000 -> shared_with_project 100001). So userb
// legitimately reads usera_project - the A1_* scenarios assert 200 - without
// being a member of it.
//
// Hopsworks grants that share to the MySQL account of the RECEIVING project
// (OnlineFeaturestoreController.shareOnlineFeatureStore uses
// onlineDbUsername(member.getProject(), member.getUser()) where getProject()
// is the recipient), so SQL reads of usera_project by that user are billed to
// userb_project_userb000. RDRS must reach the same bucket, otherwise every
// shared-store read over REST is unmetered while its SQL equivalent is not.
const sharedStoreReaderIdentity = "userb_project_userb000"

// TestSharedStoreReadIsBilledToRecipientProject verifies that a read of a
// database reached only through a store share is rate limited under the
// reader's own project-user.
//
// Before RONDB-978's shared-store billing this was the rate limit bypass:
// rlIdentities only held the owner's member projects, so usera_project
// resolved to no identity and the request ran unmetered no matter how low the
// reader's quota was set. Capping userb's identity and then hammering
// usera_project is exactly the case that used to sail through.
func TestSharedStoreReadIsBilledToRecipientProject(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)
	if config.GetAll().RateLimit.Identity != "username" {
		t.Skip("Skipping: shared-store billing only applies in username identity mode")
	}

	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.ScanFilter{
			Op:     "CMP",
			Column: "customer_id",
			Cond:   "EQ",
			Value:  1,
		},
	}
	body, err := json.Marshal(query)
	if err != nil {
		t.Fatalf("marshal scan query: %v", err)
	}
	reqBody := string(body)
	url := testutils.NewScanURL(useraProject, customersTable)

	testutils.RunRateLimitTestForIdentity(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClientAndKey(t, client,
			testdbs.USERB_API_KEY, config.SCAN_HTTP_VERB, url, reqBody, "",
			http.StatusOK, http.StatusTooManyRequests)
		return code
	}, sharedStoreReaderIdentity)
}
