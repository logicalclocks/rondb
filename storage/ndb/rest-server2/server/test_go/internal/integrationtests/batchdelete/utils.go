/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026, 2026 Hopsworks AB
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

package batchdelete

import (
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/pkg/api"
)

func batchDeleteTestMultiple(t *testing.T, tests map[string]api.BatchDeleteOperationTestInfo) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			batchDeleteTest(t, testInfo)
		})
	}
}

func batchDeleteTest(t testing.TB, testInfo api.BatchDeleteOperationTestInfo) {
	if config.GetAll().REST.Enable {
		batchDeleteRESTTest(t, testInfo)
	}
}

func checkDeleteOpIDandStatus(
	t testing.TB,
	testInfo api.BatchDeleteSubOperationTestInfo,
	opIDGot *string,
	statusGot int,
	subResponse api.PKReadResponseWithCode,
) {
	expectingOpID := testInfo.SubOperation.Body.OperationID
	expectingStatus := testInfo.HttpCode

	if expectingOpID != nil && opIDGot != nil {
		if *expectingOpID != *opIDGot {
			t.Fatalf("Operation ID does not match. Expecting: %s, Got: %s. TestInfo: %v",
				*expectingOpID, *opIDGot, testInfo)
		}
	}

	idx := -1
	for i, c := range expectingStatus {
		if c == statusGot {
			idx = i
		}
	}
	if idx == -1 {
		t.Fatalf("Return code does not match. Expecting: %v, Got: %d. TestInfo: %v. Body: %v.",
			expectingStatus, statusGot, testInfo, subResponse.String())
	}
}
