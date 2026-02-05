/*
 * Copyright (C) 2026, 2026 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

package batchwrite

import (
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/pkg/api"
)

func batchWriteTestMultiple(t *testing.T, tests map[string]api.BatchWriteOperationTestInfo) {
	for name, testInfo := range tests {
		t.Run(name, func(t *testing.T) {
			batchWriteTest(t, testInfo)
		})
	}
}

func batchWriteTest(t testing.TB, testInfo api.BatchWriteOperationTestInfo) {
	if config.GetAll().REST.Enable {
		batchWriteRESTTest(t, testInfo)
	}
}

func checkWriteOpIDandStatus(
	t testing.TB,
	testInfo api.BatchWriteSubOperationTestInfo,
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
