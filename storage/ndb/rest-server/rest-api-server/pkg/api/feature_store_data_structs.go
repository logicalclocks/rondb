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
package api

import (
	"encoding/json"
	"fmt"
)

type FeatureStoreRequest struct {
	FeatureStoreName   *string                      `json:"featureStoreName"`
	FeatureViewName    *string                      `json:"featureViewName"`
	FeatureViewVersion *int                         `json:"featureViewVersion"`
	PassedFeatures     *map[string]*json.RawMessage `json:"passedFeatures"`
	Entries            *map[string]*json.RawMessage `json:"entries"`
	RequestId          *string                      `json:"requestId"`
}

func (freq FeatureStoreRequest) String() string {
	strBytes, err := json.MarshalIndent(freq, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshal FeatureStoreRequest. Error: %v", err)
	} else {
		return string(strBytes)
	}
}

type FeatureStoreResponse struct {
	Features []interface{}       `json:"features"`
	Metadata []*FeatureMeatadata `json:"metadata"`
}

type FeatureMeatadata struct {
}

func (r *FeatureStoreResponse) String() string {
	strBytes, err := json.MarshalIndent(*r, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshar PKReadResponseJSON. Error: %v", err)
	} else {
		return string(strBytes)
	}
}
