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

type BatchFeatureStoreRequest struct {
	FeatureStoreName   *string                         `json:"featureStoreName"`
	FeatureViewName    *string                         `json:"featureViewName"`
	FeatureViewVersion *int                            `json:"featureViewVersion"`
	PassedFeatures     *[]*map[string]*json.RawMessage `json:"passedFeatures"`
	Entries            *[]*map[string]*json.RawMessage `json:"entries"`
	RequestId          *string                         `json:"requestId"`
	MetadataRequest    *MetadataRequest                `json:"metadata_options"`
}

type MetadataRequest struct {
	FeatureName bool `json:"feature_name"`
	FeatureType bool `json:"feature_type"`
}

func (freq BatchFeatureStoreRequest) String() string {
	strBytes, err := json.MarshalIndent(freq, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshal FeatureStoreRequest. Error: %v", err)
	} else {
		return string(strBytes)
	}
}

type FeatureStoreRequest struct {
	FeatureStoreName   *string                      `json:"featureStoreName"`
	FeatureViewName    *string                      `json:"featureViewName"`
	FeatureViewVersion *int                         `json:"featureViewVersion"`
	PassedFeatures     *map[string]*json.RawMessage `json:"passedFeatures"`
	Entries            *map[string]*json.RawMessage `json:"entries"`
	RequestId          *string                      `json:"requestId"`
	MetadataRequest    *MetadataRequest             `json:"metadata_options"`
}

func (freq FeatureStoreRequest) String() string {
	strBytes, err := json.MarshalIndent(freq, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshal FeatureStoreRequest. Error: %v", err)
	} else {
		return string(strBytes)
	}
}

type BatchFeatureStoreResponse struct {
	Features [][]interface{}     `json:"features"`
	Metadata []*FeatureMeatadata `json:"metadata"`
	Status   []FeatureStatus     `json:"status"`
}

func (r *BatchFeatureStoreResponse) String() string {
	strBytes, err := json.MarshalIndent(*r, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshar PKReadResponseJSON. Error: %v", err)
	} else {
		return string(strBytes)
	}
}

type FeatureStatus string

const (
	FEATURE_STATUS_COMPLETE FeatureStatus = "COMPLETE"
	FEATURE_STATUS_MISSING  FeatureStatus = "MISSING"
	FEATURE_STATUS_ERROR    FeatureStatus = "ERROR"
)

type FeatureStoreResponse struct {
	Features []interface{}       `json:"features"`
	Metadata []*FeatureMeatadata `json:"metadata"`
	Status   FeatureStatus       `json:"status"`
}

func (r *FeatureStoreResponse) String() string {
	strBytes, err := json.MarshalIndent(*r, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshal FeatureStoreResponse. Error: %v", err)
	} else {
		return string(strBytes)
	}
}

type FeatureMeatadata struct { // use pointer because fields below are nullable
	Name *string `json:"feature_name"`
	Type *string `json:"feature_type"`
}

func (r *FeatureMeatadata) String() string {
	strBytes, err := json.MarshalIndent(*r, "", "\t")
	if err != nil {
		return fmt.Sprintf("Failed to marshal FeatureMeatadata. Error: %v", err)
	} else {
		return string(strBytes)
	}
}
