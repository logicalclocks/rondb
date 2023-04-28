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
	"fmt"

	"hopsworks.ai/rdrs/internal/dal"
)

//TODO cache this medata

func GetFeatureStoreMetadata(featureStoreName, featureViewName string, featureViewVersion int) (*dal.FeatureStoreMetadata, error) {

	featureStoreMetadata := dal.FeatureStoreMetadata{}
	fsID, err := dal.GetFeatureStorID(featureStoreName)
	if err != nil {
		return nil, fmt.Errorf("reading feature store ID failed. Error: %s ", err)
	}

	fvID, err := dal.GetFeatureViewID(fsID, featureViewName, featureViewVersion)
	if err != nil {
		return nil, fmt.Errorf("reading feature view ID failed. Error: %s ", err.VerboseError())
	}

	tdJoinID, featureGroupID, prefix, err := dal.GetTrainingDatasetJoinData(fvID)
	if err != nil {
		return nil, fmt.Errorf("reading training dataset join failed. Error: %s ", err.VerboseError())
	}

	name, onlineEnabled, _, err := dal.GetFeatureGroupData(featureGroupID)
	if err != nil {
		return nil, fmt.Errorf("reading feature group failed. Error: %s ", err.VerboseError())
	}

	tdfs, err := dal.GetTrainingDatasetFeature(fvID)
	if err != nil {
		return nil, fmt.Errorf("reading training dataset feature failed %s ", err.VerboseError())
	}

	featureStoreMetadata.FeatureStoreID = fsID
	featureStoreMetadata.FeatureViewID = fvID
	featureStoreMetadata.FeatureStoreName = featureStoreName
	featureStoreMetadata.FeatureViewName = featureViewName
	featureStoreMetadata.FeatureViewVersion = featureViewVersion
	featureStoreMetadata.TDJoinID = tdJoinID
	featureStoreMetadata.TDJoinPrefix = prefix
	featureStoreMetadata.FeatureGroupName = name
	featureStoreMetadata.FeatureGroupOnlineEnabled = onlineEnabled
	featureStoreMetadata.TrainingDatasetFeatures = tdfs

	return &featureStoreMetadata, nil
}
