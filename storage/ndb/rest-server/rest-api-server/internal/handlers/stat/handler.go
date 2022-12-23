/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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

package stat

import (
	"net/http"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct{}

func (h Handler) Validate(request interface{}) error {
	return nil
}

func (h Handler) Authenticate(apiKey *string, request interface{}) error {
	if !config.Configuration().Security.UseHopsWorksAPIKeys {
		return nil
	}
	return apikey.ValidateAPIKey(apiKey, nil)
}

func (h Handler) Execute(request interface{}, response interface{}) (int, error) {
	rondbStats, err := dal.GetRonDBStats()
	if err != nil {
		return http.StatusInternalServerError, err
	}

	nativeBuffersStats := dal.GetNativeBuffersStats()

	statsResponse := response.(*api.StatResponse)
	statsResponse.MemoryStats = nativeBuffersStats
	statsResponse.RonDBStats = *rondbStats

	return http.StatusOK, nil
}
