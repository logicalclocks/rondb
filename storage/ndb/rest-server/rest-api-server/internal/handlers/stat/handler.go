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

package stat

import (
	"net/http"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct {
	heap        *heap.Heap
	apiKeyCache apikey.Cache
}

func New(heap *heap.Heap, apiKeyCache apikey.Cache) Handler {
	return Handler{heap, apiKeyCache}
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
	rondbStats, dalErr := dal.GetRonDBStats()
	if dalErr != nil {
		return http.StatusInternalServerError, dalErr
	}

	stats := h.heap.GetNativeBuffersStats()

	statsResponse := response.(*api.StatResponse)
	statsResponse.MemoryStats = stats
	statsResponse.RonDBStats = *rondbStats

	return http.StatusOK, nil
}
