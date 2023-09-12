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

package health

import (
	"net/http"

	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct {
}

func (h *Handler) Authenticate(apiKey *string, request interface{}) error {
	return nil
}

func New() Handler {
	return Handler{}
}

func (h *Handler) Validate(request interface{}) error {
	return nil
}

func (h *Handler) Execute(request interface{}, response interface{}) (int, error) {
	rondbHealth, dalErr := dal.GetRonDBStats()
	if dalErr != nil {
		return http.StatusInternalServerError, dalErr
	}

	healthResponse := response.(*api.HealthResponse)
	healthResponse.RonDBHealth = int(rondbHealth.NdbConnectionState)

	return http.StatusOK, nil
}
