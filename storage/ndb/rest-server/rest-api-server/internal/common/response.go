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

package common

import "C"
import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

type ErrorResponse struct {
	Error string `json:"error"    form:"error"    binding:"required"`
}

func SetResponseBodyError(c *gin.Context, code int, err error) {
	errstruct := ErrorResponse{Error: fmt.Sprintf("Error Code: %d, Error: %v", code, err)}
	b, _ := json.Marshal(errstruct)
	c.Writer.WriteHeader(code)
	c.Writer.Write(b)
}

func SetResponseBody(c *gin.Context, code int, response interface{}) {
	responseBytes, err := json.Marshal(response)
	if err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		c.Writer.Write(([]byte)(fmt.Sprintf("Unable to marshal response; error: %v", err)))
	} else {
		c.Writer.WriteHeader(code)
		c.Writer.Write(responseBytes)
	}
}
