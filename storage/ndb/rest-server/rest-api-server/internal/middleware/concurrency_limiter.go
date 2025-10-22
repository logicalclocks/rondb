/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2025 Hopsworks AB
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

package middleware

import (
	"github.com/gin-gonic/gin"
)

// ConcurrencyLimiterWithQueue limits the number of concurrent requests using a semaphore pattern.
// Parameters:
//   - maxConcurrent: Maximum number of requests that can be processed simultaneously.
//     Must be greater than 0.
//
// Returns:
//   - gin.HandlerFunc: Middleware that can be registered with router.Use()
func ConcurrencyLimiterWithQueue(maxConcurrent uint32) gin.HandlerFunc {
	// Use buffered channel as semaphore
	semaphore := make(chan struct{}, maxConcurrent)

	return func(c *gin.Context) {
		// Acquire slot (blocks if at capacity)
		semaphore <- struct{}{}

		// Release slot when done
		defer func() { <-semaphore }()

		// Process request
		c.Next()
	}
}
