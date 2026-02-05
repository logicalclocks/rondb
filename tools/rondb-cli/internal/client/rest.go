/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// APIVersion is the REST API version (0.2.0 returns JSON error responses)
const APIVersion = "0.2.0"

// RestClient handles REST API calls to RDRS2
type RestClient struct {
	client  *http.Client
	baseURL string
	apiKey  string
}

// RestOptions holds connection options for REST API
type RestOptions struct {
	Host   string
	Port   int
	TLS    bool
	APIKey string
}

// NewRestClient creates a new REST client with default options
func NewRestClient(host string, port int) (*RestClient, error) {
	return NewRestClientWithOptions(RestOptions{
		Host: host,
		Port: port,
	})
}

// NewRestClientWithOptions creates a new REST client with extended options
func NewRestClientWithOptions(opts RestOptions) (*RestClient, error) {
	protocol := "http"
	if opts.TLS {
		protocol = "https"
	}

	baseURL := fmt.Sprintf("%s://%s:%d", protocol, opts.Host, opts.Port)

	transport := &http.Transport{}
	if opts.TLS {
		transport.TLSClientConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}
	}

	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}

	client := &RestClient{
		client:  httpClient,
		baseURL: baseURL,
		apiKey:  opts.APIKey,
	}

	// Validate connection
	if err := client.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to REST API: %w", err)
	}

	return client, nil
}

// Ping checks if the REST API is reachable
func (c *RestClient) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to reach the API - use a simple GET to check connectivity
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/"+APIVersion, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	defer resp.Body.Close()

	// Any response (even 404) means the server is up
	return nil
}

// Post sends a POST request with JSON body and returns the response
func (c *RestClient) Post(endpoint string, body interface{}) ([]byte, time.Duration, error) {
	return c.doRequest(http.MethodPost, endpoint, body)
}

// Delete sends a DELETE request with JSON body and returns the response
func (c *RestClient) Delete(endpoint string, body interface{}) ([]byte, time.Duration, error) {
	return c.doRequest(http.MethodDelete, endpoint, body)
}

// doRequest sends an HTTP request with JSON body and returns the response
func (c *RestClient) doRequest(method, endpoint string, body interface{}) ([]byte, time.Duration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal request body: %w", err)
	}

	url := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-API-Key", c.apiKey)
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return nil, duration, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, duration, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return data, duration, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(data))
	}

	return data, duration, nil
}

// Close releases any resources held by the client
func (c *RestClient) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

// PrettyJSON formats JSON data with indentation for display
func PrettyJSON(data []byte) string {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, data, "", "  "); err != nil {
		// If we can't pretty print, return as-is
		return string(data)
	}
	return prettyJSON.String()
}
