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

// RestClient handles REST API calls to RDRS2
type RestClient struct {
	client  *http.Client
	baseURL string
}

// RestOptions holds connection options for REST API
type RestOptions struct {
	Host string
	Port int
	TLS  bool
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/0.1.0", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal request body: %w", err)
	}

	url := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

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
