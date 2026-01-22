package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RondisClient struct {
	client *redis.Client
}

// RondisOptions holds connection options
type RondisOptions struct {
	Host     string
	Port     int
	Password string
	TLS      bool
}

func NewRondisClient(host string, port int) (*RondisClient, error) {
	return NewRondisClientWithOptions(RondisOptions{
		Host: host,
		Port: port,
	})
}

// NewRondisClientWithOptions creates a new Rondis client with extended options
func NewRondisClientWithOptions(opts RondisOptions) (*RondisClient, error) {
	redisOpts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", opts.Host, opts.Port),
		Password: opts.Password,
	}

	if opts.TLS {
		redisOpts.TLSConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}
	}

	client := redis.NewClient(redisOpts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to rondis: %w", err)
	}

	return &RondisClient{client: client}, nil
}

func (c *RondisClient) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.client.Ping(ctx).Err()
}

func (c *RondisClient) Execute(args []string) (string, time.Duration, error) {
	if len(args) == 0 || args[0] == "" {
		return "", 0, fmt.Errorf("no command provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Convert []string to []interface{} for redis.Do
	iargs := make([]interface{}, len(args))
	for i, a := range args {
		iargs[i] = a
	}

	start := time.Now()
	val := c.client.Do(ctx, iargs...)
	duration := time.Since(start)

	if err := val.Err(); err != nil {
		return "", duration, err
	}

	result := formatResult(val.Val())
	return result, duration, nil
}

func (c *RondisClient) Close() error {
	return c.client.Close()
}

func formatResult(val interface{}) string {
	if val == nil {
		return "(nil)"
	}

	switch v := val.(type) {
	case string:
		return v
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case bool:
		if v {
			return "OK"
		}
		return "(empty)"
	case []interface{}:
		if len(v) == 0 {
			return "(empty list or set)"
		}
		var lines []string
		for i, item := range v {
			lines = append(lines, fmt.Sprintf("%d) %v", i+1, formatResult(item)))
		}
		return strings.Join(lines, "\n")
	default:
		return fmt.Sprintf("%v", v)
	}
}
