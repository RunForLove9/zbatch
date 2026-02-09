package zbatch

import "time"

// BatchQueryerConfig batch config for query
type BatchQueryerConfig struct {
	MaxBatchSize  int           // if the number of keys is more than the max batch size, the query will be executed immediately
	MaxWaitTime   time.Duration // the query will be executed immediately after the max wait time ( when the batch size is less than the max batch size )
	MaxCtxTimeOut time.Duration // max context timeout
}

type ConfigOption func(config *BatchQueryerConfig)

func WithMaxBatchSize(maxBatchSize int) ConfigOption {
	return func(config *BatchQueryerConfig) {
		config.MaxBatchSize = maxBatchSize
	}
}

func WithMaxWaitTime(maxWaitTime time.Duration) ConfigOption {
	return func(config *BatchQueryerConfig) {
		config.MaxWaitTime = maxWaitTime
	}
}

func WithMaxCtxTimeOut(maxCtxTimeOut time.Duration) ConfigOption {
	return func(config *BatchQueryerConfig) {
		config.MaxCtxTimeOut = maxCtxTimeOut
	}
}

// DefaultBatchQueryerConfig default config for query
func DefaultBatchQueryerConfig() *BatchQueryerConfig {
	return &BatchQueryerConfig{
		MaxBatchSize:  100,
		MaxWaitTime:   1000 * time.Millisecond,
		MaxCtxTimeOut: 3000 * time.Millisecond,
	}
}

// NewBatchQueryerConfig new a batch queryer config with options
func NewBatchQueryerConfig(options ...ConfigOption) *BatchQueryerConfig {
	config := DefaultBatchQueryerConfig()
	for _, option := range options {
		option(config)
	}
	return config
}

func (c *BatchQueryerConfig) SetMaxBatchSize(maxBatchSize int) *BatchQueryerConfig {
	c.MaxBatchSize = maxBatchSize
	return c
}

func (c *BatchQueryerConfig) SetMaxWaitTime(maxWaitTime time.Duration) *BatchQueryerConfig {
	c.MaxWaitTime = maxWaitTime
	return c
}

func (c *BatchQueryerConfig) SetMaxCtxTimeOut(maxCtxTimeOut time.Duration) *BatchQueryerConfig {
	c.MaxCtxTimeOut = maxCtxTimeOut
	return c
}
