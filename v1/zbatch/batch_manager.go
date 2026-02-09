package zbatch

import (
	"context"
	"sync"
)

type BatchQueryManager[K comparable, V any] struct {
	ctx     context.Context    // it will be used to control the query life status
	config  BatchQueryerConfig // batch queryer config, it's used to config the batch queryer
	queryer BatchQueryI[K, V]  // batch queryer implement, the real query function

	executor *BatchQueryExecutor[K, V] // real executor of batch query

	mu sync.Mutex // lock the executor, make sure the executor is only modified by one goroutine at a time
}

// NewBatchQueryManager new a batch query manager
func NewBatchQueryManager[K comparable, V any](ctx context.Context, config BatchQueryerConfig, queryer BatchQueryI[K, V]) *BatchQueryManager[K, V] {
	return &BatchQueryManager[K, V]{
		ctx:      ctx,
		config:   config,
		queryer:  queryer,
		executor: nil, // it will be initialized when the first key is added
	}
}

// Get get the results by key (key is the unique identifier of the data, return the results of the key, whether the key exists and the error)
func (m *BatchQueryManager[K, V]) Get(ctx context.Context, key K) ([]V, bool, error) {
	m.mu.Lock()
	executor := m.executor

	select {
	case <-ctx.Done():
		m.mu.Unlock()
		return nil, false, ctx.Err()
	default:
		// if the context is not done, continue
	}

	canAdd := false
	if executor != nil {
		canAdd = executor.TryAddKey(ctx, key)
		// do perf (if the key can't be added, Remove the reference to the executor)
		// if !canAdd {
		// 	m.executor = nil
		// }
	}

	// if executor is nil or can't add key, create a new executor, initialize the executor
	if !canAdd || executor == nil {
		// new executor and mark the query is pending with wg
		executor = NewBatchQueryExecutor(m.ctx, m.config, m.queryer)

		executor.wg.Add(1)

		// try to add key to the executor (it's must be added)
		_ = executor.TryAddKey(ctx, key)

		// start the trick to start the batch query
		executor.StartTrick()

		// set the executor to the manager
		m.executor = executor
	}

	m.mu.Unlock()

	// wait for the batch query is finished
	executor.wg.Wait()

	if executor.err != nil {
		return nil, false, executor.err
	}

	// get the results from the executor
	results, exists := executor.resultsMap[key]

	return results, exists, nil
}

// GetByKeys get the results by keys (the length of keys is may be more than the max batch size, return the results map of the keys and the error)
func (m *BatchQueryManager[K, V]) GetByKeys(ctx context.Context, keys []K) (map[K][]V, error) {
	m.mu.Lock()

	select {
	case <-ctx.Done():
		m.mu.Unlock()
		return nil, ctx.Err()
	default:
		// if the context is not done, continue
	}

	executor := m.executor
	canAdd := false
	if executor != nil {
		canAdd = executor.TryAddKeys(ctx, keys)
	}

	// if executor is nil or can't add key, create a new executor, initialize the executor
	if executor == nil || !canAdd {
		// new executor and mark the query is pending with wg
		executor = NewBatchQueryExecutor(ctx, m.config, m.queryer)
		executor.wg.Add(1)

		// try to add key to the executor (it's must be added)
		_ = executor.TryAddKeys(ctx, keys)

		// start the trick to start the batch query
		executor.StartTrick()

		// set the executor to the manager
		m.executor = executor
	}

	m.mu.Unlock()

	// wait for the batch query is finished
	executor.wg.Wait()

	// get the results from the executor
	if executor.err != nil {
		return nil, executor.err
	}

	// build the results map
	rs := make(map[K][]V, len(keys))
	for _, key := range keys {
		results, exists := executor.resultsMap[key]
		if exists {
			rs[key] = results
		}
	}
	return rs, nil
}
