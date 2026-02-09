package zbatch

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type BatchQueryExecutor[K comparable, V any] struct {
	executeCtx context.Context
	cancelFunc context.CancelFunc

	canAdd bool               // whether the batch query is finished
	query  BatchQueryI[K, V]  // query of batch query
	config BatchQueryerConfig // config of batch query

	err        error     // error of batch query
	keys       []K       // keys to process
	results    []V       // results of batch query
	resultsMap map[K][]V // results map: key -> result

	wg   sync.WaitGroup // wait group for batch query
	mu   sync.Mutex     // lock the canAdd and keys and err
	once sync.Once
}

// TryAddKey TryAddKe try to add key to the batch query executor
func (e *BatchQueryExecutor[K, V]) TryAddKey(ctx context.Context, key K) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.canAdd {
		return false
	}
	e.keys = append(e.keys, key)
	if len(e.keys) >= e.config.MaxBatchSize {
		e.canAdd = false
		go func() {
			e.DoQuery()
		}()
	}
	return true
}

// TryAddKeys try to add keys to the batch query executor (the length of keys is may be more than the max batch size)
func (e *BatchQueryExecutor[K, V]) TryAddKeys(ctx context.Context, keys []K) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.canAdd {
		return false
	}
	e.keys = append(e.keys, keys...)
	if len(e.keys) >= e.config.MaxBatchSize {
		e.canAdd = false
		go func() {
			e.DoQuery()
		}()
	}
	return true
}

// DoQuery do the batch query only once
func (e *BatchQueryExecutor[K, V]) DoQuery() {
	e.once.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				e.err = fmt.Errorf("BatchQueryExecutor DoQuery Panic: %v", r)
			}
			if e.cancelFunc != nil {
				e.cancelFunc()
			}
			e.wg.Done()
		}()

		// if ctx is done, return
		ctx := e.executeCtx
		select {
		case <-ctx.Done():
			e.err = ctx.Err()
			return
		default:
			// do nothing
		}

		// do query and set results
		e.results, e.err = e.query.Query(ctx, e.keys)
		if e.err != nil {
			return
		}

		// split results
		e.resultsMap = e.query.SplitResults(ctx, e.results)
	})
}

// StartTrick start the trick to start the batch query
func (e *BatchQueryExecutor[K, V]) StartTrick() {
	// initialize the execute context and cancel function
	e.executeCtx, e.cancelFunc = context.WithCancel(e.executeCtx)

	// if the max wait time is greater than 0, set the execute context with timeout
	if e.config.MaxWaitTime > 0 {
		e.executeCtx, e.cancelFunc = context.WithTimeout(e.executeCtx, e.config.MaxCtxTimeOut)
	}

	go func() {
		select {
		case <-time.After(e.config.MaxWaitTime):
			// after the max wait time, set the canAdd to false and do the query
			e.mu.Lock()
			if e.canAdd {
				e.canAdd = false
				go func() {
					e.DoQuery()
				}()
			}
			e.mu.Unlock()
		case <-e.executeCtx.Done():
			// cancel context, mark the error and done the wait group
			e.mu.Lock()
			if e.canAdd {
				e.canAdd = false
				e.err = e.executeCtx.Err()
				e.wg.Done()
			}
			e.mu.Unlock()
		}
	}()
}

// NewBatchQueryExecutor new a batch query executor
func NewBatchQueryExecutor[K comparable, V any](ctx context.Context, config BatchQueryerConfig, query BatchQueryI[K, V]) *BatchQueryExecutor[K, V] {
	return &BatchQueryExecutor[K, V]{
		executeCtx: ctx,
		config:     config,
		query:      query,
		canAdd:     true,
	}
}
