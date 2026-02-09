package zbatch

import "context"

type BatchQueryI[K comparable, V any] interface {
	// Query do batch query
	Query(ctx context.Context, keys []K) ([]V, error)

	// SplitResults split results by keys
	SplitResults(ctx context.Context, results []V) map[K][]V
}

type DefaultBatchQuery[K comparable, V any] struct {
	queryFunc        func(ctx context.Context, keys []K) ([]V, error)
	splitResultsFunc func(ctx context.Context, results []V) map[K][]V
}

func NewDefaultBatchQuery[K comparable, V any](queryFunc func(ctx context.Context, keys []K) ([]V, error), splitResultsFunc func(ctx context.Context, results []V) map[K][]V) *DefaultBatchQuery[K, V] {
	return &DefaultBatchQuery[K, V]{
		queryFunc:        queryFunc,
		splitResultsFunc: splitResultsFunc,
	}
}

func (d *DefaultBatchQuery[K, V]) Query(ctx context.Context, keys []K) ([]V, error) {
	return d.queryFunc(ctx, keys)
}

func (d *DefaultBatchQuery[K, V]) SplitResults(ctx context.Context, results []V) map[K][]V {
	return d.splitResultsFunc(ctx, results)
}
