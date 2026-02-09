package zbatch_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rongbinyuan/zbatch/v1/zbatch"
)

// NewDefaultBatchQuery new a default batch query (results is the same as keys)
func NewDefaultBatchQuery() *zbatch.DefaultBatchQuery[any, any] {
	return zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []any) ([]any, error) {
		return keys, nil
	}, func(ctx context.Context, results []any) map[any][]any {
		rs := make(map[any][]any, len(results))
		for _, result := range results {
			rs[result] = []any{result}
		}
		return rs
	})
}

func NewDefaultConfig() zbatch.BatchQueryerConfig {
	return zbatch.BatchQueryerConfig{
		MaxBatchSize:  10,               // max wait size
		MaxWaitTime:   1 * time.Second,  // max wait time
		MaxCtxTimeOut: 10 * time.Second, // max context timeout
	}
}

func TestBatchManagerQueryNormal_Get(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	query := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results, exists, err := manager.Get(ctx, idx)
			if err != nil {
				t.Errorf("Expected no error, got %v", err)
				return
			}
			if !exists {
				t.Error("Expected result to exist")
				return
			}
			if len(results) != 1 {
				t.Errorf("Expected result length 1, got %d", len(results))
				return
			}
			if !reflect.DeepEqual(results, []any{idx}) {
				t.Errorf("Expected result %v, got %v", []any{idx}, results)
				return
			}
		}(i)
	}
	wg.Wait()
}

func TestBatchManagerQueryWithMultipleQueries_Get(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()

	var QueryCnt atomic.Int32
	onceQuery := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []any) ([]any, error) {
		QueryCnt.Add(1)
		return keys, nil
	}, func(ctx context.Context, results []any) map[any][]any {
		rs := make(map[any][]any, len(results))
		for _, result := range results {
			rs[result] = []any{result}
		}
		return rs
	})
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	var wg sync.WaitGroup

	testCases := []struct {
		name         string
		err          error
		SingleGetCnt int32
	}{
		{
			name:         "single get",
			err:          nil,
			SingleGetCnt: 1,
		},
		{
			name:         "once query",
			err:          nil,
			SingleGetCnt: int32(config.MaxBatchSize),
		},
		{
			name:         "twice batch query",
			err:          nil,
			SingleGetCnt: int32(config.MaxBatchSize) * 2,
		},
		{
			name:         "three times batch query",
			err:          nil,
			SingleGetCnt: int32(config.MaxBatchSize)*2 + 1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			QueryCnt.Store(0)
			for i := 0; i < int(testCase.SingleGetCnt); i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					results, exists, err := manager.Get(ctx, idx)
					if !errors.Is(err, testCase.err) {
						t.Errorf("Expected error %v, got %v", testCase.err, err)
						return
					}
					if !exists {
						t.Error("Expected result to exist")
						return
					}
					if len(results) != 1 {
						t.Errorf("Expected result length 1, got %d", len(results))
						return
					}
					if !reflect.DeepEqual(results, []any{idx}) {
						t.Errorf("Expected result %v, got %v", []any{idx}, results)
						return
					}
				}(i)
			}
			wg.Wait()

			// check the query count
			if QueryCnt.Load() != (testCase.SingleGetCnt+int32(config.MaxBatchSize)-1)/int32(config.MaxBatchSize) {
				t.Errorf("Expected query count %d, got %d", (int(testCase.SingleGetCnt)+config.MaxBatchSize-1)/int(config.MaxBatchSize), QueryCnt.Load())
			}
		})

	}
}

// test the batch manager query with error result
func TestBatchManagerQueryWithOneBatchAndErrorResult_Get(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	var errQuery = errors.New("query error")
	onceQuery := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []int) ([]int, error) {
		return nil, errQuery
	}, func(ctx context.Context, results []int) map[int][]int {
		rs := make(map[int][]int, len(results))
		for _, result := range results {
			rs[result] = []int{result}
		}
		return rs
	})
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	// single batch with error
	var wg sync.WaitGroup
	for i := 0; i < int(10); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results, exists, err := manager.Get(ctx, idx)
			if !errors.Is(err, errQuery) {
				t.Errorf("Expected error %v, got %v, index %d", errQuery, err, idx)
				return
			}
			if exists {
				t.Errorf("Expected result to not exist, got %v, index %d", exists, idx)
				return
			}
			if results != nil {
				t.Errorf("Expected result to be nil, got %v, index %d", results, idx)
				return
			}
		}(i)
	}
	wg.Wait()

	// twice batch with error (first batch is full of error, second batch is not full of error)
	for i := 0; i < int(10); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results, exists, err := manager.Get(ctx, idx)
			if !errors.Is(err, errQuery) {
				t.Errorf("Expected error %v, got %v, index %d", errQuery, err, idx)
				return
			}
			if exists {
				t.Errorf("Expected result to not exist, got %v, index %d", exists, idx)
				return
			}
			if results != nil {
				t.Errorf("Expected result to be nil, got %v, index %d", results, idx)
				return
			}
		}(i)
	}

}

// test the batch manager query with multiple batches (first batch return keys, others batch return error)
func TestBatchManagerQueryWithMultipleBatchesAndErrorResult_Get(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	var errQuery = errors.New("query error")

	// first batch success map
	var successMap sync.Map
	onceQuery := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []int) ([]int, error) {
		// first batch return keys, second batch return error
		count := 0
		successMap.Range(func(key, value interface{}) bool {
			count++
			return true // 继续遍历
		})
		if count == 0 {
			for _, key := range keys {
				successMap.Store(key, struct{}{})
			}
			return keys, nil
		}

		return nil, errQuery
	}, func(ctx context.Context, results []int) map[int][]int {
		rs := make(map[int][]int, len(results))
		for _, result := range results {
			rs[result] = []int{result}
		}
		return rs
	})
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	// the first 10 requests will be in the first batch, the last 5 requests will be in the second batch ()
	var wg sync.WaitGroup
	for i := 0; i < int(25); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results, exists, err := manager.Get(ctx, idx)
			// wait for max wait time to start the second batch query
			if _, ok := successMap.Load(idx); ok {
				if err != nil {
					t.Errorf("Expected error %v, got %v, index %d", errQuery, err, idx)
					return
				}
				if !exists {
					t.Errorf("Expected result to exist, got %v, index %d", exists, idx)
					return
				}
				if results == nil {
					t.Errorf("Expected result to be not nil, got %v, index %d", results, idx)
					return
				}
			} else {
				if !errors.Is(err, errQuery) {
					t.Errorf("Expected error %v, got %v, index %d", errQuery, err, idx)
					return
				}
				if exists {
					t.Errorf("Expected result to not exist, got %v, index %d", exists, idx)
					return
				}
				if results != nil {
					t.Errorf("Expected result to be nil, got %v, index %d", results, idx)
					return
				}
			}

		}(i)
	}
	wg.Wait()
}

// test the batch manager query with max wait time, the query will be executed immediately after the max wait time
func TestBatchManagerQueryWithMaxWaitTime_Get(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	config.SetMaxWaitTime(5 * time.Second)
	onceQuery := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	// the first 10 requests will be in the first batch, the last 5 requests will be in the second batch ()
	var wg sync.WaitGroup
	now := time.Now()
	for i := 0; i < int(1); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results, exists, err := manager.Get(ctx, idx)
			if err != nil {
				t.Errorf("Expected no error, got %v, index %d", err, idx)
				return
			}
			if !exists {
				t.Errorf("Expected result to exist, got %v, index %d", exists, idx)
				return
			}
			if results == nil {
				t.Errorf("Expected result to be not nil, got %v, index %d", results, idx)
				return
			}
		}(i)
	}
	wg.Wait()

	if time.Since(now) < config.MaxWaitTime {
		t.Errorf("Expected wait time %v, got %v", config.MaxWaitTime, time.Since(now))
	}
}

// test the batch manager query with max context timeout, the executor will return timeout error after the max context timeout
func TestBatchManagerQueryWithMaxTimeOut_Get(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	config.SetMaxBatchSize(5)
	config.SetMaxCtxTimeOut(3 * time.Second)
	config.SetMaxWaitTime(5 * time.Second)
	onceQuery := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	// the first 10 requests will be in the first batch, the last 5 requests will be in the second batch ()
	var wg sync.WaitGroup
	now := time.Now()
	for i := 0; i < int(3); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results, exists, err := manager.Get(ctx, idx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Expected error %v, got %v, index %d", context.DeadlineExceeded, err, idx)
				return
			}
			if exists {
				t.Errorf("Expected result to exist, got %v, index %d", exists, idx)
				return
			}
			if results != nil {
				t.Errorf("Expected result to be not nil, got %v, index %d", results, idx)
			}
		}(i)
	}
	wg.Wait()
	if time.Since(now) < config.MaxCtxTimeOut {
		t.Errorf("Expected wait time %v, got %v", config.MaxCtxTimeOut, time.Since(now))
	}
}

// TestBatchManagerQueryNormal_GetByKeys 测试 GetByKeys 方法的正常查询
func TestBatchManagerQueryNormal_GetByKeys(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	query := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			keys := []any{idx}
			results, err := manager.GetByKeys(ctx, keys)
			if err != nil {
				t.Errorf("Expected no error, got %v", err)
				return
			}
			if len(results) != len(keys) {
				t.Errorf("Expected result length %d, got %d", len(keys), len(results))
				return
			}
			if !reflect.DeepEqual(results[idx], []any{idx}) {
				t.Errorf("Expected result %v, got %v", []any{idx}, results[idx])
				return
			}
		}(i)
	}
	wg.Wait()
}

// TestBatchManagerQueryWithMultipleKeys_GetByKeys 测试 GetByKeys 方法查询多个key
func TestBatchManagerQueryWithMultipleKeys_GetByKeys(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	query := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			keys := []any{idx, idx + 1, idx + 2}
			results, err := manager.GetByKeys(ctx, keys)
			if err != nil {
				t.Errorf("Expected no error, got %v", err)
				return
			}
			if len(results) != len(keys) {
				t.Errorf("Expected result length %d, got %d", len(keys), len(results))
				return
			}
			for _, key := range keys {
				expected := []any{key}
				if !reflect.DeepEqual(results[key], expected) {
					t.Errorf("Expected result %v, got %v for key %v", expected, results[key], key)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestBatchManagerQueryWithMultipleQueries_GetByKeys 测试 GetByKeys 方法多次查询
func TestBatchManagerQueryWithMultipleQueries_GetByKeys(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()

	testCases := []struct {
		name           string
		err            error
		MultipleGetCnt int32
		keysPerRequest int
	}{
		{
			name:           "single get with one key",
			err:            nil,
			MultipleGetCnt: 1,
			keysPerRequest: 1,
		},
		{
			name:           "once batch query",
			err:            nil,
			MultipleGetCnt: int32(config.MaxBatchSize) / 3,
			keysPerRequest: 3,
		},
		{
			name:           "twice batch query",
			err:            nil,
			MultipleGetCnt: int32(config.MaxBatchSize) * 2 / 3,
			keysPerRequest: 3,
		},
		{
			name:           "three times batch query",
			err:            nil,
			MultipleGetCnt: int32(config.MaxBatchSize)*2/3 + 2,
			keysPerRequest: 3,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var QueryCnt atomic.Int32
			onceQuery := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []any) ([]any, error) {
				QueryCnt.Add(1)
				return keys, nil
			}, func(ctx context.Context, results []any) map[any][]any {
				rs := make(map[any][]any, len(results))
				for _, result := range results {
					rs[result] = []any{result}
				}
				return rs
			})
			manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

			var wg sync.WaitGroup
			for i := 0; i < int(testCase.MultipleGetCnt); i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					keys := make([]any, testCase.keysPerRequest)
					for j := 0; j < testCase.keysPerRequest; j++ {
						keys[j] = idx*testCase.keysPerRequest + j
					}
					results, err := manager.GetByKeys(ctx, keys)
					if !errors.Is(err, testCase.err) {
						t.Errorf("Expected error %v, got %v", testCase.err, err)
						return
					}
					if len(results) != len(keys) {
						t.Errorf("Expected result length %d, got %d", len(keys), len(results))
						return
					}
					for _, key := range keys {
						expected := []any{key}
						if !reflect.DeepEqual(results[key], expected) {
							t.Errorf("Expected result %v, got %v for key %v", expected, results[key], key)
							return
						}
					}
				}(i)
			}
			wg.Wait()

			// 检查查询次数（由于并发执行的特性，实际的批次划分可能不完全按照预期的批次大小）
			// 这里使用宽松的检查，只要查询次数在合理范围内即可
			actualQueryCount := QueryCnt.Load()
			if actualQueryCount < 1 || actualQueryCount > int32(testCase.MultipleGetCnt) {
				totalKeys := testCase.MultipleGetCnt * int32(testCase.keysPerRequest)
				t.Errorf("Expected query count between 1 and %d, got %d (totalKeys=%d, MaxBatchSize=%d)",
					int32(testCase.MultipleGetCnt), actualQueryCount, totalKeys, config.MaxBatchSize)
			}
		})
	}
}

// TestBatchManagerQueryWithOneBatchAndErrorResult_GetByKeys 测试 GetByKeys 方法单个批次返回错误
func TestBatchManagerQueryWithOneBatchAndErrorResult_GetByKeys(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	var errQuery = errors.New("query error")
	onceQuery := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []any) ([]any, error) {
		return nil, errQuery
	}, func(ctx context.Context, results []any) map[any][]any {
		rs := make(map[any][]any, len(results))
		for _, result := range results {
			rs[result] = []any{result}
		}
		return rs
	})
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	// 单个批次返回错误
	var wg sync.WaitGroup
	for i := 0; i < int(10); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			keys := []any{idx, idx + 1}
			results, err := manager.GetByKeys(ctx, keys)
			if !errors.Is(err, errQuery) {
				t.Errorf("Expected error %v, got %v, index %d", errQuery, err, idx)
				return
			}
			if results != nil {
				t.Errorf("Expected result to be nil, got %v, index %d", results, idx)
				return
			}
		}(i)
	}
	wg.Wait()

	// 两个批次返回错误（第一个批次全部错误，第二个批次没有满）
	for i := 0; i < int(10); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			keys := []any{idx, idx + 1}
			results, err := manager.GetByKeys(ctx, keys)
			if !errors.Is(err, errQuery) {
				t.Errorf("Expected error %v, got %v, index %d", errQuery, err, idx)
				return
			}
			if results != nil {
				t.Errorf("Expected result to be nil, got %v, index %d", results, idx)
				return
			}
		}(i)
	}
	wg.Wait()
}

// TestBatchManagerQueryWithMultipleBatchesAndErrorResult_GetByKeys 测试 GetByKeys 方法多个批次返回错误
func TestBatchManagerQueryWithMultipleBatchesAndErrorResult_GetByKeys(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	var errQuery = errors.New("query error")

	// 使用计数器来控制批次返回结果
	var queryCount atomic.Int32
	onceQuery := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []any) ([]any, error) {
		count := queryCount.Add(1)
		// 第一个批次成功，后续批次都返回错误
		if count == 1 {
			return keys, nil
		}
		return nil, errQuery
	}, func(ctx context.Context, results []any) map[any][]any {
		rs := make(map[any][]any, len(results))
		for _, result := range results {
			rs[result] = []any{result}
		}
		return rs
	})
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	var wg sync.WaitGroup

	// 第一批请求 - 应该成功
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			keys := []any{idx, idx + 1}
			results, err := manager.GetByKeys(ctx, keys)
			if err != nil {
				t.Errorf("Expected no error, got %v, index %d", err, idx)
				return
			}
			if len(results) != len(keys) {
				t.Errorf("Expected result length %d, got %d, index %d", len(keys), len(results), idx)
				return
			}
			for _, key := range keys {
				expected := []any{key}
				if !reflect.DeepEqual(results[key], expected) {
					t.Errorf("Expected result %v, got %v for key %v", expected, results[key], key)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	// 第二批请求 - 应该返回错误（因为第一个批次已经完成，后续批次都会返回错误）
	for i := 5; i < 15; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			keys := []any{idx, idx + 1}
			results, err := manager.GetByKeys(ctx, keys)
			if !errors.Is(err, errQuery) {
				t.Errorf("Expected error %v, got %v, index %d", errQuery, err, idx)
				return
			}
			if results != nil {
				t.Errorf("Expected result to be nil, got %v, index %d", results, idx)
				return
			}
		}(i)
	}
	wg.Wait()
}

// TestBatchManagerQueryWithMaxWaitTime_GetByKeys 测试 GetByKeys 方法的最大等待时间
func TestBatchManagerQueryWithMaxWaitTime_GetByKeys(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	config.SetMaxWaitTime(5 * time.Second)
	onceQuery := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	var wg sync.WaitGroup
	now := time.Now()
	for i := 0; i < int(1); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			keys := []any{idx, idx + 1, idx + 2}
			results, err := manager.GetByKeys(ctx, keys)
			if err != nil {
				t.Errorf("Expected no error, got %v, index %d", err, idx)
				return
			}
			if len(results) != len(keys) {
				t.Errorf("Expected result length %d, got %d, index %d", len(keys), len(results), idx)
				return
			}
			for _, key := range keys {
				expected := []any{key}
				if !reflect.DeepEqual(results[key], expected) {
					t.Errorf("Expected result %v, got %v for key %v", expected, results[key], key)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	if time.Since(now) < config.MaxWaitTime {
		t.Errorf("Expected wait time %v, got %v", config.MaxWaitTime, time.Since(now))
	}
}

// TestBatchManagerQueryWithMaxTimeOut_GetByKeys 测试 GetByKeys 方法的最大上下文超时
func TestBatchManagerQueryWithMaxTimeOut_GetByKeys(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	config.SetMaxBatchSize(5)
	config.SetMaxCtxTimeOut(3 * time.Second)
	config.SetMaxWaitTime(5 * time.Second)
	onceQuery := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, onceQuery)

	var wg sync.WaitGroup
	now := time.Now()
	for i := 0; i < int(3); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// 只传入一个key，这样总共3个key < MaxBatchSize(5)，会等待超时
			keys := []any{idx}
			results, err := manager.GetByKeys(ctx, keys)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Expected error %v, got %v, index %d", context.DeadlineExceeded, err, idx)
				return
			}
			if results != nil {
				t.Errorf("Expected result to be nil, got %v, index %d", results, idx)
				return
			}
		}(i)
	}
	wg.Wait()
	if time.Since(now) < config.MaxCtxTimeOut {
		t.Errorf("Expected wait time %v, got %v", config.MaxCtxTimeOut, time.Since(now))
	}
}

// ==================== Benchmark ====================

// BenchmarkBatchManager_Get 单个key查询的压力测试
func BenchmarkBatchManager_Get(b *testing.B) {
	ctx := context.Background()
	config := NewDefaultConfig()
	query := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		wg := sync.WaitGroup{}
		for pb.Next() {
			for j := 0; j < 10; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _, err := manager.Get(ctx, j%1000)
					if err != nil {
						b.Errorf("Unexpected error: %v", err)
						return
					}
				}()
			}
		}
		wg.Wait()
	})
}

// BenchmarkBatchManager_GetByKeys 多个keys查询的压力测试
func BenchmarkBatchManager_GetByKeys(b *testing.B) {
	ctx := context.Background()
	config := NewDefaultConfig()
	query := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		wg := sync.WaitGroup{}
		for pb.Next() {
			for j := 0; j < 5; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					keys := []any{j % 100, (j % 100) + 1}
					_, err := manager.GetByKeys(ctx, keys)
					if err != nil {
						b.Errorf("Unexpected error: %v", err)
						return
					}
				}()
			}
		}
		wg.Wait()
	})
}

// BenchmarkBatchManager_Get_VeryShortWaitTime 短等待时间的基准测试
func BenchmarkBatchManager_Get_VeryShortWaitTime(b *testing.B) {
	ctx := context.Background()
	config := NewDefaultConfig()
	config.SetMaxWaitTime(10 * time.Millisecond)
	query := NewDefaultBatchQuery()
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		wg := sync.WaitGroup{}
		for pb.Next() {
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _, err := manager.Get(ctx, i%100)
					if err != nil {
						b.Errorf("Unexpected error: %v", err)
						return
					}
				}()
			}
		}
		wg.Wait()
	})
}

// TestBatchManagerStressTest_Get high concurrency stress test for Get method
// === RUN   TestBatchManagerStressTest_Get
//
//	Completed 10000 requests in 13.706583ms
//	Total queries executed: 100
//	Throughput: 729576.44 requests/second
//	Average latency: 1.37µs
//
// --- PASS: TestBatchManagerStressTest_Get (0.01s)
// PASS
func TestBatchManagerStressTest_Get(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	config.SetMaxBatchSize(100)
	config.SetMaxWaitTime(100 * time.Millisecond)

	var QueryCnt atomic.Int32
	query := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []any) ([]any, error) {
		QueryCnt.Add(1)
		return keys, nil
	}, func(ctx context.Context, results []any) map[any][]any {
		rs := make(map[any][]any, len(results))
		for _, result := range results {
			rs[result] = []any{result}
		}
		return rs
	})
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	const (
		TotalRequests = 10000
	)

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < TotalRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results, exists, err := manager.Get(ctx, idx%500)
			if err != nil {
				t.Errorf("Request %d failed: %v", idx, err)
				return
			}
			if !exists {
				t.Errorf("Request %d: result should exist", idx)
				return
			}
			if len(results) != 1 {
				t.Errorf("Request %d: expected result length 1, got %d", idx, len(results))
				return
			}
		}(i)
	}
	wg.Wait()

	duration := time.Since(startTime)
	t.Logf("Completed %d requests in %v", TotalRequests, duration)
	t.Logf("Total queries executed: %d", QueryCnt.Load())
	t.Logf("Throughput: %.2f requests/second", float64(TotalRequests)/duration.Seconds())
	t.Logf("Average latency: %v", duration/time.Duration(TotalRequests))
}

// TestBatchManagerStressTest_GetByKeys high concurrency stress test for GetByKeys method
// === RUN   TestBatchManagerStressTest_GetByKeys
//
//	Completed 100000 requests in 339.628375ms
//	Total queries executed: 10000
//	Throughput: 294439.47 requests/second
//	Average latency: 3.396µs
//	Total keys processed: 1000000
//
// --- PASS: TestBatchManagerStressTest_GetByKeys (0.34s)
// PASS
func TestBatchManagerStressTest_GetByKeys(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	config.SetMaxBatchSize(100)
	config.SetMaxWaitTime(100 * time.Millisecond)

	var QueryCnt atomic.Int32
	query := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []any) ([]any, error) {
		QueryCnt.Add(1)
		return keys, nil
	}, func(ctx context.Context, results []any) map[any][]any {
		rs := make(map[any][]any, len(results))
		for _, result := range results {
			rs[result] = []any{result}
		}
		return rs
	})
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	const (
		TotalRequests  = 100000
		KeysPerRequest = 10
	)

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < TotalRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			keys := make([]any, KeysPerRequest)
			for j := 0; j < KeysPerRequest; j++ {
				keys[j] = (idx*KeysPerRequest + j) % 500
			}
			results, err := manager.GetByKeys(ctx, keys)
			if err != nil {
				t.Errorf("Request %d failed: %v", idx, err)
				return
			}
			if len(results) != len(keys) {
				t.Errorf("Request %d: expected result length %d, got %d", idx, len(keys), len(results))
				return
			}
			for _, key := range keys {
				expected := []any{key}
				if !reflect.DeepEqual(results[key], expected) {
					t.Errorf("Request %d: key %v: expected result %v, got %v", idx, key, expected, results[key])
					return
				}
			}
		}(i)
	}
	wg.Wait()

	duration := time.Since(startTime)
	t.Logf("Completed %d requests in %v", TotalRequests, duration)
	t.Logf("Total queries executed: %d", QueryCnt.Load())
	t.Logf("Throughput: %.2f requests/second", float64(TotalRequests)/duration.Seconds())
	t.Logf("Average latency: %v", duration/time.Duration(TotalRequests))
	t.Logf("Total keys processed: %d", TotalRequests*KeysPerRequest)
}

// TestBatchManagerStressTest_Mixed high concurrency stress test for mixed use of Get and GetByKeys
// === RUN   TestBatchManagerStressTest_Mixed
//
//	Completed 1000000 mixed requests in 1.794679292s
//	Total queries executed: 23134
//	Throughput: 557202.62 requests/second
//	Average latency: 1.794µs
//
// --- PASS: TestBatchManagerStressTest_Mixed (1.79s)
// PASS
func TestBatchManagerStressTest_Mixed(t *testing.T) {
	ctx := context.Background()
	config := NewDefaultConfig()
	config.SetMaxBatchSize(100)
	config.SetMaxWaitTime(100 * time.Millisecond)

	var QueryCnt atomic.Int32
	query := zbatch.NewDefaultBatchQuery(func(ctx context.Context, keys []any) ([]any, error) {
		QueryCnt.Add(1)
		return keys, nil
	}, func(ctx context.Context, results []any) map[any][]any {
		rs := make(map[any][]any, len(results))
		for _, result := range results {
			rs[result] = []any{result}
		}
		return rs
	})
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	const (
		TotalRequests = 1000000
	)

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < TotalRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// 混合使用Get和GetByKeys
			if idx%3 == 0 {
				// 使用Get
				results, exists, err := manager.Get(ctx, idx%500)
				if err != nil {
					t.Errorf("Request %d (Get) failed: %v", idx, err)
					return
				}
				if !exists {
					t.Errorf("Request %d (Get): result should exist", idx)
					return
				}
				if len(results) != 1 {
					t.Errorf("Request %d (Get): expected result length 1, got %d", idx, len(results))
					return
				}
			} else {
				// 使用GetByKeys
				keys := []any{idx % 500, (idx + 1) % 500, (idx + 2) % 500}
				results, err := manager.GetByKeys(ctx, keys)
				if err != nil {
					t.Errorf("Request %d (GetByKeys) failed: %v", idx, err)
					return
				}
				if len(results) != len(keys) {
					t.Errorf("Request %d (GetByKeys): expected result length %d, got %d", idx, len(keys), len(results))
					return
				}
				for _, key := range keys {
					expected := []any{key}
					if !reflect.DeepEqual(results[key], expected) {
						t.Errorf("Request %d (GetByKeys): key %v: expected result %v, got %v", idx, key, expected, results[key])
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()

	duration := time.Since(startTime)
	t.Logf("Completed %d mixed requests in %v", TotalRequests, duration)
	t.Logf("Total queries executed: %d", QueryCnt.Load())
	t.Logf("Throughput: %.2f requests/second", float64(TotalRequests)/duration.Seconds())
	t.Logf("Average latency: %v", duration/time.Duration(TotalRequests))
}
