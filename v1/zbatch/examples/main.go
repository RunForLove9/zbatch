// @File: main.go
// @Author: rongbinyuan
// @Date: 2026/2/6 14:54
// @Description: Example usage of BatchQueryManager

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/RunForLove9/zbatch/v1/zbatch"
)

// User represents a user entity
type User struct {
	ID   int
	Name string
	Age  int
}

// MockUserQuery is a mock batch query for users
type MockUserQuery struct{}

// Query implements BatchQueryI interface
// It simulates querying users from a database or external service
func (q *MockUserQuery) Query(ctx context.Context, keys []int) ([]User, error) {
	// Simulate database query delay
	time.Sleep(10 * time.Millisecond)

	users := make([]User, 0, len(keys))
	for _, id := range keys {
		users = append(users, User{
			ID:   id,
			Name: fmt.Sprintf("User%d", id),
			Age:  20 + (id % 50),
		})
	}
	return users, nil
}

// SplitResults implements BatchQueryI interface
// It maps query results back to original keys
func (q *MockUserQuery) SplitResults(ctx context.Context, results []User) map[int][]User {
	resultMap := make(map[int][]User, len(results))
	for _, user := range results {
		resultMap[user.ID] = []User{user}
	}
	return resultMap
}

// Example 1: Using Get method to query a single user
// This example demonstrates how to query a single user using the BatchQueryManager
func exampleGet() {
	fmt.Println("=== Example 1: Get Method ===")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Configure batch query settings
	config := zbatch.BatchQueryerConfig{
		MaxBatchSize:  10,                     // Max keys per batch
		MaxWaitTime:   100 * time.Millisecond, // Max wait time before executing batch
		MaxCtxTimeOut: 3 * time.Second,        // Max context timeout for each batch
	}

	// Create query manager
	query := &MockUserQuery{}
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	// Query a single user
	userIDs := []int{1, 2, 3, 4, 5}
	for _, userID := range userIDs {
		users, exists, err := manager.Get(ctx, userID)
		if err != nil {
			fmt.Printf("Error querying user %d: %v\n", userID, err)
			continue
		}
		if !exists {
			fmt.Printf("User %d not found\n", userID)
			continue
		}
		//  Compatible with multiple return values
		fmt.Printf("Queried user: ID=%d, Name=%s, Age=%d\n",
			users[0].ID, users[0].Name, users[0].Age)
	}
	fmt.Println()
}

// Example 2: Using GetByKeys method to query multiple users
// This example demonstrates how to query multiple users in a single request
func exampleGetByKeys() {
	fmt.Println("=== Example 2: GetByKeys Method ===")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Configure batch query settings
	config := zbatch.BatchQueryerConfig{
		MaxBatchSize:  10,                     // Max keys per batch
		MaxWaitTime:   100 * time.Millisecond, // Max wait time before executing batch
		MaxCtxTimeOut: 3 * time.Second,        // Max context timeout for each batch
	}

	// Create query manager
	query := &MockUserQuery{}
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	// Query multiple users at once
	userIDs := []int{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	results, err := manager.GetByKeys(ctx, userIDs)
	if err != nil {
		fmt.Printf("Error querying users: %v\n", err)
		return
	}

	// Print all results
	for userID, users := range results {
		if len(users) > 0 {
			fmt.Printf("User %d: Name=%s, Age=%d\n",
				userID, users[0].Name, users[0].Age)
		}
	}
	fmt.Printf("Total users queried: %d\n\n", len(results))
}

// Example 3: High concurrency scenario
// This example demonstrates batch query performance under high concurrency
func exampleConcurrent() {
	fmt.Println("=== Example 3: High Concurrency ===")

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Configure batch query settings optimized for high concurrency
	config := zbatch.BatchQueryerConfig{
		MaxBatchSize:  100,                   // Larger batch size for better throughput
		MaxWaitTime:   50 * time.Millisecond, // Shorter wait time
		MaxCtxTimeOut: 5 * time.Second,       // Longer timeout
	}

	// Create query manager
	query := &MockUserQuery{}
	manager := zbatch.NewBatchQueryManager(ctx, config, query)

	// Simulate 100 concurrent requests
	const numRequests = 100
	done := make(chan bool, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(userID int) {
			defer func() { done <- true }()

			// Mix of Get and GetByKeys calls
			if userID%3 == 0 {
				// Using Get method
				users, exists, err := manager.Get(ctx, userID)
				if err != nil {
					fmt.Printf("Error (Get): %v\n", err)
					return
				}
				if exists && len(users) > 0 {
					// Success (don't print all to avoid clutter)
				}
			} else {
				// Using GetByKeys method
				keys := []int{userID, userID + 1, userID + 2}
				results, err := manager.GetByKeys(ctx, keys)
				if err != nil {
					fmt.Printf("Error (GetByKeys): %v\n", err)
					return
				}
				if len(results) > 0 {
					// Success, do
				}
			}
		}(i)
	}

	// Wait for all requests to complete
	for i := 0; i < numRequests; i++ {
		<-done
	}

	fmt.Printf("Completed %d concurrent requests\n\n", numRequests)
}

func main() {
	// Run all examples
	exampleGet()
	exampleGetByKeys()
	exampleConcurrent()

	fmt.Println("=== All examples completed ===")
}
