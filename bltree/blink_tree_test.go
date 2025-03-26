package bltree

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestBasicOperations(t *testing.T) {
	tree := NewBLinkTree(4) // Test splitting with a small order

	// Test Insert and Get
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	// Insert data
	for k, v := range testData {
		err := tree.Insert([]byte(k), []byte(v))
		if err != nil {
			t.Errorf("Failed to insert key %s: %v", k, err)
		}
	}

	// Verify with Get
	for k, v := range testData {
		value, err := tree.Get([]byte(k))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", k, err)
		}
		if string(value) != v {
			t.Errorf("Incorrect value for key %s: got %s, want %s", k, string(value), v)
		}
	}

	// Test for non-existent key
	_, err := tree.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for non-existent key, got: %v", err)
	}

	// Test update
	err = tree.Insert([]byte("key1"), []byte("updated_value1"))
	if err != nil {
		t.Errorf("Failed to update key: %v", err)
	}

	value, err := tree.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get updated key: %v", err)
	}
	if string(value) != "updated_value1" {
		t.Errorf("Incorrect updated value: got %s, want %s", string(value), "updated_value1")
	}

	// Test delete
	err = tree.Delete([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to delete key: %v", err)
	}

	_, err = tree.Get([]byte("key1"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after deletion, got: %v", err)
	}

	// Test delete for non-existent key
	err = tree.Delete([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound when deleting non-existent key, got: %v", err)
	}
}

func TestEmptyKey(t *testing.T) {
	tree := NewBLinkTree(DefaultOrder)

	// Test operations with empty key
	_, err := tree.Get([]byte{})
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey for Get with empty key, got: %v", err)
	}

	err = tree.Insert([]byte{}, []byte("value"))
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey for Insert with empty key, got: %v", err)
	}

	err = tree.Delete([]byte{})
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey for Delete with empty key, got: %v", err)
	}

	_, err = tree.RangeScan([]byte{}, []byte("key"))
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey for RangeScan with empty start key, got: %v", err)
	}
}

func TestRangeScan(t *testing.T) {
	tree := NewBLinkTree(4) // Test splitting with a small order

	// Dataset for range scan test
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for i, k := range keys {
		err := tree.Insert([]byte(k), []byte(fmt.Sprintf("value%d", i)))
		if err != nil {
			t.Fatalf("Failed to insert key %s: %v", k, err)
		}
	}

	testCases := []struct {
		name      string
		startKey  string
		endKey    string
		expectLen int
		expectErr bool
	}{
		{"Full range", "a", "k", 10, false},
		{"Partial range", "c", "g", 4, false},
		{"Single key", "e", "f", 1, false},
		{"Range end", "i", "", 2, false},      // j and i
		{"Omit end key", "d", "", 7, false},   // Search to the end
		{"Start > End", "h", "e", 0, false},   // Return empty result
		{"Empty start key", "", "c", 0, true}, // Return error
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var endKey []byte
			if tc.endKey != "" {
				endKey = []byte(tc.endKey)
			}

			entries, err := tree.RangeScan([]byte(tc.startKey), endKey)

			if tc.expectErr {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(entries) != tc.expectLen {
				t.Errorf("Incorrect number of entries: got %d, want %d", len(entries), tc.expectLen)
				for i, e := range entries {
					t.Logf("Entry %d: %s -> %s", i, string(e.Key), string(e.Value))
				}
			}

			// Check if entries are in order
			for i := 1; i < len(entries); i++ {
				if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
					t.Errorf("Entries not in order: %s >= %s", string(entries[i-1].Key), string(entries[i].Key))
				}
			}
		})
	}
}

func TestLargeDataset(t *testing.T) {
	tree := NewBLinkTree(16)

	// Large dataset to test tree splitting and balancing
	n := 1000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%d", i)
		err := tree.Insert([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to insert key %s: %v", key, err)
		}
	}

	// Verify all items
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		value, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
		}
		if string(value) != expectedValue {
			t.Errorf("Incorrect value for key %s: got %s, want %s", key, string(value), expectedValue)
		}
	}

	// Delete every third item
	for i := 0; i < n; i += 3 {
		key := fmt.Sprintf("key%06d", i)
		err := tree.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete key %s: %v", key, err)
		}
	}

	// Verify deleted and remaining items
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%06d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		value, err := tree.Get([]byte(key))

		if i%3 == 0 {
			// Should be deleted
			if err != ErrKeyNotFound {
				t.Errorf("Key %s should be deleted", key)
			}
		} else {
			// Should exist
			if err != nil {
				t.Errorf("Failed to get key %s: %v", key, err)
			}
			if string(value) != expectedValue {
				t.Errorf("Incorrect value for key %s: got %s, want %s", key, string(value), expectedValue)
			}
		}
	}
}

func TestConcurrentOperations(t *testing.T) {
	tree := NewBLinkTree(DefaultOrder)
	n := 1000
	numGoroutines := 10

	// Insert initial data
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("init-key%06d", i)
		value := fmt.Sprintf("init-value%d", i)
		if err := tree.Insert([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to insert initial data: %v", err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*3) // Buffer for collecting errors

	// Concurrent insert
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("ins-g%d-key%06d", id, i)
				value := fmt.Sprintf("ins-g%d-value%d", id, i)
				if err := tree.Insert([]byte(key), []byte(value)); err != nil {
					errors <- fmt.Errorf("Insert error in goroutine %d: %v", id, err)
					return
				}
			}
		}(g)
	}

	// Concurrent get
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				// Get key from initial data
				key := fmt.Sprintf("init-key%06d", i)
				expectedValue := fmt.Sprintf("init-value%d", i)
				value, err := tree.Get([]byte(key))

				if err != nil {
					errors <- fmt.Errorf("Get error in goroutine %d: %v", id, err)
					return
				}
				if string(value) != expectedValue {
					errors <- fmt.Errorf("Incorrect value in goroutine %d for key %s: got %s, want %s",
						id, key, string(value), expectedValue)
					return
				}

				// Get key inserted by this goroutine
				key = fmt.Sprintf("ins-g%d-key%06d", id, i)
				expectedValue = fmt.Sprintf("ins-g%d-value%d", id, i)
				value, err = tree.Get([]byte(key))

				// Check if already inserted
				if err == nil && string(value) != expectedValue {
					errors <- fmt.Errorf("Incorrect value in goroutine %d for key %s: got %s, want %s",
						id, key, string(value), expectedValue)
					return
				}
			}
		}(g)
	}

	// Concurrent delete and range scan
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Delete every third key
			for i := 0; i < n; i += 3 {
				key := fmt.Sprintf("ins-g%d-key%06d", id, i)
				if err := tree.Delete([]byte(key)); err != nil && err != ErrKeyNotFound {
					errors <- fmt.Errorf("Delete error in goroutine %d: %v", id, err)
					return
				}
			}

			// Perform range scan
			startKey := fmt.Sprintf("ins-g%d-key%06d", id, n/4)
			endKey := fmt.Sprintf("ins-g%d-key%06d", id, n/2)
			entries, err := tree.RangeScan([]byte(startKey), []byte(endKey))
			if err != nil {
				errors <- fmt.Errorf("RangeScan error in goroutine %d: %v", id, err)
				return
			}

			// Check if entries are in order after some deletions
			for i := 1; i < len(entries); i++ {
				if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
					errors <- fmt.Errorf("Entries not in order in goroutine %d: %s >= %s",
						id, string(entries[i-1].Key), string(entries[i].Key))
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check errors
	errCount := 0
	for err := range errors {
		errCount++
		t.Error(err)
		if errCount >= 10 {
			t.Fatal("Too many errors, stopping test")
		}
	}

	// Final verification - check keys that should not be deleted
	for g := 0; g < numGoroutines; g++ {
		for i := 1; i < n; i += 7 { // Skip some for efficiency
			// Check keys that should not be deleted
			if i%3 != 0 {
				key := fmt.Sprintf("ins-g%d-key%06d", g, i)
				expectedValue := fmt.Sprintf("ins-g%d-value%d", g, i)
				value, err := tree.Get([]byte(key))

				if err != nil {
					t.Errorf("Final verification: key %s should exist but got error: %v", key, err)
				} else if string(value) != expectedValue {
					t.Errorf("Final verification: incorrect value for key %s: got %s, want %s",
						key, string(value), expectedValue)
				}
			}
		}
	}
}

// Test concurrent execution of mixed operations
func TestMixedOperations(t *testing.T) {
	tree := NewBLinkTree(8)
	var wg sync.WaitGroup
	numGoroutines := 5
	opsPerGoroutine := 500

	// Track keys inserted for verification
	keyMutex := sync.Mutex{}
	allKeys := make(map[string]bool)

	// Each goroutine performs mixed operations
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Track keys inserted by this goroutine
			localKeys := make([]string, 0, opsPerGoroutine)

			for i := 0; i < opsPerGoroutine; i++ {
				// Determine operation based on i
				op := i % 4 // 0=insert, 1=get, 2=delete, 3=range scan

				switch op {
				case 0: // Insert
					key := fmt.Sprintf("g%d-key%06d", id, i)
					value := fmt.Sprintf("g%d-value%d", id, i)
					if err := tree.Insert([]byte(key), []byte(value)); err != nil {
						t.Errorf("Insert error in goroutine %d: %v", id, err)
					} else {
						localKeys = append(localKeys, key)
						keyMutex.Lock()
						allKeys[key] = true
						keyMutex.Unlock()
					}

				case 1: // Get
					if len(localKeys) > 0 {
						// Get previously inserted key
						idx := i % len(localKeys)
						key := localKeys[idx]

						// Extract index from key to construct value
						var keyIndex int
						fmt.Sscanf(key, fmt.Sprintf("g%d-key%%06d", id), &keyIndex)
						expectedValue := fmt.Sprintf("g%d-value%d", id, keyIndex)

						result, err := tree.Get([]byte(key))
						if err != nil {
							t.Errorf("Get error in goroutine %d: %v", id, err)
						} else if string(result) != expectedValue {
							t.Errorf("Incorrect value in goroutine %d for key %s: got %s, want %s",
								id, key, string(result), expectedValue)
						}
					}

				case 2: // Delete
					if len(localKeys) > 10 { // Keep some keys
						// Delete key
						idx := i % (len(localKeys) / 2) // Delete from the first half to avoid out of range
						key := localKeys[idx]

						err := tree.Delete([]byte(key))
						if err != nil {
							t.Errorf("Delete error in goroutine %d: %v", id, err)
						}

						// Remove from tracking
						localKeys = append(localKeys[:idx], localKeys[idx+1:]...)
						keyMutex.Lock()
						delete(allKeys, key)
						keyMutex.Unlock()
					}

				case 3: // Range scan
					if len(localKeys) >= 2 {
						// Simple bubble sort to sort keys
						sortedKeys := make([]string, len(localKeys))
						copy(sortedKeys, localKeys)

						for j := 0; j < len(sortedKeys)-1; j++ {
							for k := j + 1; k < len(sortedKeys); k++ {
								if sortedKeys[j] > sortedKeys[k] {
									sortedKeys[j], sortedKeys[k] = sortedKeys[k], sortedKeys[j]
								}
							}
						}

						// Specify range with first and middle key
						startKey := sortedKeys[0]
						endKey := sortedKeys[len(sortedKeys)/2]

						entries, err := tree.RangeScan([]byte(startKey), []byte(endKey))
						if err != nil {
							t.Errorf("RangeScan error in goroutine %d: %v", id, err)
						} else {
							// Verify entries are in order
							for j := 1; j < len(entries); j++ {
								if bytes.Compare(entries[j-1].Key, entries[j].Key) >= 0 {
									t.Errorf("Entries not in order in goroutine %d", id)
									break
								}
							}
						}
					}
				}
			}
		}(g)
	}

	wg.Wait()

	// Final verification - check all keys that should exist
	keyMutex.Lock()
	keysToCheck := make([]string, 0, len(allKeys))
	for k := range allKeys {
		keysToCheck = append(keysToCheck, k)
	}
	keyMutex.Unlock()

	for _, key := range keysToCheck {
		var goroutineID, index int
		fmt.Sscanf(key, "g%d-key%06d", &goroutineID, &index)
		expectedValue := fmt.Sprintf("g%d-value%d", goroutineID, index)

		value, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Final verification: key %s should exist but got error: %v", key, err)
		} else if string(value) != expectedValue {
			t.Errorf("Final verification: incorrect value for key %s: got %s, want %s",
				key, string(value), expectedValue)
		}
	}
}

// Benchmark configuration
const (
	totalOperations    = 100000 // Total number of operations
	preloadedKeysCount = 500000 // Number of keys to preload before benchmarking
	insertRatio        = 0.1    // Ratio of insert operations
	getRatio           = 0.8    // Ratio of get operations
	deleteRatio        = 0.1    // Ratio of delete operations
	keySize            = 8      // Key size in bytes
	valueSize          = 8      // Value size in bytes
	treeOrder          = 16     // Tree order
)

// Number of goroutines to measure
var goroutineCounts = []int{1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 24, 32}

// BenchmarkConcurrentAccess measures performance of parallel access
func BenchmarkConcurrentAccess(b *testing.B) {
	// Reset timer to run only once
	b.ResetTimer()
	b.StopTimer()

	for _, numGoroutines := range goroutineCounts {
		// Run multiple times for each thread count to calculate average
		b.Run(fmt.Sprintf("Goroutines-%d", numGoroutines), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Create a new tree
				tree := NewBLinkTree(treeOrder)

				// Calculate operations per goroutine
				opsPerGoroutine := totalOperations / numGoroutines

				// Start measurement
				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(numGoroutines)

				// Launch each goroutine
				for g := 0; g < numGoroutines; g++ {
					go func(id int) {
						defer wg.Done()

						// Distribute key ranges (each goroutine works on different key range)
						keyPrefix := fmt.Sprintf("g%d-", id)

						// Calculate number of operations by type
						insertOps := int(float64(opsPerGoroutine) * insertRatio)
						getOps := int(float64(opsPerGoroutine) * getRatio)
						deleteOps := int(float64(opsPerGoroutine) * deleteRatio)

						// Insert operations
						for i := 0; i < insertOps; i++ {
							key := []byte(fmt.Sprintf("%skey%06d", keyPrefix, i))
							value := make([]byte, valueSize)
							// Fill with dummy data
							for j := 0; j < valueSize; j++ {
								value[j] = byte((i + j) % 256)
							}
							tree.Insert(key, value)
						}

						// Get operations
						for i := 0; i < getOps; i++ {
							// Select a key from previously inserted keys
							idx := i % insertOps
							key := []byte(fmt.Sprintf("%skey%06d", keyPrefix, idx))
							_, _ = tree.Get(key)
						}

						// Delete operations
						for i := 0; i < deleteOps; i++ {
							// Select a key from previously inserted keys
							idx := i % insertOps
							key := []byte(fmt.Sprintf("%skey%06d", keyPrefix, idx))
							_ = tree.Delete(key)
						}
					}(g)
				}

				// Wait for all goroutines to complete
				wg.Wait()

				// End measurement
				elapsed := time.Since(start)

				// Display results
				opsPerSecond := float64(totalOperations) / elapsed.Seconds()
				b.ReportMetric(opsPerSecond, "ops/sec")
				b.ReportMetric(float64(elapsed)/float64(time.Millisecond), "ms/op")
			}
		})
	}
}

// Benchmarks for specific workloads

// BenchmarkInsertOnly measures insert-only performance
func BenchmarkInsertOnly(b *testing.B) {
	benchmarkSpecificWorkload(b, 1.0, 0, 0)
}

// BenchmarkGetOnly measures get-only performance
func BenchmarkGetOnly(b *testing.B) {
	// Preload: Insert data into the tree
	tree := NewBLinkTree(treeOrder)
	for i := 0; i < totalOperations; i++ {
		key := []byte(fmt.Sprintf("preload-key%06d", i))
		value := make([]byte, valueSize)
		tree.Insert(key, value)
	}

	benchmarkSpecificWorkload(b, 0, 1.0, 0, tree)
}

// BenchmarkMixedWorkload measures performance with mixed operations
func BenchmarkMixedWorkload(b *testing.B) {
	benchmarkSpecificWorkload(b, insertRatio, getRatio, deleteRatio)
}

// Helper function to run benchmarks with specific workload profiles
func benchmarkSpecificWorkload(b *testing.B, insRatio, getRatio, delRatio float64, preloadedTree ...*BLinkTree) {
	b.ResetTimer()
	b.StopTimer()

	for _, numGoroutines := range goroutineCounts {
		b.Run(fmt.Sprintf("Goroutines-%d", numGoroutines), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Prepare tree
				var tree *BLinkTree
				if len(preloadedTree) > 0 {
					tree = preloadedTree[0]
				} else {
					tree = NewBLinkTree(treeOrder)
				}

				opsPerGoroutine := totalOperations / numGoroutines

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(numGoroutines)

				for g := 0; g < numGoroutines; g++ {
					go func(id int) {
						defer wg.Done()

						keyPrefix := fmt.Sprintf("g%d-", id)

						insertOps := int(float64(opsPerGoroutine) * insRatio)
						getOps := int(float64(opsPerGoroutine) * getRatio)
						deleteOps := int(float64(opsPerGoroutine) * delRatio)

						// Execute operations
						insertedKeys := make([]string, 0, insertOps)

						// Insert
						for i := 0; i < insertOps; i++ {
							key := fmt.Sprintf("%skey%06d", keyPrefix, i)
							insertedKeys = append(insertedKeys, key)
							value := make([]byte, valueSize)
							for j := 0; j < valueSize; j++ {
								value[j] = byte((i + j) % 256)
							}
							tree.Insert([]byte(key), value)
						}

						// Get
						for i := 0; i < getOps; i++ {
							var key string
							if len(insertedKeys) > 0 {
								// Get from inserted keys
								key = insertedKeys[i%len(insertedKeys)]
							} else if len(preloadedTree) > 0 {
								// Get from preloaded tree
								key = fmt.Sprintf("preload-key%06d", (id*opsPerGoroutine+i)%totalOperations)
							} else {
								continue
							}
							_, _ = tree.Get([]byte(key))
						}

						// Delete
						for i := 0; i < deleteOps; i++ {
							if len(insertedKeys) == 0 {
								continue
							}
							key := insertedKeys[i%len(insertedKeys)]
							_ = tree.Delete([]byte(key))
						}
					}(g)
				}

				wg.Wait()

				elapsed := time.Since(start)

				// Results
				opsPerSecond := float64(totalOperations) / elapsed.Seconds()
				b.ReportMetric(opsPerSecond, "ops/sec")
				b.ReportMetric(float64(elapsed)/float64(time.Millisecond), "ms/op")
			}
		})
	}
}

// BenchmarkScalability evaluates the scalability of the tree
func BenchmarkScalability(b *testing.B) {
	// Measure baseline performance with 1 thread
	baseTree := NewBLinkTree(treeOrder)
	singleThreadStart := time.Now()

	// Single-thread operations
	insertOps := int(float64(totalOperations) * insertRatio)
	getOps := int(float64(totalOperations) * getRatio)
	deleteOps := int(float64(totalOperations) * deleteRatio)

	// Insert
	for i := 0; i < insertOps; i++ {
		key := []byte(fmt.Sprintf("base-key%06d", i))
		value := make([]byte, valueSize)
		baseTree.Insert(key, value)
	}

	// Get
	for i := 0; i < getOps; i++ {
		key := []byte(fmt.Sprintf("base-key%06d", i%insertOps))
		_, _ = baseTree.Get(key)
	}

	// Delete
	for i := 0; i < deleteOps; i++ {
		key := []byte(fmt.Sprintf("base-key%06d", i%insertOps))
		_ = baseTree.Delete(key)
	}

	singleThreadTime := time.Since(singleThreadStart)

	// Measure multi-thread performance and evaluate scalability
	for _, numGoroutines := range goroutineCounts {
		if numGoroutines == 1 {
			continue // Already measured single-thread
		}

		b.Run(fmt.Sprintf("ScaleFactor-%d", numGoroutines), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tree := NewBLinkTree(treeOrder)
				opsPerGoroutine := totalOperations / numGoroutines

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(numGoroutines)

				for g := 0; g < numGoroutines; g++ {
					go func(id int) {
						defer wg.Done()

						keyPrefix := fmt.Sprintf("g%d-", id)

						insertOps := int(float64(opsPerGoroutine) * insertRatio)
						getOps := int(float64(opsPerGoroutine) * getRatio)
						deleteOps := int(float64(opsPerGoroutine) * deleteRatio)

						// Execute operations
						for i := 0; i < insertOps; i++ {
							key := []byte(fmt.Sprintf("%skey%06d", keyPrefix, i))
							value := make([]byte, valueSize)
							tree.Insert(key, value)
						}

						for i := 0; i < getOps; i++ {
							key := []byte(fmt.Sprintf("%skey%06d", keyPrefix, i%insertOps))
							_, _ = tree.Get(key)
						}

						for i := 0; i < deleteOps; i++ {
							key := []byte(fmt.Sprintf("%skey%06d", keyPrefix, i%insertOps))
							_ = tree.Delete(key)
						}
					}(g)
				}

				wg.Wait()

				multiThreadTime := time.Since(start)

				// Calculate scalability factor (ideally close to numGoroutines)
				scaleFactor := singleThreadTime.Seconds() / (multiThreadTime.Seconds() * float64(numGoroutines))
				// 100% is ideal scalability
				scalePercent := scaleFactor * 100

				b.ReportMetric(scalePercent, "%scale")
				b.ReportMetric(float64(multiThreadTime)/float64(time.Millisecond), "ms/op")
			}
		})
	}
}

// BenchmarkRandomMixedAccess measures performance of concurrent random operations on a shared key space
func BenchmarkRandomMixedAccess(b *testing.B) {
	b.ResetTimer()
	b.StopTimer()

	// Store the baseline performance for scaling calculations
	var baselineOpsPerSecond float64
	//var baselineTimeMs float64

	for _, numGoroutines := range goroutineCounts {
		b.Run(fmt.Sprintf("Goroutines-%d", numGoroutines), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Create and preload the tree
				tree := NewBLinkTree(treeOrder)

				// Preload with data
				for j := 0; j < preloadedKeysCount; j++ {
					key := []byte(fmt.Sprintf("key%010d", j))
					value := make([]byte, valueSize)
					// Fill with deterministic data
					for k := 0; k < valueSize; k++ {
						value[k] = byte((j + k) % 256)
					}
					tree.Insert(key, value)
				}

				// Calculate operations per goroutine
				opsPerGoroutine := totalOperations / numGoroutines

				// Pre-calculate operation counts
				insertOpsTotal := int(float64(totalOperations) * insertRatio)
				getOpsTotal := int(float64(totalOperations) * getRatio)
				deleteOpsTotal := int(float64(totalOperations) * deleteRatio)

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(numGoroutines)

				// Initialize random number generators for each goroutine
				rngs := make([]*rand.Rand, numGoroutines)
				for g := 0; g < numGoroutines; g++ {
					rngs[g] = rand.New(rand.NewSource(int64(g) + time.Now().UnixNano()))
				}

				// Launch goroutines
				for g := 0; g < numGoroutines; g++ {
					go func(id int, rng *rand.Rand) {
						defer wg.Done()

						// Local operation counters
						localInsert := 0
						localGet := 0
						localDelete := 0

						// Try to perform balanced operations
						for j := 0; j < opsPerGoroutine; j++ {
							// Choose operation randomly but try to maintain ratio
							opType := rng.Intn(10)

							if opType < int(insertRatio*10) && localInsert < insertOpsTotal/numGoroutines {
								// Insert operation
								// Generate a random key in the shared key space
								keyNum := rng.Intn(preloadedKeysCount * 2) // Expand key space
								key := []byte(fmt.Sprintf("key%010d", keyNum))
								value := make([]byte, valueSize)
								for k := 0; k < valueSize; k++ {
									value[k] = byte((keyNum + k) % 256)
								}
								tree.Insert(key, value)
								localInsert++

							} else if opType < int((insertRatio+getRatio)*10) && localGet < getOpsTotal/numGoroutines {
								// Get operation
								keyNum := rng.Intn(preloadedKeysCount)
								key := []byte(fmt.Sprintf("key%010d", keyNum))
								_, _ = tree.Get(key)
								localGet++

							} else if localDelete < deleteOpsTotal/numGoroutines {
								// Delete operation
								keyNum := rng.Intn(preloadedKeysCount)
								key := []byte(fmt.Sprintf("key%010d", keyNum))
								_ = tree.Delete(key)
								localDelete++

							} else {
								// Fallback to get if other operations already met their quota
								keyNum := rng.Intn(preloadedKeysCount)
								key := []byte(fmt.Sprintf("key%010d", keyNum))
								_, _ = tree.Get(key)
								localGet++
							}
						}
					}(g, rngs[g])
				}

				wg.Wait()

				elapsed := time.Since(start)

				// Calculate performance metrics
				opsPerSecond := float64(totalOperations) / elapsed.Seconds()
				timeMs := float64(elapsed) / float64(time.Millisecond)

				// For the first thread count (1), store baseline performance
				if numGoroutines == 1 {
					baselineOpsPerSecond = opsPerSecond
					//baselineTimeMs = timeMs
					b.ReportMetric(100.0, "%speedup") // 100% baseline
				} else {
					// Calculate speedup percentage compared to single thread
					speedupPercent := (opsPerSecond / baselineOpsPerSecond) * 100
					b.ReportMetric(speedupPercent, "%speedup")
				}

				// Report standard metrics
				b.ReportMetric(opsPerSecond, "ops/sec")
				b.ReportMetric(timeMs, "ms/op")
			}
		})
	}
}
