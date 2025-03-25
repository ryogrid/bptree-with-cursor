package bltree

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
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
