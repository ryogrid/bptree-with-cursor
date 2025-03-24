package bltree

import (
	"sync"
	"testing"
)

func TestBLinkTree_Insert(t *testing.T) {
	tree := NewBLinkTree()

	// Test basic insertion
	testData := []int{10, 20, 5, 6, 15, 30, 25}

	for _, key := range testData {
		tree.Insert(key)
	}

	// Verify keys are inserted
	for _, key := range testData {
		if !tree.Get(key) {
			t.Errorf("Key %d not found after insertion", key)
		}
	}
}

func TestBLinkTree_Get(t *testing.T) {
	tree := NewBLinkTree()

	// Insert test data
	testData := []int{10, 20, 5, 6, 15, 30, 25}

	for _, key := range testData {
		tree.Insert(key)
	}

	// Test existing keys
	for _, key := range testData {
		if !tree.Get(key) {
			t.Errorf("Key %d not found", key)
		}
	}

	// Test non-existent key
	if tree.Get(100) {
		t.Error("Found non-existent key")
	}
}

func TestBLinkTree_Delete(t *testing.T) {
	tree := NewBLinkTree()

	// Insert test data
	testData := []int{10, 20, 5, 6, 15, 30, 25}

	for _, key := range testData {
		tree.Insert(key)
	}

	// Test deleting existing keys
	for _, key := range testData {
		tree.Delete(key)
		if tree.Get(key) {
			t.Errorf("Key %d still exists after deletion", key)
		}
	}

	// Test deleting non-existent key
	tree.Delete(100)
}

func TestBLinkTree_RangeScan(t *testing.T) {
	tree := NewBLinkTree()

	// Insert test data
	testData := []int{10, 20, 5, 6, 15, 30, 25}

	for _, key := range testData {
		tree.Insert(key)
	}

	// Test range scan
	expected := []int{10, 15, 20, 25, 30}
	result := tree.RangeScan(10, 30)

	if len(result) != len(expected) {
		t.Errorf("RangeScan returned wrong number of elements. Expected %d, got %d", len(expected), len(result))
	}

	for i, key := range expected {
		if result[i] != key {
			t.Errorf("Expected key %d, got %d", key, result[i])
		}
	}
}

func TestBLinkTree_ConcurrentAccess(t *testing.T) {
	tree := NewBLinkTree()
	var wg sync.WaitGroup

	// Number of goroutines
	numGoroutines := 10
	// Number of operations per goroutine
	numOperations := 100

	// Insert keys concurrently
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := id*numOperations + j
				tree.Insert(key)
			}
		}(i)
	}
	wg.Wait()

	// Verify all keys are inserted
	for i := 0; i < numGoroutines*numOperations; i++ {
		if !tree.Get(i) {
			t.Errorf("Key %d not found after concurrent insertion", i)
		}
	}

	// Delete keys concurrently
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := id*numOperations + j
				tree.Delete(key)
			}
		}(i)
	}
	wg.Wait()

	// Verify all keys are deleted
	for i := 0; i < numGoroutines*numOperations; i++ {
		if tree.Get(i) {
			t.Errorf("Key %d still exists after concurrent deletion", i)
		}
	}
}
