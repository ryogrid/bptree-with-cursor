package bptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestBPlusTree_Insert(t *testing.T) {
	tree := NewBPlusTree(3)

	// Test basic insertion
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, data := range testData {
		err := tree.Insert(data.key, data.value)
		if err != nil {
			t.Errorf("Failed to insert key %s: %v", data.key, err)
		}
	}

	// Test key overwrite
	err := tree.Insert([]byte("key1"), []byte("newvalue1"))
	if err != nil {
		t.Errorf("Failed to overwrite existing key: %v", err)
	}

	value, exists := tree.Get([]byte("key1"))
	if !exists {
		t.Error("Key not found after overwrite")
	}
	if !bytes.Equal(value, []byte("newvalue1")) {
		t.Error("Value not overwritten correctly")
	}

	// Test nil key
	err = tree.Insert(nil, []byte("value"))
	if err == nil {
		t.Error("Expected error when inserting nil key")
	}
}

func TestBPlusTree_Get(t *testing.T) {
	tree := NewBPlusTree(3)

	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err := tree.Insert([]byte(k), []byte(v))
		if err != nil {
			t.Errorf("Failed to insert test data: %v", err)
		}
	}

	// Test existing keys
	for k, v := range testData {
		value, exists := tree.Get([]byte(k))
		if !exists {
			t.Errorf("Key %s not found", k)
		}
		if !bytes.Equal(value, []byte(v)) {
			t.Errorf("Wrong value for key %s", k)
		}
	}

	// Test non-existent key
	_, exists := tree.Get([]byte("nonexistent"))
	if exists {
		t.Error("Found non-existent key")
	}
}

func TestBPlusTree_Iterator(t *testing.T) {
	tree := NewBPlusTree(3)

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
	}

	for _, data := range testData {
		err := tree.Insert([]byte(data.key), []byte(data.value))
		if err != nil {
			t.Errorf("Failed to insert test data: %v", err)
		}
	}

	// Test range scan
	it := tree.NewIterator([]byte("key2"))
	count := 0
	expectedValues := []string{"key2", "key3", "key4", "key5"}

	// First call to Next() to get to the first element
	if !it.Next() {
		t.Fatal("Iterator should have elements")
	}

	// Verify each element
	for count < len(expectedValues) {
		key := it.Key()
		value := it.Value()

		if key == nil || value == nil {
			t.Error("Iterator returned nil key or value")
			break
		}

		expectedKey := expectedValues[count]
		if !bytes.Equal(key, []byte(expectedKey)) {
			t.Errorf("Expected key %s, got %s", expectedKey, string(key))
		}

		count++
		if count < len(expectedValues) && !it.Next() {
			t.Error("Iterator ended prematurely")
			break
		}
	}

	// Verify we got all expected elements
	if count != len(expectedValues) {
		t.Errorf("Iterator returned wrong number of elements. Expected %d, got %d", len(expectedValues), count)
	}

	// Verify iterator is done
	if it.Next() {
		t.Error("Iterator should be done but returned true")
	}
}

func TestBPlusTree_Delete(t *testing.T) {
	tree := NewBPlusTree(3)

	// Test deleting from empty tree
	err := tree.Delete([]byte("nonexistent"))
	if err != nil {
		t.Errorf("Delete from empty tree should not return error: %v", err)
	}

	// Test deleting nil key
	err = tree.Delete(nil)
	if err == nil {
		t.Error("Delete with nil key should return error")
	}

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
	}

	for _, data := range testData {
		err := tree.Insert([]byte(data.key), []byte(data.value))
		if err != nil {
			t.Errorf("Failed to insert test data: %v", err)
		}
	}

	// Test deleting existing keys
	deleteTests := []struct {
		deleteKey string
		remaining []string
	}{
		{"key3", []string{"key1", "key2", "key4", "key5"}},
		{"key1", []string{"key2", "key4", "key5"}},
		{"key5", []string{"key2", "key4"}},
	}

	for _, test := range deleteTests {
		err := tree.Delete([]byte(test.deleteKey))
		if err != nil {
			t.Errorf("Failed to delete key %s: %v", test.deleteKey, err)
		}

		// Verify deleted key is gone
		_, exists := tree.Get([]byte(test.deleteKey))
		if exists {
			t.Errorf("Key %s still exists after deletion", test.deleteKey)
		}

		// Verify remaining keys
		for _, expectedKey := range test.remaining {
			_, exists := tree.Get([]byte(expectedKey))
			if !exists {
				t.Errorf("Key %s should exist but was not found", expectedKey)
			}
		}
	}

	// Test deleting non-existent key
	err = tree.Delete([]byte("nonexistent"))
	if err != nil {
		t.Errorf("Delete of non-existent key should not return error: %v", err)
	}

	// Test deleting all remaining keys
	remainingKeys := []string{"key2", "key4"}
	for _, key := range remainingKeys {
		err := tree.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete key %s: %v", key, err)
		}
	}

	// Verify tree is empty
	for _, key := range testData {
		_, exists := tree.Get([]byte(key.key))
		if exists {
			t.Errorf("Key %s still exists after deleting all keys", key.key)
		}
	}
}

func TestBPlusTree_Delete_WithIterator(t *testing.T) {
	tree := NewBPlusTree(3)

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
	}

	for _, data := range testData {
		err := tree.Insert([]byte(data.key), []byte(data.value))
		if err != nil {
			t.Errorf("Failed to insert test data: %v", err)
		}
	}

	// Delete middle key and verify iterator still works
	err := tree.Delete([]byte("key3"))
	if err != nil {
		t.Errorf("Failed to delete key3: %v", err)
	}

	// Test iterator after deletion
	it := tree.NewIterator([]byte("key2"))
	expectedKeys := []string{"key2", "key4", "key5"}
	count := 0

	if !it.Next() {
		t.Fatal("Iterator should have elements")
	}

	for count < len(expectedKeys) {
		key := it.Key()
		if key == nil {
			t.Error("Iterator returned nil key")
			break
		}

		if !bytes.Equal(key, []byte(expectedKeys[count])) {
			t.Errorf("Expected key %s, got %s", expectedKeys[count], string(key))
		}

		count++
		if count < len(expectedKeys) && !it.Next() {
			t.Error("Iterator ended prematurely")
			break
		}
	}

	if count != len(expectedKeys) {
		t.Errorf("Iterator returned wrong number of elements after deletion. Expected %d, got %d", len(expectedKeys), count)
	}
}

func TestBPlusTree_RandomInsertions(t *testing.T) {
	// Test with different degrees
	degrees := []int{2, 3, 4}
	for _, degree := range degrees {
		t.Run(fmt.Sprintf("degree=%d", degree), func(t *testing.T) {
			tree := NewBPlusTree(degree)

			// Point iterator to smallest key - add this at the start
			// to find the minimum key after tests fail
			var minKey []byte

			// Calculate maximum number of elements that can be stored
			// For a tree of height h:
			// - Internal nodes can have up to 2d-1 keys and 2d children
			// - Leaf nodes can have up to 2d-1 keys
			// Let's insert enough elements to ensure at least 2 levels
			maxElements := (2*degree - 1) * (2 * degree)

			// Create random key-value pairs
			type kvPair struct {
				key   []byte
				value []byte
			}
			pairs := make([]kvPair, maxElements)

			// Generate random key-value pairs
			for i := 0; i < maxElements; i++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, uint64(i))
				value := make([]byte, 8)
				binary.BigEndian.PutUint64(value, uint64(i*2))
				pairs[i] = kvPair{key, value}
			}

			// Shuffle the pairs
			rand.Shuffle(len(pairs), func(i, j int) {
				pairs[i], pairs[j] = pairs[j], pairs[i]
			})

			// Insert all pairs and validate after each insertion
			for i, pair := range pairs {
				err := tree.Insert(pair.key, pair.value)
				if err != nil {
					t.Fatalf("Failed to insert pair %d: %v", i, err)
				}

				// Validate tree properties
				if err := tree.Validate(); err != nil {
					t.Fatalf("Tree validation failed after inserting pair %d: %v", i, err)
				}

				// Verify the inserted value can be retrieved
				val, exists := tree.Get(pair.key)
				if !exists {
					t.Fatalf("Inserted key %d not found", i)
				}
				if !bytes.Equal(val, pair.value) {
					t.Fatalf("Wrong value for key %d", i)
				}
			}

			// Verify all pairs can be retrieved
			for i, pair := range pairs {
				val, exists := tree.Get(pair.key)
				if !exists {
					t.Errorf("Key %d not found after all insertions", i)
					continue
				}
				if !bytes.Equal(val, pair.value) {
					t.Errorf("Wrong value for key %d after all insertions", i)
				}
			}

			// After all pairs are inserted, set the minKey to the first key
			// Add this code before the iterator test
			// Find the smallest key for iterator start
			minKey = pairs[0].key
			for _, pair := range pairs {
				if bytes.Compare(pair.key, minKey) < 0 {
					minKey = pair.key
				}
			}

			// Use minKey in the iterator
			it := tree.NewIterator(minKey)

			// Test iterator to verify keys are in order
			var prevKey []byte
			count := 0
			for it.Next() {
				currentKey := it.Key()
				if prevKey != nil && bytes.Compare(prevKey, currentKey) >= 0 {
					t.Error("Keys are not in ascending order")
				}
				prevKey = currentKey
				count++
			}

			if count != maxElements {
				t.Errorf("Iterator returned wrong number of elements. Expected %d, got %d", maxElements, count)
			}
		})
	}
}
