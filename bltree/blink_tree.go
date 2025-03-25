package bltree

// Implementation of Concurrent B-Link Tree

import (
	"bytes"
	"errors"
	"sort"
	"sync"
)

// Constants for tree configuration
const (
	DefaultOrder = 8 // Default number of keys per node
)

// Errors that can be returned
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("empty key is not allowed")
)

// Entry represents a key-value pair
type Entry struct {
	Key   []byte
	Value []byte
}

// Node represents a node in the B-Link tree
type Node struct {
	isLeaf    bool
	entries   []*Entry // For leaf nodes: contains actual entries, for internal nodes: used as routing keys
	children  []*Node  // Only used for internal nodes
	rightLink *Node    // Link to the next node at the same level (B-Link tree property)
	highKey   []byte   // Upper bound for keys in this node
	mutex     sync.RWMutex
}

// BLinkTree represents a B-Link tree
type BLinkTree struct {
	root  *Node
	order int // Maximum number of entries per node
	mutex sync.RWMutex
}

// NewBLinkTree creates a new B-Link tree with the specified order
func NewBLinkTree(order int) *BLinkTree {
	if order <= 0 {
		order = DefaultOrder
	}

	// Create an empty leaf node as the initial root
	root := &Node{
		isLeaf:   true,
		entries:  make([]*Entry, 0),
		children: nil,
	}

	return &BLinkTree{
		root:  root,
		order: order,
	}
}

// Get retrieves the value associated with the given key
func (t *BLinkTree) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	t.mutex.RLock()
	root := t.root
	t.mutex.RUnlock()

	return t.searchNode(root, key)
}

// searchNode searches for a key in a node and its children
func (t *BLinkTree) searchNode(node *Node, key []byte) ([]byte, error) {
	for {
		node.mutex.RLock()

		// Check if we should follow the right link
		if node.highKey != nil && bytes.Compare(key, node.highKey) >= 0 {
			rightNode := node.rightLink
			node.mutex.RUnlock()
			node = rightNode
			continue
		}

		// If it's a leaf node, search for the key using binary search
		if node.isLeaf {
			i := sort.Search(len(node.entries), func(i int) bool {
				return bytes.Compare(node.entries[i].Key, key) >= 0
			})
			if i < len(node.entries) && bytes.Equal(node.entries[i].Key, key) {
				value := make([]byte, len(node.entries[i].Value))
				copy(value, node.entries[i].Value)
				node.mutex.RUnlock()
				return value, nil
			}
			node.mutex.RUnlock()
			return nil, ErrKeyNotFound
		}

		// For internal nodes, find the appropriate child using binary search
		childIndex := sort.Search(len(node.entries), func(i int) bool {
			return bytes.Compare(node.entries[i].Key, key) > 0
		})

		childNode := node.children[childIndex]
		node.mutex.RUnlock()
		node = childNode
	}
}

// Insert adds or updates a key-value pair in the tree
func (t *BLinkTree) Insert(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	// Create a copy of the key and value
	keyCopy := make([]byte, len(key))
	valueCopy := make([]byte, len(value))
	copy(keyCopy, key)
	copy(valueCopy, value)

	t.mutex.RLock()
	root := t.root
	t.mutex.RUnlock()

	// Handle root split if needed
	newNode, splitKey, ok := t.insertIntoNode(root, keyCopy, valueCopy)
	if ok {
		t.mutex.Lock()
		// Check if the root has changed during our operation
		if t.root == root {
			newRoot := &Node{
				isLeaf:   false,
				entries:  []*Entry{{Key: splitKey}},
				children: []*Node{root, newNode},
			}
			t.root = newRoot
		}
		t.mutex.Unlock()
	}

	return nil
}

// insertIntoNode inserts a key-value pair into a node or its children
// Returns (nil, nil, false) if no split occurred, or (newNode, splitKey, true) if a split occurred
func (t *BLinkTree) insertIntoNode(node *Node, key []byte, value []byte) (*Node, []byte, bool) {
	node.mutex.Lock()

	// Check if we should follow the right link
	if node.highKey != nil && bytes.Compare(key, node.highKey) >= 0 {
		rightNode := node.rightLink
		node.mutex.Unlock()
		return t.insertIntoNode(rightNode, key, value)
	}

	// If it's a leaf node, insert the entry
	if node.isLeaf {
		// Use binary search to find the position to insert
		i := sort.Search(len(node.entries), func(i int) bool {
			return bytes.Compare(node.entries[i].Key, key) >= 0
		})

		// Check if the key already exists
		if i < len(node.entries) && bytes.Equal(node.entries[i].Key, key) {
			// Update the value
			node.entries[i].Value = value
			node.mutex.Unlock()
			return nil, nil, false
		}

		// Insert the new entry
		newEntry := &Entry{Key: key, Value: value}
		node.entries = append(node.entries, nil)
		copy(node.entries[i+1:], node.entries[i:])
		node.entries[i] = newEntry

		// Check if we need to split
		if len(node.entries) <= t.order {
			node.mutex.Unlock()
			return nil, nil, false
		}

		// Split the node
		return t.splitLeafNode(node)
	}

	// For internal nodes, find the appropriate child using binary search
	childIndex := sort.Search(len(node.entries), func(i int) bool {
		return bytes.Compare(node.entries[i].Key, key) > 0
	})

	childNode := node.children[childIndex]
	node.mutex.Unlock()

	// Insert into the child node
	newChild, splitKey, split := t.insertIntoNode(childNode, key, value)
	if !split {
		return nil, nil, false
	}

	// Handle the split
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Find the insert position for the new key using binary search
	pos := sort.Search(len(node.entries), func(i int) bool {
		return bytes.Compare(node.entries[i].Key, splitKey) >= 0
	})

	// Insert the new key and child
	node.entries = append(node.entries, nil)
	copy(node.entries[pos+1:], node.entries[pos:])
	node.entries[pos] = &Entry{Key: splitKey}

	node.children = append(node.children, nil)
	copy(node.children[pos+2:], node.children[pos+1:])
	node.children[pos+1] = newChild

	// Check if we need to split
	if len(node.entries) <= t.order {
		return nil, nil, false
	}

	// Split the internal node
	return t.splitInternalNode(node)
}

// splitLeafNode splits a leaf node and returns the new node and split key
func (t *BLinkTree) splitLeafNode(node *Node) (*Node, []byte, bool) {
	midIndex := len(node.entries) / 2
	splitKey := make([]byte, len(node.entries[midIndex].Key))
	copy(splitKey, node.entries[midIndex].Key)

	// Create the new node
	newNode := &Node{
		isLeaf:    true,
		entries:   append([]*Entry{}, node.entries[midIndex:]...),
		highKey:   node.highKey,
		rightLink: node.rightLink,
	}

	// Update the original node
	node.entries = node.entries[:midIndex]
	node.highKey = splitKey
	node.rightLink = newNode

	node.mutex.Unlock()
	return newNode, splitKey, true
}

// splitInternalNode splits an internal node and returns the new node and split key
func (t *BLinkTree) splitInternalNode(node *Node) (*Node, []byte, bool) {
	midIndex := len(node.entries) / 2
	splitKey := make([]byte, len(node.entries[midIndex].Key))
	copy(splitKey, node.entries[midIndex].Key)

	// Create the new node
	newNode := &Node{
		isLeaf:    false,
		entries:   append([]*Entry{}, node.entries[midIndex+1:]...),
		children:  append([]*Node{}, node.children[midIndex+1:]...),
		highKey:   node.highKey,
		rightLink: node.rightLink,
	}

	// Update the original node
	node.entries = node.entries[:midIndex]
	node.children = node.children[:midIndex+1]
	node.highKey = splitKey
	node.rightLink = newNode

	return newNode, splitKey, true
}

// Delete removes a key-value pair from the tree
func (t *BLinkTree) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	t.mutex.RLock()
	root := t.root
	t.mutex.RUnlock()

	return t.deleteFromNode(root, key)
}

// deleteFromNode deletes a key from a node or its children
func (t *BLinkTree) deleteFromNode(node *Node, key []byte) error {
	for {
		node.mutex.Lock()

		// Check if we should follow the right link
		if node.highKey != nil && bytes.Compare(key, node.highKey) >= 0 {
			rightNode := node.rightLink
			node.mutex.Unlock()
			node = rightNode
			continue
		}

		// If it's a leaf node, delete the key using binary search
		if node.isLeaf {
			i := sort.Search(len(node.entries), func(i int) bool {
				return bytes.Compare(node.entries[i].Key, key) >= 0
			})
			if i < len(node.entries) && bytes.Equal(node.entries[i].Key, key) {
				// Remove the entry
				copy(node.entries[i:], node.entries[i+1:])
				node.entries = node.entries[:len(node.entries)-1]
				node.mutex.Unlock()
				return nil
			}
			node.mutex.Unlock()
			return ErrKeyNotFound
		}

		// For internal nodes, find the appropriate child using binary search
		childIndex := sort.Search(len(node.entries), func(i int) bool {
			return bytes.Compare(node.entries[i].Key, key) > 0
		})

		childNode := node.children[childIndex]
		node.mutex.Unlock()
		node = childNode
	}
}

// RangeScan returns all key-value pairs in the specified key range [startKey, endKey)
func (t *BLinkTree) RangeScan(startKey, endKey []byte) ([]*Entry, error) {
	if len(startKey) == 0 {
		return nil, ErrEmptyKey
	}

	// If endKey is nil or empty, scan to the end
	scanToEnd := len(endKey) == 0

	t.mutex.RLock()
	root := t.root
	t.mutex.RUnlock()

	// Find the leaf node containing the start key
	node := t.findLeafNode(root, startKey)

	result := make([]*Entry, 0)

	for node != nil {
		node.mutex.RLock()

		// Collect entries within the range
		for _, entry := range node.entries {
			if bytes.Compare(entry.Key, startKey) >= 0 && (scanToEnd || bytes.Compare(entry.Key, endKey) < 0) {
				// Create a copy of the entry
				entryCopy := &Entry{
					Key:   make([]byte, len(entry.Key)),
					Value: make([]byte, len(entry.Value)),
				}
				copy(entryCopy.Key, entry.Key)
				copy(entryCopy.Value, entry.Value)
				result = append(result, entryCopy)
			}
		}

		// Check if we've reached the end of the range
		if !scanToEnd && (node.highKey == nil || bytes.Compare(node.highKey, endKey) >= 0) {
			node.mutex.RUnlock()
			break
		}

		// Move to the next leaf node
		rightLink := node.rightLink
		node.mutex.RUnlock()
		node = rightLink
	}

	return result, nil
}

// findLeafNode finds the leaf node that should contain the given key
func (t *BLinkTree) findLeafNode(node *Node, key []byte) *Node {
	for {
		node.mutex.RLock()

		// Check if we should follow the right link
		if node.highKey != nil && bytes.Compare(key, node.highKey) >= 0 {
			rightNode := node.rightLink
			node.mutex.RUnlock()
			node = rightNode
			continue
		}

		// If it's a leaf node, we found it
		if node.isLeaf {
			node.mutex.RUnlock()
			return node
		}

		// For internal nodes, find the appropriate child
		childIndex := 0
		for i := 0; i < len(node.entries); i++ {
			if bytes.Compare(key, node.entries[i].Key) >= 0 {
				childIndex = i + 1
			} else {
				break
			}
		}

		childNode := node.children[childIndex]
		node.mutex.RUnlock()
		node = childNode
	}
}
