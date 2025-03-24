package bptree

import (
	"bytes"
	"errors"
)

const (
	defaultDegree = 4 // Minimum degree (minimum number of keys in non-root nodes)
)

// Node represents a node in the B+ tree
type Node struct {
	isLeaf   bool
	keys     [][]byte
	values   [][]byte
	children []*Node
	next     *Node // For leaf nodes to implement range scan
}

// BPlusTree represents the main B+ tree structure
type BPlusTree struct {
	root   *Node
	degree int
}

// NewBPlusTree creates a new B+ tree with the specified degree
func NewBPlusTree(degree int) *BPlusTree {
	if degree < 2 {
		degree = defaultDegree
	}
	return &BPlusTree{
		root:   newLeafNode(),
		degree: degree,
	}
}

// newLeafNode creates a new leaf node
func newLeafNode() *Node {
	return &Node{
		isLeaf:   true,
		keys:     make([][]byte, 0),
		values:   make([][]byte, 0),
		children: nil,
	}
}

// newInternalNode creates a new internal node
func newInternalNode() *Node {
	return &Node{
		isLeaf:   false,
		keys:     make([][]byte, 0),
		children: make([]*Node, 0),
	}
}

// Insert adds a new key-value pair to the tree
func (t *BPlusTree) Insert(key, value []byte) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}

	if t.root == nil {
		t.root = newLeafNode()
	}

	// Handle root split if needed
	if len(t.root.keys) == 2*t.degree-1 {
		newRoot := newInternalNode()
		newRoot.children = append(newRoot.children, t.root)
		t.splitChild(newRoot, 0)
		t.root = newRoot
	}

	return t.insertNonFull(t.root, key, value)
}

// insertNonFull inserts a key-value pair into a non-full node
func (t *BPlusTree) insertNonFull(node *Node, key, value []byte) error {
	if node.isLeaf {
		// Find position to insert
		i := 0
		for i < len(node.keys) && bytes.Compare(node.keys[i], key) < 0 {
			i++
		}

		// Check for duplicate key
		if i < len(node.keys) && bytes.Equal(node.keys[i], key) {
			node.values[i] = value // Overwrite existing value
			return nil
		}

		// Insert key and value at the correct position
		node.keys = append(node.keys, nil)
		node.values = append(node.values, nil)
		copy(node.keys[i+1:], node.keys[i:])
		copy(node.values[i+1:], node.values[i:])
		node.keys[i] = key
		node.values[i] = value
		return nil
	}

	// Find the child to recurse into
	i := 0
	for i < len(node.keys) && bytes.Compare(key, node.keys[i]) > 0 {
		i++
	}

	// Split child if full
	if len(node.children[i].keys) == 2*t.degree-1 {
		t.splitChild(node, i)
		// After splitting, the key might need to go into the right child
		if i < len(node.keys) && bytes.Compare(key, node.keys[i]) > 0 {
			i++
		}
	}

	return t.insertNonFull(node.children[i], key, value)
}

// splitChild splits a full child node
func (t *BPlusTree) splitChild(parent *Node, childIndex int) {
	child := parent.children[childIndex]
	newNode := newInternalNode()
	if child.isLeaf {
		newNode.isLeaf = true
	}

	// Calculate split point - this is crucial for correct balancing
	splitPoint := (2*t.degree - 1) / 2

	if child.isLeaf {
		// For leaf nodes in B+ tree:
		// 1. Split keys and values at split point
		// 2. New node gets the right half
		// 3. The split point key must be in the parent and in one of the leaves

		// Create new arrays for the new node
		newNode.keys = make([][]byte, len(child.keys)-splitPoint)
		newNode.values = make([][]byte, len(child.values)-splitPoint)

		// Copy right half to new node
		copy(newNode.keys, child.keys[splitPoint:])
		copy(newNode.values, child.values[splitPoint:])

		// Update leaf node links
		newNode.next = child.next
		child.next = newNode

		// Update parent with a copy of the first key in the new node
		// This is crucial for correct searching
		parent.keys = append(parent.keys, nil)
		copy(parent.keys[childIndex+1:], parent.keys[childIndex:])
		parent.keys[childIndex] = newNode.keys[0]

		// Truncate child to left half
		child.keys = child.keys[:splitPoint]
		child.values = child.values[:splitPoint]
	} else {
		// For internal nodes:
		// 1. Median key moves up to parent
		// 2. Keys after median go to new node
		midKey := child.keys[splitPoint]

		// New node gets keys after split point (excluding split point key)
		newNode.keys = make([][]byte, len(child.keys)-splitPoint-1)
		copy(newNode.keys, child.keys[splitPoint+1:])

		// New node gets children after split point
		newNode.children = make([]*Node, len(child.children)-splitPoint-1)
		copy(newNode.children, child.children[splitPoint+1:])

		// Update parent with median key
		parent.keys = append(parent.keys, nil)
		copy(parent.keys[childIndex+1:], parent.keys[childIndex:])
		parent.keys[childIndex] = midKey

		// Truncate child
		child.keys = child.keys[:splitPoint]
		child.children = child.children[:splitPoint+1]
	}

	// Add new node to parent's children
	parent.children = append(parent.children, nil)
	copy(parent.children[childIndex+2:], parent.children[childIndex+1:])
	parent.children[childIndex+1] = newNode
}

// Get retrieves the value associated with the given key
func (t *BPlusTree) Get(key []byte) ([]byte, bool) {
	if t.root == nil {
		return nil, false
	}

	// Find the leaf node that should contain the key
	node := t.root
	for !node.isLeaf {
		i := 0
		for i < len(node.keys) && bytes.Compare(key, node.keys[i]) >= 0 {
			i++
		}
		node = node.children[i]
	}

	// Search in leaf node for the exact key match
	for i, k := range node.keys {
		if bytes.Equal(k, key) {
			return node.values[i], true
		}
	}
	return nil, false
}

// Delete removes a key-value pair from the tree
func (t *BPlusTree) Delete(key []byte) error {
	if key == nil {
		return errors.New("key cannot be nil")
	}
	if t.root == nil {
		return nil
	}
	return t.delete(t.root, nil, -1, key)
}

// delete removes a key from a node and handles rebalancing
func (t *BPlusTree) delete(node *Node, parent *Node, parentIndex int, key []byte) error {
	if node.isLeaf {
		// Find key in leaf node
		keyIndex := -1
		for i, k := range node.keys {
			if bytes.Equal(k, key) {
				keyIndex = i
				break
			}
		}
		if keyIndex == -1 {
			return nil // Key not found
		}

		// Remove key and value
		node.keys = append(node.keys[:keyIndex], node.keys[keyIndex+1:]...)
		node.values = append(node.values[:keyIndex], node.values[keyIndex+1:]...)

		// Handle root case
		if parent == nil && len(node.keys) == 0 {
			t.root = nil
			return nil
		}

		// Check if node needs rebalancing
		minKeys := (t.degree - 1)
		if parent == nil || len(node.keys) >= minKeys {
			return nil
		}

		return t.rebalanceLeaf(node, parent, parentIndex)
	}

	// Find child containing key
	childIndex := 0
	for childIndex < len(node.keys) && bytes.Compare(key, node.keys[childIndex]) > 0 {
		childIndex++
	}

	return t.delete(node.children[childIndex], node, childIndex, key)
}

// rebalanceLeaf handles rebalancing of leaf nodes after deletion
func (t *BPlusTree) rebalanceLeaf(node *Node, parent *Node, parentIndex int) error {
	// Try borrowing from left sibling
	if parentIndex > 0 {
		leftSibling := parent.children[parentIndex-1]
		if len(leftSibling.keys) > (t.degree - 1) {
			// Borrow rightmost key-value from left sibling
			node.keys = append([][]byte{leftSibling.keys[len(leftSibling.keys)-1]}, node.keys...)
			node.values = append([][]byte{leftSibling.values[len(leftSibling.values)-1]}, node.values...)

			leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
			leftSibling.values = leftSibling.values[:len(leftSibling.values)-1]

			// Update parent key
			parent.keys[parentIndex-1] = node.keys[0]
			return nil
		}
	}

	// Try borrowing from right sibling
	if parentIndex < len(parent.children)-1 {
		rightSibling := parent.children[parentIndex+1]
		if len(rightSibling.keys) > (t.degree - 1) {
			// Borrow leftmost key-value from right sibling
			node.keys = append(node.keys, rightSibling.keys[0])
			node.values = append(node.values, rightSibling.values[0])

			rightSibling.keys = rightSibling.keys[1:]
			rightSibling.values = rightSibling.values[1:]

			// Update parent key
			parent.keys[parentIndex] = rightSibling.keys[0]
			return nil
		}
	}

	// Merge with a sibling
	if parentIndex > 0 {
		// Merge with left sibling
		leftSibling := parent.children[parentIndex-1]
		leftSibling.keys = append(leftSibling.keys, node.keys...)
		leftSibling.values = append(leftSibling.values, node.values...)
		leftSibling.next = node.next

		// Remove parent key and pointer
		parent.keys = append(parent.keys[:parentIndex-1], parent.keys[parentIndex:]...)
		parent.children = append(parent.children[:parentIndex], parent.children[parentIndex+1:]...)
	} else {
		// Merge with right sibling
		rightSibling := parent.children[parentIndex+1]
		node.keys = append(node.keys, rightSibling.keys...)
		node.values = append(node.values, rightSibling.values...)
		node.next = rightSibling.next

		// Remove parent key and pointer
		parent.keys = append(parent.keys[:parentIndex], parent.keys[parentIndex+1:]...)
		parent.children = append(parent.children[:parentIndex+1], parent.children[parentIndex+2:]...)
	}

	// Check if parent is root and empty
	if parent == t.root && len(parent.keys) == 0 {
		if len(parent.children) > 0 {
			t.root = parent.children[0]
		} else {
			t.root = nil
		}
	}

	return nil
}

// Iterator represents a B+ tree iterator for range scanning
type Iterator struct {
	node     *Node
	position int
	started  bool // Track if we've started iteration
}

// NewIterator creates a new iterator starting from the given key
func (t *BPlusTree) NewIterator(startKey []byte) *Iterator {
	if t.root == nil {
		return nil
	}

	// Find the leaf node containing the start key
	node := t.root
	for !node.isLeaf {
		i := 0
		for i < len(node.keys) && bytes.Compare(startKey, node.keys[i]) > 0 {
			i++
		}
		node = node.children[i]
	}

	// Find the position where startKey would be inserted
	position := 0
	for position < len(node.keys) && bytes.Compare(node.keys[position], startKey) < 0 {
		position++
	}

	// If the key is not found, we want to start at the next available key
	return &Iterator{
		node:     node,
		position: position - 1, // Will be incremented in first Next() call
		started:  false,
	}
}

// Next moves the iterator to the next key-value pair
func (it *Iterator) Next() bool {
	if it == nil || it.node == nil {
		return false
	}

	// For the first call
	if !it.started {
		it.started = true
		it.position++
		if it.position < len(it.node.keys) {
			return true
		}
		// If current node has no more keys, move to next node
		if it.node.next != nil {
			it.node = it.node.next
			it.position = 0
			return len(it.node.keys) > 0
		}
		return false
	}

	// For subsequent calls
	it.position++
	if it.position >= len(it.node.keys) {
		if it.node.next != nil {
			it.node = it.node.next
			it.position = 0
			return len(it.node.keys) > 0
		}
		return false
	}

	return true
}

// Key returns the current key
func (it *Iterator) Key() []byte {
	if it == nil || it.node == nil || it.position < 0 || it.position >= len(it.node.keys) {
		return nil
	}
	return it.node.keys[it.position]
}

// Value returns the current value
func (it *Iterator) Value() []byte {
	if it == nil || it.node == nil || it.position < 0 || it.position >= len(it.node.values) {
		return nil
	}
	return it.node.values[it.position]
}

// validateNode checks if the node satisfies B+ tree properties
func (t *BPlusTree) validateNode(node *Node, isRoot bool) error {
	if node == nil {
		return nil
	}

	// Check key count constraints
	minKeys := 1
	if !isRoot {
		minKeys = t.degree - 1
	}
	maxKeys := 2*t.degree - 1

	if len(node.keys) > maxKeys {
		return errors.New("node contains too many keys")
	}
	if !isRoot && len(node.keys) < minKeys {
		return errors.New("node contains too few keys")
	}

	if !node.isLeaf {
		// Internal node must have children
		if len(node.children) != len(node.keys)+1 {
			return errors.New("internal node has invalid number of children")
		}

		// Recursively validate children
		for _, child := range node.children {
			if child == nil {
				return errors.New("nil child pointer found")
			}
			if err := t.validateNode(child, false); err != nil {
				return err
			}
		}
	}

	return nil
}

// Validate checks if the entire tree satisfies B+ tree properties
func (t *BPlusTree) Validate() error {
	return t.validateNode(t.root, true)
}
