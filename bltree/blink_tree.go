package bltree

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const maxKeys = 4 // Example maximum number of keys per node
const minKeys = maxKeys / 2

type Node struct {
	keys     []int
	children []*Node
	next     *Node
	parent   *Node
	isLeaf   bool
	lock     sync.RWMutex
}

type BLinkTree struct {
	root *Node
	lock sync.RWMutex
}

func NewBLinkTree() *BLinkTree {
	root := &Node{
		isLeaf:   true,
		keys:     make([]int, 0),
		children: make([]*Node, 0),
	}
	return &BLinkTree{
		root: root,
	}
}

func (tree *BLinkTree) Insert(key int) {
	// Add timeout to prevent indefinite hangs
	done := make(chan bool, 1)

	go func() {
		defer func() {
			// Recover from any panics to prevent goroutine leaks
			if r := recover(); r != nil {
				fmt.Printf("Recovered from panic: %v\n", r)
				done <- false
				return
			}
			done <- true
		}()

		tree.lock.RLock()
		root := tree.root
		tree.lock.RUnlock()

		node := root
		node.lock.Lock()

		// Properly track the path for unlocking in case of errors
		var path []*Node

		// Traverse to the leaf node
		for !node.isLeaf {
			i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
			if i >= len(node.children) {
				i = len(node.children) - 1
			}
			nextNode := node.children[i]
			nextNode.lock.Lock()
			path = append(path, node) // Add to path before moving on
			node = nextNode
		}

		// At this point, only the leaf node is locked (and we need to keep it locked)
		// Unlock all nodes in the path except current leaf node
		for _, pathNode := range path {
			pathNode.lock.Unlock()
		}

		// Check if key already exists
		i := sort.SearchInts(node.keys, key)
		if i < len(node.keys) && node.keys[i] == key {
			node.lock.Unlock() // Don't forget to unlock before returning
			return             // Key already exists, no need to insert
		}

		// Add key to the node
		node.keys = append(node.keys, 0) // Make space
		copy(node.keys[i+1:], node.keys[i:])
		node.keys[i] = key // Insert in sorted position

		// If the node is full, split it
		if len(node.keys) > maxKeys {
			tree.splitLeaf(node) // node is still locked here
		}

		node.lock.Unlock() // Unlock the leaf node
	}()

	select {
	case success := <-done:
		if !success {
			fmt.Printf("Insert failed for key %d\n", key)
		}
		return
	case <-time.After(5 * time.Second):
		fmt.Printf("Possible deadlock in Insert for key %d\n", key)
		return
	}
}

func (tree *BLinkTree) splitLeaf(node *Node) {
	mid := len(node.keys) / 2

	// Create new node with right half of keys
	newNode := &Node{
		keys:     make([]int, len(node.keys)-mid),
		isLeaf:   true,
		next:     node.next,
		parent:   node.parent,
		children: []*Node{},
	}

	// Copy keys to the new node
	copy(newNode.keys, node.keys[mid:])

	// Update the original node
	node.keys = node.keys[:mid]
	node.next = newNode

	// The first key in the new node is the key we'll push up
	promoteKey := newNode.keys[0]

	// Insert into parent
	tree.insertIntoParent(node, promoteKey, newNode)
}

func (tree *BLinkTree) splitInternal(node *Node) {
	// Node should be locked by caller
	node.lock.Lock() // But let's make sure

	mid := len(node.keys) / 2
	midKey := node.keys[mid]

	// Create a new node for the right half
	newNode := &Node{
		keys:     make([]int, len(node.keys)-mid-1),
		children: make([]*Node, len(node.children)-mid-1),
		isLeaf:   false,
		next:     node.next,
		parent:   node.parent,
	}

	// Copy keys and children to the new node
	copy(newNode.keys, node.keys[mid+1:])
	copy(newNode.children, node.children[mid+1:])

	// Update the original node
	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]
	node.next = newNode

	// Update parent pointers for all children in new node
	for _, child := range newNode.children {
		child.parent = newNode
	}

	// Unlock node before calling insertIntoParent to avoid lock ordering issues
	node.lock.Unlock()

	// Insert the middle key and new node into parent
	tree.insertIntoParent(node, midKey, newNode)
}

func (tree *BLinkTree) insertIntoParent(left *Node, key int, right *Node) {
	parent := left.parent

	if parent == nil {
		// Create a new root
		tree.lock.Lock()
		newRoot := &Node{
			keys:     []int{key},
			children: []*Node{left, right},
			isLeaf:   false,
		}
		left.parent = newRoot
		right.parent = newRoot
		tree.root = newRoot
		tree.lock.Unlock()
		return
	}

	// Get lock on parent
	parent.lock.Lock()
	// REMOVE the defer - we need explicit unlocking based on conditions

	// Find the position to insert the key
	pos := 0
	for pos < len(parent.keys) && parent.keys[pos] < key {
		pos++
	}

	// Insert the key
	parent.keys = append(parent.keys, 0) // Make space
	copy(parent.keys[pos+1:], parent.keys[pos:])
	parent.keys[pos] = key

	// Find position for child pointer
	childPos := 0
	for childPos < len(parent.children) && parent.children[childPos] != left {
		childPos++
	}
	if childPos == len(parent.children) {
		// This should not happen in a correctly structured tree
		parent.lock.Unlock() // Don't forget to unlock!
		return
	}

	// Insert the child pointer after left's position
	parent.children = append(parent.children, nil) // Make space
	copy(parent.children[childPos+2:], parent.children[childPos+1:])
	parent.children[childPos+1] = right

	// Update the right node's parent
	right.parent = parent

	// If the parent node is full, split it
	if len(parent.keys) > maxKeys {
		// Need to release the lock BEFORE potentially recursive calls
		// to avoid lock ordering issues
		parentCopy := parent           // Save reference
		parent.lock.Unlock()           // Unlock before recursive call
		tree.splitInternal(parentCopy) // Now safe to call
	} else {
		parent.lock.Unlock() // Always unlock at end
	}
}

func (tree *BLinkTree) Get(key int) bool {
	tree.lock.RLock()
	root := tree.root
	tree.lock.RUnlock()

	if root == nil {
		return false
	}

	node := root
	node.lock.RLock() // Lock for reading

	// Find the leaf node that may contain the key
	for !node.isLeaf {
		i := 0
		for i < len(node.keys) && key >= node.keys[i] {
			i++
		}

		if i >= len(node.children) {
			if len(node.children) == 0 {
				node.lock.RUnlock() // Make sure to unlock before returning
				return false
			}
			i = len(node.children) - 1
		}

		nextNode := node.children[i]
		nextNode.lock.RLock()
		node.lock.RUnlock() // Release previous lock
		node = nextNode
	}

	// At this point, node is a leaf and we have a read lock on it
	i := sort.SearchInts(node.keys, key)
	result := i < len(node.keys) && node.keys[i] == key
	node.lock.RUnlock() // Release the lock before returning
	return result
}

func (tree *BLinkTree) Delete(key int) {
	// Add timeout to prevent indefinite hangs like in Insert
	done := make(chan bool, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Recovered from panic in Delete: %v\n", r)
				done <- false
				return
			}
			done <- true
		}()

		tree.lock.RLock()
		root := tree.root
		tree.lock.RUnlock()

		if root == nil {
			done <- true
			return
		}

		node := root
		node.lock.Lock()

		var path []*Node

		// Find the leaf node that contains the key - USE SAME TRAVERSAL AS GET
		for !node.isLeaf {
			i := 0
			for i < len(node.keys) && key >= node.keys[i] {
				i++
			}

			if i >= len(node.children) {
				i = len(node.children) - 1
			}

			nextNode := node.children[i]
			nextNode.lock.Lock()
			path = append(path, node)
			node = nextNode
		}

		// Unlock path nodes
		for _, pathNode := range path {
			pathNode.lock.Unlock()
		}

		// Node is now a leaf and is locked
		// Find the key in the leaf node
		i := sort.SearchInts(node.keys, key)
		if i < len(node.keys) && node.keys[i] == key {
			// Remove the key
			node.keys = append(node.keys[:i], node.keys[i+1:]...)

			// Check if the node needs to be rebalanced
			if len(node.keys) < minKeys && node.parent != nil {
				tree.handleUnderflow(node)
			}
		}

		node.lock.Unlock()
	}()

	select {
	case <-done:
		return
	case <-time.After(5 * time.Second):
		fmt.Printf("Possible deadlock in Delete for key %d\n", key)
		return
	}
}

func (tree *BLinkTree) handleUnderflow(node *Node) {
	if len(node.keys) >= minKeys {
		return
	}

	parent := node.parent
	if parent == nil {
		// Root node case - nothing to do
		return
	}

	// Lock the parent before examining siblings
	parent.lock.Lock()

	// Find node index in parent
	nodeIndex := -1
	for i, child := range parent.children {
		if child == node {
			nodeIndex = i
			break
		}
	}

	if nodeIndex == -1 {
		// Node not found in parent's children - shouldn't happen
		parent.lock.Unlock()
		return
	}

	var leftSibling, rightSibling *Node
	var leftLocked, rightLocked bool

	// Try to borrow from siblings or merge
	if nodeIndex > 0 {
		leftSibling = parent.children[nodeIndex-1]
		leftSibling.lock.Lock()
		leftLocked = true

		// Borrow from left sibling if possible
		if len(leftSibling.keys) > minKeys {
			// Move key from left sibling through parent to node
			node.keys = append([]int{parent.keys[nodeIndex-1]}, node.keys...)
			parent.keys[nodeIndex-1] = leftSibling.keys[len(leftSibling.keys)-1]
			leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]

			if !node.isLeaf && len(leftSibling.children) > 0 {
				child := leftSibling.children[len(leftSibling.children)-1]
				child.parent = node
				node.children = append([]*Node{child}, node.children...)
				leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
			}

			if leftLocked {
				leftSibling.lock.Unlock()
			}
			parent.lock.Unlock()
			return
		}
	}

	if nodeIndex < len(parent.children)-1 {
		rightSibling = parent.children[nodeIndex+1]
		rightSibling.lock.Lock()
		rightLocked = true

		// Borrow from right sibling if possible
		if len(rightSibling.keys) > minKeys {
			// Move key from right sibling through parent to node
			node.keys = append(node.keys, parent.keys[nodeIndex])
			parent.keys[nodeIndex] = rightSibling.keys[0]
			rightSibling.keys = rightSibling.keys[1:]

			if !node.isLeaf && len(rightSibling.children) > 0 {
				child := rightSibling.children[0]
				child.parent = node
				node.children = append(node.children, child)
				rightSibling.children = rightSibling.children[1:]
			}

			if leftLocked {
				leftSibling.lock.Unlock()
			}
			if rightLocked {
				rightSibling.lock.Unlock()
			}
			parent.lock.Unlock()
			return
		}
	}

	// If we reach here, we need to merge nodes
	if leftSibling != nil {
		// Merge node into left sibling
		if !node.isLeaf {
			leftSibling.keys = append(leftSibling.keys, parent.keys[nodeIndex-1])
			parent.keys = append(parent.keys[:nodeIndex-1], parent.keys[nodeIndex:]...)
			for _, child := range node.children {
				child.parent = leftSibling
			}
			leftSibling.children = append(leftSibling.children, node.children...)
		} else {
			leftSibling.keys = append(leftSibling.keys, node.keys...)
			parent.keys = append(parent.keys[:nodeIndex-1], parent.keys[nodeIndex:]...)
		}
		leftSibling.next = node.next
		parent.children = append(parent.children[:nodeIndex], parent.children[nodeIndex+1:]...)
	} else if rightSibling != nil {
		// Merge right sibling into node
		if !node.isLeaf {
			node.keys = append(node.keys, parent.keys[nodeIndex])
			node.keys = append(node.keys, rightSibling.keys...)
			parent.keys = append(parent.keys[:nodeIndex], parent.keys[nodeIndex+1:]...)
			for _, child := range rightSibling.children {
				child.parent = node
			}
			node.children = append(node.children, rightSibling.children...)
		} else {
			node.keys = append(node.keys, rightSibling.keys...)
			parent.keys = append(parent.keys[:nodeIndex], parent.keys[nodeIndex+1:]...)
		}
		node.next = rightSibling.next
		parent.children = append(parent.children[:nodeIndex+1], parent.children[nodeIndex+2:]...)
	}

	// Release locks before recursive calls
	if leftLocked {
		leftSibling.lock.Unlock()
	}
	if rightLocked {
		rightSibling.lock.Unlock()
	}

	// Check if parent needs rebalancing
	needRebalance := len(parent.keys) < minKeys && parent.parent != nil
	parent.lock.Unlock()

	if needRebalance {
		tree.handleUnderflow(parent)
	}
}

func (tree *BLinkTree) RangeScan(start, end int) []int {
	tree.lock.RLock()
	root := tree.root
	tree.lock.RUnlock()

	var result []int
	node := root
	node.lock.RLock()

	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= start })
		if i >= len(node.children) {
			i = len(node.children) - 1
		}
		nextNode := node.children[i]
		nextNode.lock.RLock()
		node.lock.RUnlock()
		node = nextNode
	}

	// Process the current node and move to next nodes, properly releasing locks
	for node != nil {
		for _, key := range node.keys {
			if key >= start && key <= end {
				result = append(result, key)
			}
			if key > end {
				break
			}
		}

		next := node.next
		if next == nil {
			node.lock.RUnlock()
			break
		}

		next.lock.RLock()
		node.lock.RUnlock()
		node = next
	}

	return result
}
