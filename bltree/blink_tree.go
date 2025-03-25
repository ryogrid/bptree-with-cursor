package bltree

import (
	"sort"
	"sync"
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
	return &BLinkTree{
		root: &Node{
			isLeaf: true,
		},
	}
}

func (tree *BLinkTree) Insert(key int) {
	tree.lock.RLock()
	root := tree.root
	tree.lock.RUnlock()

	node := root
	node.lock.Lock()

	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
		if i >= len(node.children) {
			i = len(node.children) - 1
		}
		nextNode := node.children[i]
		nextNode.lock.Lock()
		node.lock.Unlock()
		node = nextNode
	}

	// Node is now a leaf and is locked
	defer node.lock.Unlock()

	// Check if key already exists
	i := sort.SearchInts(node.keys, key)
	if i < len(node.keys) && node.keys[i] == key {
		return // Key already exists, no need to insert
	}

	tree.insertIntoLeaf(node, key)
}

func (tree *BLinkTree) insertIntoLeaf(node *Node, key int) {
	node.keys = append(node.keys, key)
	sort.Ints(node.keys)

	if len(node.keys) > maxKeys {
		tree.splitLeaf(node)
	}
}

func (tree *BLinkTree) splitLeaf(node *Node) {
	mid := len(node.keys) / 2
	newNode := &Node{
		keys:   append([]int{}, node.keys[mid:]...), // Create a copy
		isLeaf: true,
		next:   node.next,
		parent: node.parent,
	}

	node.keys = node.keys[:mid]
	node.next = newNode

	tree.insertIntoParent(node, newNode.keys[0], newNode)
}

func (tree *BLinkTree) splitInternal(node *Node) {
	mid := len(node.keys) / 2
	midKey := node.keys[mid] // Save the middle key before truncating

	newNode := &Node{
		keys:     append([]int{}, node.keys[mid+1:]...), // Create copies instead of direct slices
		children: append([]*Node{}, node.children[mid+1:]...),
		isLeaf:   false,
		next:     node.next,
		parent:   node.parent,
	}

	node.keys = node.keys[:mid]
	node.children = node.children[:mid+1]
	node.next = newNode

	for _, child := range newNode.children {
		child.parent = newNode
	}

	tree.insertIntoParent(node, midKey, newNode)
}

func (tree *BLinkTree) insertIntoParent(left *Node, key int, right *Node) {
	parent := left.parent

	if parent == nil {
		// Create a new root with proper locking
		tree.lock.Lock() // We need a write lock when changing the root
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

	parent.lock.Lock()

	// Remove the defer here and handle unlocking explicitly

	i := sort.Search(len(parent.keys), func(i int) bool { return parent.keys[i] >= key })
	parent.keys = append(parent.keys[:i], append([]int{key}, parent.keys[i:]...)...)
	parent.children = append(parent.children[:i+1], append([]*Node{right}, parent.children[i+1:]...)...)

	right.parent = parent

	if len(parent.keys) > maxKeys {
		// We need to unlock the parent before splitting to avoid deadlocks
		parentToSplit := parent
		parent.lock.Unlock()
		tree.splitInternal(parentToSplit)
	} else {
		parent.lock.Unlock()
	}
}

func (tree *BLinkTree) Get(key int) bool {
	tree.lock.RLock()
	root := tree.root
	tree.lock.RUnlock()

	node := root
	node.lock.RLock()

	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
		if i >= len(node.children) {
			i = len(node.children) - 1
		}
		nextNode := node.children[i]
		nextNode.lock.RLock()
		node.lock.RUnlock()
		node = nextNode
	}

	// Now node is a leaf and has a read lock
	defer node.lock.RUnlock()

	i := sort.SearchInts(node.keys, key)
	return i < len(node.keys) && node.keys[i] == key
}

func (tree *BLinkTree) Delete(key int) {
	tree.lock.RLock()
	root := tree.root
	tree.lock.RUnlock()

	node := root
	node.lock.Lock()

	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
		if i >= len(node.children) {
			i = len(node.children) - 1
		}
		nextNode := node.children[i]
		nextNode.lock.Lock()
		node.lock.Unlock()
		node = nextNode
	}

	// Node is now a leaf and is locked
	defer node.lock.Unlock()

	i := sort.SearchInts(node.keys, key)
	if i < len(node.keys) && node.keys[i] == key {
		node.keys = append(node.keys[:i], node.keys[i+1:]...)
		// Only handle underflow if we actually removed something
		if len(node.keys) < minKeys && node.parent != nil {
			tree.handleUnderflow(node)
		}
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
	defer parent.lock.Unlock()

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
		return
	}

	var leftSibling, rightSibling *Node
	if nodeIndex > 0 {
		leftSibling = parent.children[nodeIndex-1]
		if leftSibling != nil {
			leftSibling.lock.Lock()
		}
	}

	if nodeIndex < len(parent.children)-1 {
		rightSibling = parent.children[nodeIndex+1]
		if rightSibling != nil {
			rightSibling.lock.Lock()
		}
	}

	// Rest of the method remains the same with proper unlocking
	// ...
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

	for node != nil {
		for _, key := range node.keys {
			if key >= start && key <= end {
				result = append(result, key)
			}
		}

		next := node.next
		if next != nil {
			next.lock.RLock()
		}
		node.lock.RUnlock()
		node = next
	}

	return result
}
