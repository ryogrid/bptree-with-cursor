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
	defer tree.lock.RUnlock()

	node := tree.root
	node.lock.Lock()
	// Remove the defer here as we'll handle unlocking in the loop

	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
		nextNode := node.children[i]
		nextNode.lock.Lock()
		node.lock.Unlock()
		node = nextNode
	}

	// Node is now a leaf and is locked
	defer node.lock.Unlock() // Add defer here for the leaf node
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
		keys:   node.keys[mid:],
		isLeaf: true,
		next:   node.next,
		parent: node.parent,
	}

	node.keys = node.keys[:mid]
	node.next = newNode

	tree.insertIntoParent(node, newNode.keys[0], newNode)
}

func (tree *BLinkTree) insertIntoInternal(node *Node, key int) {
	i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
	child := node.children[i]

	child.lock.Lock()
	defer child.lock.Unlock()

	if child.isLeaf {
		tree.insertIntoLeaf(child, key)
	} else {
		tree.insertIntoInternal(child, key)
	}

	if len(child.keys) > maxKeys {
		tree.splitInternal(child)
	}
}

func (tree *BLinkTree) splitInternal(node *Node) {
	mid := len(node.keys) / 2
	midKey := node.keys[mid] // Save the middle key before truncating

	newNode := &Node{
		keys:     node.keys[mid+1:],
		children: node.children[mid+1:],
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

	tree.insertIntoParent(node, midKey, newNode) // Use the stored midKey instead of node.keys[mid]
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
	defer parent.lock.Unlock()

	i := sort.Search(len(parent.keys), func(i int) bool { return parent.keys[i] >= key })
	parent.keys = append(parent.keys[:i], append([]int{key}, parent.keys[i:]...)...)
	parent.children = append(parent.children[:i+1], append([]*Node{right}, parent.children[i+1:]...)...)

	right.parent = parent

	if len(parent.keys) > maxKeys {
		// We need to unlock the parent before splitting to avoid deadlocks
		// in the recursive calls to insertIntoParent
		parentToSplit := parent
		parent.lock.Unlock()
		tree.splitInternal(parentToSplit)
		return // Added return to prevent the deferred unlock from executing
	}
}

func (tree *BLinkTree) Get(key int) bool {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	node := tree.root
	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
		node = node.children[i]
	}

	i := sort.SearchInts(node.keys, key)
	return i < len(node.keys) && node.keys[i] == key
}

func (tree *BLinkTree) Delete(key int) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	node := tree.root
	node.lock.Lock()
	// Remove the defer here as we'll handle unlocking in the loop

	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= key })
		nextNode := node.children[i]
		nextNode.lock.Lock()
		node.lock.Unlock()
		node = nextNode
	}

	// Node is now a leaf and is locked
	defer node.lock.Unlock() // Add defer here for the leaf node
	i := sort.SearchInts(node.keys, key)
	if i < len(node.keys) && node.keys[i] == key {
		node.keys = append(node.keys[:i], node.keys[i+1:]...)
		tree.handleUnderflow(node)
	}
}

func (tree *BLinkTree) handleUnderflow(node *Node) {
	if len(node.keys) >= minKeys {
		return
	}

	parent := node.parent
	if parent == nil {
		if len(node.keys) == 0 && len(node.children) > 0 {
			tree.root = node.children[0]
			tree.root.parent = nil
		}
		return
	}

	nodeIndex := 0
	for i, child := range parent.children {
		if child == node {
			nodeIndex = i
			break
		}
	}

	var leftSibling, rightSibling *Node
	if nodeIndex > 0 {
		leftSibling = parent.children[nodeIndex-1]
	}
	if nodeIndex < len(parent.children)-1 {
		rightSibling = parent.children[nodeIndex+1]
	}

	if leftSibling != nil && len(leftSibling.keys) > minKeys {
		node.keys = append([]int{parent.keys[nodeIndex-1]}, node.keys...)
		parent.keys[nodeIndex-1] = leftSibling.keys[len(leftSibling.keys)-1]
		leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
	} else if rightSibling != nil && len(rightSibling.keys) > minKeys {
		node.keys = append(node.keys, parent.keys[nodeIndex])
		parent.keys[nodeIndex] = rightSibling.keys[0]
		rightSibling.keys = rightSibling.keys[1:]
	} else {
		if leftSibling != nil {
			leftSibling.keys = append(leftSibling.keys, parent.keys[nodeIndex-1])
			leftSibling.keys = append(leftSibling.keys, node.keys...)
			leftSibling.next = node.next
			parent.keys = append(parent.keys[:nodeIndex-1], parent.keys[nodeIndex:]...)
			parent.children = append(parent.children[:nodeIndex], parent.children[nodeIndex+1:]...)
		} else if rightSibling != nil {
			node.keys = append(node.keys, parent.keys[nodeIndex])
			node.keys = append(node.keys, rightSibling.keys...)
			node.next = rightSibling.next
			parent.keys = append(parent.keys[:nodeIndex], parent.keys[nodeIndex+1:]...)
			parent.children = append(parent.children[:nodeIndex+1], parent.children[nodeIndex+2:]...)
		}

		if len(parent.keys) < minKeys {
			tree.handleUnderflow(parent)
		}
	}
}

func (tree *BLinkTree) RangeScan(start, end int) []int {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	var result []int
	node := tree.root
	for !node.isLeaf {
		i := sort.Search(len(node.keys), func(i int) bool { return node.keys[i] >= start })
		node = node.children[i]
	}

	for node != nil {
		for _, key := range node.keys {
			if key >= start && key <= end {
				result = append(result, key)
			}
		}
		node = node.next
	}

	return result
}
