package sort_tree

import (
	"mylsmtree/pkg/kv"
	"sync"
)

type TreeNode struct {
	KV kv.Value
	Left *TreeNode
	Right *TreeNode
}

type Tree struct {
	root *TreeNode
	count int
	rwLock *sync.RWMutex
}

func (tree *Tree) Init() {
	tree.rwLock = &sync.RWMutex{}
}

func (tree *Tree) GetCount() int {
	return tree.count
}

func (tree *Tree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.rwLock.RLock()
	defer tree.rwLock.RUnlock()

	if tree == nil {
		return kv.Value{}, kv.None
	}

	currentNode := tree.root
	for currentNode != nil {
		if key == currentNode.KV.Key {
			if currentNode.KV.Deleted == false {
				return currentNode.KV, kv.Success
			}else {
				return currentNode.KV, kv.Deleted
			}
		}
		if key < currentNode.KV.Key {
			currentNode = currentNode.Left;
		}else{
			currentNode = currentNode.Right;
		}
	}
	return kv.Value{}, kv.None
}

func (tree *Tree) Set(key string, value []byte) (*kv.Value, bool) {
	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()

	if tree == nil {
		return &kv.Value{}, false
	}

	current := tree.root

	newNode := &TreeNode{
		KV: kv.Value{
			Key: key,
			Value: value,
		},
	}

	if current == nil {
		tree.root = newNode
		tree.count ++
		return &kv.Value{}, false
	}

	for current != nil {
		if key == current.KV.Key {
			oldKV := current.KV.Copy()
			current.KV.Value = value
			current.KV.Deleted = false
			if oldKV.Deleted {
				return &kv.Value{}, false
			} else {
				return oldKV, true
			}
		}
		if key < current.KV.Key {
			if current.Left == nil {
				current.Left = newNode
				tree.count ++
				return &kv.Value{}, false
			}
			current = current.Left
		}else {
			if current.Right == nil {
				current.Right = newNode
				tree.count ++
				return &kv.Value{}, false
			}
			current = current.Right
		}
	}
	return &kv.Value{}, false
}

func (tree *Tree) Delete(key string) (oldValue kv.Value, hasOld bool) {
	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()

	if tree == nil {
		return kv.Value{}, false
	}

	newNode := &TreeNode{
		KV: kv.Value{
			Key: key,
			Value: nil,
			Deleted: true,
		},
	}

	currentNode := tree.root
	if currentNode == nil {
		tree.root = newNode
		return kv.Value{}, false
	}

	for currentNode != nil {
		if key == currentNode.KV.Key {
			if currentNode.KV.Deleted == false {
				oldKV := currentNode.KV.Copy()
				currentNode.KV.Value = nil
				currentNode.KV.Deleted = true
				tree.count --
				return *oldKV, true
			}else {
				return kv.Value{}, false
			}
		}
		if key < currentNode.KV.Key {
			if currentNode.Left == nil {
				currentNode.Left = newNode
				tree.count++
			}
			currentNode = currentNode.Left
		} else {
			if currentNode.Right == nil {
				currentNode.Right = newNode
				tree.count ++
			}
			currentNode = currentNode.Right
		}
	}
	return kv.Value{}, false
}

func (tree *Tree) GetValues() []kv.Value {
	tree.rwLock.RLock()
	defer tree.rwLock.RUnlock()

	stack := InitStack(tree.count)
	values := make([]kv.Value, tree.count)

	currentNode := tree.root
	for {
		if currentNode != nil {
			stack.Push(currentNode)
			currentNode = currentNode.Left
		}else {
			popNode, success := stack.Pop()
			if success == false {
				break
			}
			values = append(values, popNode.KV)
			currentNode = popNode.Right
		}
	}
	return values
}

func (tree *Tree) Swap() *Tree {
	tree.rwLock.Lock()
	defer tree.rwLock.Unlock()

	newTree := &Tree{}
	newTree.Init()
	newTree.root = tree.root
	tree.root = nil
	tree.count = 0
	return newTree
}










