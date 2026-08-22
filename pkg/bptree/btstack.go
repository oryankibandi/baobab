package bptree

import (
	"log"
)

// BTStack stores the path used to traverse the BTree so that incase of splits
// and merges that propagate from child nodes the parent can be retrieved
// from the stack. Without this we would require recursion, which may increase
// stack memory usage.
// This is also implemented by Postgres -> https://github.com/postgres/postgres/blob/REL_12_STABLE/src/include/access/nbtree.h#L405-L425

type TraversePath struct {
	pid uint32
	// index followed to get to child node
	idx uint32
	// this is the height of the node. root node is 0
	height uint32
	// seperator key used to get to this node. Used during merges/rebalances
	sepKey uint32
}

type BTStack struct {
	Count int
	stack map[int]*TraversePath // ID and value. Id is incremented monotonically from 0
	maxId int
	root  uint32
}

func (bt *BTStack) Add(n *TraversePath) (bool, error) {
	newKey := bt.maxId + 1

	bt.stack[newKey] = n

	bt.maxId++
	bt.Count++

	log.Println("Added Item.")

	return true, nil
}

func (bt *BTStack) Pop() *TraversePath {
	if bt.Count <= 0 || bt.maxId == 0 {
		return nil
	}

	k := bt.maxId
	v, ok := bt.stack[k]

	if !ok {
		return nil
	}

	delete(bt.stack, k)

	if bt.Count > 0 {
		bt.Count--
	}
	bt.maxId--

	return v
}

func (bt *BTStack) Clear() (bool, error) {
	if bt.Count <= 0 {
		return true, nil
	}

	for k := range bt.stack {
		if k != 0 {
			delete(bt.stack, k)
		}
	}

	bt.Count = 0
	bt.maxId = 0

	return true, nil
}

// Returns the immediate parent.
func (bt *BTStack) Parent() (*TraversePath, error) {
	if bt.Count <= 0 || bt.maxId == 0 {
		return nil, BTreeError{Message: "No items in stack"}
	}

	k := bt.maxId
	v, ok := bt.stack[k]

	if !ok {
		return nil, BTreeError{Message: "No items in stack"}
	}

	return v, nil
}

func NewBTStack(root uint32) (*BTStack, error) {
	st := BTStack{
		root:  root,
		stack: make(map[int]*TraversePath),
	}

	return &st, nil
}
