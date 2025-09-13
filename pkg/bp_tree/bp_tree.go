package bp_tree

import (
	"cmp"
	"encoding/binary"
	"fmt"

	"github.com/oryankibandi/on_disk_btree/pkg/disk_io"
)

type BTree[K cmp.Ordered] struct {
	Root   *Node[K]
	Degree int
}

type Node[K cmp.Ordered] struct {
	Keys     []K      // Keys in the Node
	Children []int32  // Internal Node children
	Leaf     bool     // Whether Node is leaf
	Values   [][]byte // Values in leaf node
	// LeftSibling  int32   // Left sibling in leaf node
	// RightSibling int32   // right sibling in leaf node
	Page *diskio.Page // Associated on disk page
}

const (
	DEGREE = 2
)

// Create new node - does not include associated page
func createNode[T cmp.Ordered](separatorKeys []T, isLeaf bool, isRoot bool) (*Node[T], error) {
	if (len(separatorKeys) > (DEGREE*2) || len(separatorKeys) < DEGREE) && !isLeaf && !isRoot {
		msg := fmt.Sprintf("Keys must be from %d to %d", DEGREE, 2*DEGREE)
		return nil, BTreeError{Message: msg}
	}

	var newNode Node[T]

	if isLeaf {
		newNode = Node[T]{
			Keys:   separatorKeys,
			Leaf:   isLeaf,
			Values: make([][]byte, (2 * DEGREE)),
		}
	} else {
		newNode = Node[T]{
			Keys:     separatorKeys,
			Children: make([]int32, 2*DEGREE+1),
			Leaf:     isLeaf,
		}
	}

	return &newNode, nil
}

// Load node from page
func loadNode[T cmp.Ordered](page *diskio.Page) (*Node[T], error) {
	if page == nil {
		return nil, BTreeError{Message: "Page not provided"}
	}

	k := make([]T, 0)
	v := make([][]byte, 0)
	children := make([]int32, 0)
	internalNode, err := page.IsInternal()

	if err != nil {
		panic(err.Error())
	}

	for i := 0; i < len(page.CellPointers); i++ {
		k = append(k, T(binary.LittleEndian.Uint32(page.CellPointers[i].CellRef.Key)))

		if internalNode {
			children = append(children, page.CellPointers[i].CellRef.PageId)
		} else {
			v = append(v, page.CellPointers[i].CellRef.Value)
		}
	}

	var n Node[T]

	if internalNode {
		n = Node[T]{
			Keys:     k,
			Children: children,
			Leaf:     !internalNode,
		}
	} else {
		n = Node[T]{
			Keys:   k,
			Values: v,
			Leaf:   !internalNode,
		}
	}

	return &n, nil

}
