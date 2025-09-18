package bp_tree

import (
	"bytes"
	"cmp"
	"fmt"
	"sync"

	"github.com/oryankibandi/on_disk_btree/pkg/disk_io"
)

type BTree[K cmp.Ordered] struct {
	Root *Node
}

// TODO: Convert Keys to  be [][]bytes. Use bytes.Compare() for the comparison
type Node struct {
	Keys     [][]byte // Keys in the Node
	Children []int32  // Internal Node children
	Leaf     bool     // Whether Node is leaf
	Values   [][]byte // Values in leaf node
	// LeftSibling  int32   // Left sibling in leaf node
	// RightSibling int32   // right sibling in leaf node
	Page *diskio.Page // Associated on disk page
	mu   sync.Mutex
}

type NodeOpResponse struct {
	splitOutput *SplitResponse
	delresponse *DeleteResponse
}

type SplitResponse struct {
	PromotedKey []byte // Key to promote
	child       *Node  // Child Reference to add.
	// leftChild *Node[T]
}

type DeleteResponse struct {
	key       []byte
	balanced  bool
	newParent *Node
}

type InsertResponse struct {
	NewNodeId            uint32
	PromotedSeparatorKey []byte
}

const (
	DEGREE = 2
)

func InitBTree[K cmp.Ordered]() (*BTree[K], error) {
	// Load Root Node from diskio
	if diskio.DiskBTree == nil {
		return nil, BTreeError{Message: "BTree not initialized."}
	}

	if diskio.DiskBTree.RootNode == nil {
		return nil, BTreeError{Message: "Root Node not set."}
	}

	node, err := loadNode(diskio.DiskBTree.RootNode)

	if err != nil {
		return nil, err
	}

	b := BTree[K]{
		Root: node,
	}

	return &b, nil
}

// Create new node - does not include associated page
func createNode(separatorKeys [][]byte, isLeaf bool, isRoot bool) (*Node, error) {
	if (len(separatorKeys) > (DEGREE*2) || len(separatorKeys) < DEGREE) && !isLeaf && !isRoot {
		msg := fmt.Sprintf("Keys must be from %d to %d", DEGREE, 2*DEGREE)
		return nil, BTreeError{Message: msg}
	}

	var newNode Node

	if isLeaf {
		newNode = Node{
			Keys:   separatorKeys,
			Leaf:   isLeaf,
			Values: make([][]byte, (2 * DEGREE)), // Empty list
		}
	} else {
		newNode = Node{
			Keys:     separatorKeys,
			Children: make([]int32, 2*DEGREE+1),
			Leaf:     isLeaf,
		}
	}

	// TODO: Create Page on disk for new Node
	return &newNode, nil
}

// Load node from existing page
func loadNode(page *diskio.Page) (*Node, error) {
	if page == nil {
		return nil, BTreeError{Message: "Page not provided"}
	}

	k := make([][]byte, 0) // keys
	v := make([][]byte, 0) // values
	children := make([]int32, 0)
	internalNode, err := page.IsInternal()

	if err != nil {
		panic(err.Error())
	}

	for i := 0; i < len(page.CellPointers); i++ {
		k = append(k, page.CellPointers[i].CellRef.Key)

		if internalNode {
			children = append(children, page.CellPointers[i].CellRef.PageId)
		} else {
			v = append(v, page.CellPointers[i].CellRef.Value)
		}
	}

	var n Node

	if internalNode {
		n = Node{
			Keys:     k,
			Children: children,
			Leaf:     !internalNode,
			Page:     page,
		}
	} else {
		n = Node{
			Keys:   k,
			Values: v,
			Leaf:   !internalNode,
			Page:   page,
		}
	}

	return &n, nil
}

func (n *Node) split() (SplitResponse, error) {
	if n.Leaf {
		mid := len(n.Keys) / 2
		rightKeys := n.Keys[mid:]
		rightVals := n.Values[mid:]
		// remove keys from node
		n.Keys = n.Keys[:mid]
		n.Values = n.Values[:mid]

		// Create new Node
		rightNode, err := createNode(rightKeys, true, false)

		if err != nil {
			panic(err.Error())
		}

		rightNode.Values = rightVals

		rightNode.assignPage()
		i := SplitResponse{
			PromotedKey: rightKeys[0],
			child:       rightNode,
		}

		return i, nil
	} else {
		// None leaf node split
		mid := len(n.Keys) / 2
		promotedKey := n.Keys[mid]
		rightKeys := n.Keys[mid+1:]
		rightChildren := n.Children[mid+1:]

		// replace keys on left Node
		n.Keys = n.Keys[:mid]
		n.Children = n.Children[:mid+1]

		// Create new node
		rightNode, err := createNode(rightKeys, false, false)

		if err != nil {
			panic(err.Error())
		}

		rightNode.Children = rightChildren

		rightNode.assignPage()

		i := SplitResponse{
			PromotedKey: promotedKey,
			child:       rightNode,
		}

		return i, nil
	}
}

// Assign a page to node
func (n *Node) assignPage() (*Node, error) {
	if n.Page != nil {
		return n, nil
	}

	if n.Leaf {
		pge, err := diskio.New(n.Keys, &n.Values, nil)

		if err != nil {
			return nil, err
		}

		n.Page = pge
		return n, nil

	} else {
		pge, err := diskio.New(n.Keys, nil, &n.Children)

		if err != nil {
			return nil, err
		}

		n.Page = pge
		return n, nil
	}
}

func (n *Node) insert(key []byte, val []byte, rp *InsertResponse, stack *BTStack) (int, error) {
	idx, err := InsertBinarySearch(n.Keys, key, 0)

	if err != nil {
		panic(err.Error())
	}

	// compare index
	if n.Leaf {
		if bytes.Compare(key, n.Keys[idx]) == -1 {
			// insert at index `idx`
			InsertToList(&n.Keys, idx, key)
			InsertToList[[]byte](&n.Values, idx, val)
		} else {
			// insert at index `idx+1`
			InsertToList[[]byte](&n.Keys, idx+1, key)
			InsertToList[[]byte](&n.Values, idx+1, val)
		}

		// Check overflow
		if len(n.Keys) > (2 * DEGREE) {
			r, err := n.split()

			if err != nil {
				return 0, err
			}

			rp.PromotedSeparatorKey = r.PromotedKey
			rp.NewNodeId = uint32(r.child.Page.Header.PageId)

			return 0, nil
		}

		return 0, nil
	} else {
		// internal node
		if bytes.Compare(key, n.Keys[idx]) == -1 {
			// Add to BTStack
			tn := TraversePath{
				n:   n,
				idx: uint32(idx),
			}
			stack.Add(&tn)

			// return page ID of child to follow
			return int(n.Children[idx]), nil
		} else {
			// Add to BTStack
			tn := TraversePath{
				n:   n,
				idx: uint32(idx + 1),
			}
			stack.Add(&tn)

			return int(n.Children[idx+1]), nil
		}
	}
}
