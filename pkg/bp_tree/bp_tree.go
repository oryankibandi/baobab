package bp_tree

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"github.com/oryankibandi/on_disk_btree/pkg/disk_io"
	"github.com/oryankibandi/on_disk_btree/pkg/helpers"
)

type BTree struct {
	Root *Node
	mu   sync.RWMutex
}

type Node struct {
	Keys         [][]byte     // Keys in the Node
	Children     []int32      // Internal Node children
	Leaf         bool         // Whether Node is leaf
	Values       [][]byte     // Values in leaf node
	LeftSibling  int32        // Left sibling in leaf node
	RightSibling int32        // right sibling in leaf node
	Page         *diskio.Page // Associated on disk page
	mu           sync.RWMutex // Reader Writer Mutex
}

type NodeOpResponse struct {
	splitOutput *SplitResponse
	delresponse *DeleteResponse
}

type SplitResponse struct {
	PromotedSeparatorKey []byte // Key to promote
	NewNodeId            uint32 // Child Reference to add.
}

type MergeMetadata struct {
	rebalanceKey []byte // new key after rebalance. Should replace the parent's separator key. Default to empty if no redistribution happened
	rightMerge   bool   // if a merge/rebalance  happened with the right sibing
	merged       bool   // if a merge happened.
}

type DeleteResponse struct {
	key       []byte
	balanced  bool
	newParent *Node
}

type RangeItem struct {
	Key []byte
	Val []byte
}

//
//type InsertResponse struct {
//	NewNodeId            uint32
//	PromotedSeparatorKey []byte
//}

const (
	DEGREE = 2
)

var bTree *BTree

func InitBTree[K cmp.Ordered]() (*BTree, error) {
	// Load Root Node from diskio
	if diskio.DiskBTree == nil {
		return nil, BTreeError{Message: "BTree not initialized."}
	}

	if diskio.DiskBTree.RootNode == nil {
		bTree = &BTree{}
		log.Println("No root node set.")
		return bTree, nil
	}

	node, err := loadNode(diskio.DiskBTree.RootNode)

	if err != nil {
		return nil, err
	}

	bTree = &BTree{
		Root: node,
	}

	return bTree, nil
}

// Create new node - does not include associated page
func createNode(separatorKeys [][]byte, isLeaf bool, isRoot bool) (*Node, error) {
	if !isLeaf && (len(separatorKeys) < diskio.DEGREE-1 && len(separatorKeys) > diskio.ORDER-1) {
		msg := fmt.Sprintf("Keys must be between %d to %d", diskio.DEGREE-1, diskio.ORDER-1)
		return nil, BTreeError{Message: msg}
	}

	if isLeaf && (len(separatorKeys) < diskio.DEGREE && len(separatorKeys) > diskio.ORDER) {
		msg := fmt.Sprintf("Keys must be from %d to %d", diskio.DEGREE, diskio.ORDER)
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
			Children: make([]int32, 0),
			Leaf:     isLeaf,
		}
	}

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
		rightPtr := page.Header.RightChild

		if rightPtr != 0 {
			children = append(children, rightPtr)
		}

		n = Node{
			Keys:     k,
			Children: children,
			Leaf:     !internalNode,
			Page:     page,
		}
	} else {
		n = Node{
			Keys:         k,
			Values:       v,
			Leaf:         !internalNode,
			Page:         page,
			RightSibling: page.Header.RightSibling,
			LeftSibling:  page.Header.LeftSibling,
		}
	}

	return &n, nil
}

// Splits the given node and returns the new created node, promoted key if `n` is an internal node and error
func (n *Node) split() (*Node, []byte, error) {
	fmt.Println("KEYS B4 SPLIT => ", n.Keys)
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

		fmt.Println("KEYS AFTER SPLIT => ", n.Keys)
		return rightNode, nil, nil
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

		fmt.Println("KEYS AFTER SPLIT => ", n.Keys)
		return rightNode, promotedKey, nil
	}
}

// Assign a page to node
func (n *Node) assignPage(isRoot bool) (*Node, error) {
	if n.Page != nil {
		log.Println("(assignPage) Page already exists")
		return n, nil
	}

	if n.Leaf {
		pge, err := diskio.New(n.Keys, &n.Values, nil, isRoot)

		if err != nil {
			return nil, err
		}

		n.Page = pge
	} else {
		pge, err := diskio.New(n.Keys, nil, &n.Children, isRoot)

		if err != nil {
			return nil, err
		}

		n.Page = pge

	}

	diskio.BPool.AddPageToCache(uint32(n.Page.Header.PageId), n.Page)

	return n, nil
}

// Inserts key and value to node. If is internal node, return page ID of child page. If leaf node insert and return
func (n *Node) insert(key []byte, val []byte, rp *SplitResponse, stack *BTStack) (int32, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// If empty (new node), add the values
	if len(n.Keys) <= 0 && len(n.Values) <= 0 {
		n.Keys = append(n.Keys, key)
		n.Values = append(n.Values, val)

		// Sync to page
		err := n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		fmt.Println("/////// KEYS AFTER INSERT => ", n.Keys)

		if err != nil {
			return 0, err
		}

		return 0, nil
	}

	idx, err := helpers.InsertBinarySearch(n.Keys, key, 0)

	if err != nil {
		panic(err.Error())
	}

	kAtIdx := n.Keys[idx]

	// compare index
	if n.Leaf {
		if bytes.Compare(key, kAtIdx) == -1 {
			// insert at index `idx`

			helpers.InsertToList(&n.Keys, idx, key)
			fmt.Println("*////////////// KEYS AFTER INSERT: ", n.Keys)
			helpers.InsertToList[[]byte](&n.Values, idx, val)
			fmt.Println("*////////////// VALS AFTER INSERT: ", n.Values)

		} else if bytes.Compare(key, kAtIdx) == 0 {
			// replace key
			fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ REPLACING $$$$$$$$$$$$$$$$$$$$$")
			n.Keys[idx] = key
			n.Values[idx] = val

			fmt.Println("$$$$$ KEY AFTER INSERT: ", n.Keys[idx])
			fmt.Println("$$$$$ VAL AFTER INSERT: ", n.Values[idx])
		} else {
			// insert at index `idx+1`
			helpers.InsertToList[[]byte](&n.Keys, idx+1, key)
			fmt.Println("*////////////// KEYS AFTER INSERT: ", n.Keys)
			helpers.InsertToList[[]byte](&n.Values, idx+1, val)
			fmt.Println("*////////////// VALS AFTER INSERT: ", n.Values)
		}

		// Check overflow
		if len(n.Keys) > diskio.ORDER {
			log.Println("(insert) OVERFLOW DETECTED...")
			newRightNode, _, err := n.split()

			if err != nil {
				return 0, err
			}

			// persist left node

			// remove old keys from left node
			// n.postSplitCleanup(idx)

			// create  page for right node
			newRightNode.assignPage(false)
			newRightNode.mu.Lock()
			defer newRightNode.mu.Unlock()
			// update sibling links
			var oldRightSibling *Node
			newRightNode.LeftSibling = n.Page.Header.PageId
			newRightNode.RightSibling = n.Page.Header.RightSibling

			if n.Page.Header.RightSibling != 0 {
				// load node update sibling link
				oldRightSibling, err = buildPage(uint32(n.Page.Header.RightSibling))

				if err != nil {
					return 0, err
				}

				oldRightSibling.mu.Lock()
				oldRightSibling.LeftSibling = newRightNode.Page.Header.PageId
			}

			n.RightSibling = newRightNode.Page.Header.PageId

			//			r := SplitResponse{
			//				PromotedSeparatorKey: newRightNode.Keys[0],
			//				NewNodeId:            uint32(newRightNode.Page.Header.PageId),
			//			}
			//
			rp.PromotedSeparatorKey = newRightNode.Keys[0]
			rp.NewNodeId = uint32(newRightNode.Page.Header.PageId)

			fmt.Println("+---------------------SPLIT DONE ---------------------+")
			//
			// sync btreee node to page
			n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
			newRightNode.Page.Sync(newRightNode.Keys, newRightNode.Values, newRightNode.Children, uint32(newRightNode.RightSibling), uint32(newRightNode.LeftSibling))

			if oldRightSibling != nil {
				fmt.Println("SYNCING OLD RIGHT SIBLING...")
				oldRightSibling.Page.Sync(oldRightSibling.Keys, oldRightSibling.Values, oldRightSibling.Children, uint32(oldRightSibling.RightSibling), uint32(oldRightSibling.LeftSibling))

				oldRightSibling.mu.Unlock()
			}

			return 0, nil
		}

		err = n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		fmt.Println("/////// KEYS AFTER INSERT => ", n.Keys)

		if err != nil {
			return 0, err
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
			return n.Children[idx], nil
		} else {
			// Add to BTStack
			tn := TraversePath{
				n:   n,
				idx: uint32(idx + 1),
			}
			stack.Add(&tn)

			return n.Children[idx+1], nil
		}
	}
}

func (n *Node) search(key []byte, nextNodeId *int32) ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Println("--------------------------------------------------")
	fmt.Printf("SEARCH: %v\n", key)
	fmt.Println("KEYS: ", n.Keys)
	if n.Leaf {
		fmt.Println("VALS: ", n.Values)
	} else {
		fmt.Println("CHILDREN: ", n.Children)
	}
	fmt.Println("--------------------------------------------------")

	if len(n.Keys) <= 0 || (len(n.Values) <= 0 && len(n.Children) <= 0) {
		return nil, nil
	}

	idx, err := helpers.InsertBinarySearch(n.Keys, key, 0)

	if err != nil {
		return nil, err
	}

	if n.Leaf {
		if bytes.Compare(key, n.Keys[idx]) == 0 {
			*nextNodeId = 0
			// return value
			return n.Values[idx], nil
		} else {
			*nextNodeId = 0
			return nil, BTreeError{Message: "Key not found"}
		}
	} else {
		// internal node
		if bytes.Compare(key, n.Keys[idx]) == -1 {
			*nextNodeId = n.Children[idx]
		} else {

			*nextNodeId = n.Children[idx+1]
		}
		fmt.Println("FOLLOW ---> ", *nextNodeId)

		return nil, nil
	}
}

// Retrieves node with key, and returns values in the node greater than key, right sibling page id and error if any
func (n *Node) rangeSearch(key []byte, nextNodeId *int32) ([]RangeItem, int32, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Println("--------------------------------------------------")
	fmt.Printf("SEARCH: %v\n", key)
	fmt.Println("KEYS: ", n.Keys)
	if n.Leaf {
		fmt.Println("VALS: ", n.Values)
	} else {
		fmt.Println("CHILDREN: ", n.Children)
	}
	fmt.Println("--------------------------------------------------")

	if len(n.Keys) <= 0 || (len(n.Values) <= 0 && len(n.Children) <= 0) {
		return nil, 0, nil
	}

	idx, err := helpers.InsertBinarySearch(n.Keys, key, 0)

	if err != nil {
		return nil, 0, err
	}

	if n.Leaf {
		if bytes.Compare(key, n.Keys[idx]) == 0 {
			*nextNodeId = 0
			// return value
			items := make([]RangeItem, 0)
			tempVals := n.Values[idx:]
			var ri RangeItem

			for i, k := range n.Keys[idx:] {
				ri = RangeItem{
					Key: k,
					Val: tempVals[i],
				}

				items = append(items, ri)
			}

			return items, n.RightSibling, nil
		} else {
			*nextNodeId = 0
			return nil, 0, BTreeError{Message: "Key not found"}
		}
	} else {
		// internal node
		if bytes.Compare(key, n.Keys[idx]) == -1 {
			*nextNodeId = n.Children[idx]
		} else {

			*nextNodeId = n.Children[idx+1]
		}
		fmt.Println("FOLLOW ---> ", *nextNodeId)

		return nil, 0, nil
	}
}

// Deletes key and associated value from BTree. If n is an internal node, returns pageID of child to follow, -1 if error and 0 if delete was successful or key doesn't exist.
func (n *Node) deleteValue(key []byte, stack *BTStack, mr *MergeMetadata) (int32, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	fmt.Println("(DELETE) KEYS: ", n.Keys)
	fmt.Println("(DELETE) VALS: ", n.Values)
	fmt.Println("(DELETE) CHILDREN: ", n.Children)
	if len(n.Keys) <= 0 {
		return -1, nil
	}

	idx, err := helpers.InsertBinarySearch(n.Keys, key, 0)

	if err != nil {
		return -1, err
	}

	kAtIdx := n.Keys[idx]

	if n.Leaf {
		if bytes.Compare(key, kAtIdx) != 0 {
			// item not in leaf/btree
			return -1, nil
		}

		// delete Item
		n.Keys = append(n.Keys[:idx], n.Keys[idx+1:]...)
		n.Values = append(n.Values[:idx], n.Values[idx+1:]...)

		// Check for underflow
		if len(n.Keys) < DEGREE {
			fmt.Println("UNDERFLOW DETECTED------------------>")
			// merge
			err = n.handleLeafUnderflow(mr)

			if err != nil {
				return -1, nil
			}

			fmt.Println("KEYS AFTER DELETION => ", n.Keys)
			fmt.Println("VALS AFTER DELETION => ", n.Values)

			return 1, nil
		}

		// sync
		err = n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		if err != nil {
			return -1, err
		}

		fmt.Println("KEYS AFTER DELETION => ", n.Keys)
		fmt.Println("VALS AFTER DELETION => ", n.Values)

		return 1, nil
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
			return n.Children[idx], nil
		} else {
			// Add to BTStack
			tn := TraversePath{
				n:   n,
				idx: uint32(idx + 1),
			}
			stack.Add(&tn)

			return n.Children[idx+1], nil
		}
	}
}

// Redistributes keys or merges underflowed node with sibling node
func (n *Node) handleLeafUnderflow(mr *MergeMetadata) error {
	if !n.Leaf {
		return BTreeError{Message: "(handleLeafUnderflow) Tried to merge internal node"}
	}

	var mergeSibling *Node
	var rightMerge bool
	var err error

	// determing sibling to merge with, default to right sibling
	if n.RightSibling > 0 {
		mergeSibling, err = buildPage(uint32(n.RightSibling))

		if err != nil {
			return err
		}

		rightMerge = true
	} else if n.LeftSibling > 0 {
		mergeSibling, err = buildPage(uint32(n.LeftSibling))

		if err != nil {
			return err
		}

		rightMerge = false
	} else {

		if bTree.Root.Page.Header.PageId == n.Page.Header.PageId {
			// Node is root, underflow Invariant allowed
			mr.merged = false
			mr.rebalanceKey = make([]byte, 0)
			return nil
		}

		// TODO: If not root, collapse into parent
		return BTreeError{Message: "No sibling to merge/borrow"}
	}

	mergeSibling.mu.Lock()
	if len(n.Keys)+len(mergeSibling.Keys) >= diskio.ORDER {
		// redistribute/borrow keys
		totKeys := make([][]byte, 0)
		totVals := make([][]byte, 0)

		if rightMerge {
			totKeys = append(n.Keys, mergeSibling.Keys...)
			totVals = append(n.Values, mergeSibling.Values...)

			mid := len(totKeys) / 2

			n.Keys = totKeys[:mid]
			n.Values = totVals[:mid]

			mergeSibling.Keys = totKeys[mid:]
			mergeSibling.Values = totVals[mid:]

			// set rebalance key
			mr.rebalanceKey = mergeSibling.Keys[0]
		} else {
			totKeys = append(mergeSibling.Keys, n.Keys...)
			totVals = append(mergeSibling.Values, n.Values...)

			mid := len(totKeys) / 2

			mergeSibling.Keys = totKeys[:mid]
			mergeSibling.Values = totVals[:mid]

			n.Keys = totKeys[mid:]
			n.Values = totVals[mid:]

			// set rebalance key
			mr.rebalanceKey = n.Keys[0]
		}

		// Clear
		totKeys = nil
		totVals = nil

		n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		mergeSibling.mu.Unlock()
	} else {
		// merge
		if rightMerge {
			mergeSibling.Keys = append(n.Keys, mergeSibling.Keys...)
			mergeSibling.Values = append(n.Values, mergeSibling.Values...)

			// update sibling links
			if n.LeftSibling > 0 {
				// get left sibling
				pge, err := diskio.BPool.FetchPage(uint32(n.LeftSibling))

				if err != nil {
					return err
				}

				leftSib, err := loadNode(pge)

				if err != nil {
					return err
				}

				leftSib.mu.Lock()
				leftSib.RightSibling = n.RightSibling
				mergeSibling.LeftSibling = n.LeftSibling
				leftSib.mu.Unlock()

				leftSib.Page.Sync(leftSib.Keys, leftSib.Values, leftSib.Children, uint32(leftSib.RightSibling), uint32(leftSib.LeftSibling))
			}
		} else {
			mergeSibling.Keys = append(mergeSibling.Keys, n.Keys...)
			mergeSibling.Values = append(mergeSibling.Values, n.Values...)

			// update sibling links
			if n.RightSibling > 0 {
				// get left sibling
				pge, err := diskio.BPool.FetchPage(uint32(n.RightSibling))

				if err != nil {
					return err
				}

				rightSib, err := loadNode(pge)

				if err != nil {
					return err
				}

				rightSib.mu.Lock()
				rightSib.LeftSibling = n.LeftSibling
				mergeSibling.RightSibling = n.RightSibling
				rightSib.mu.Unlock()

				rightSib.Page.Sync(rightSib.Keys, rightSib.Values, rightSib.Children, uint32(rightSib.RightSibling), uint32(rightSib.LeftSibling))
			}
		}

		mr.merged = true
		// update siblings

		// mark node as deleted
		err = n.Page.MarkAsDead()

		if err != nil {
			return err
		}

		// Sync to page
		n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))
		mergeSibling.mu.Unlock()
	}

	mr.rightMerge = rightMerge

	return nil
}

// Handle underflow of an internal node's children - merges or rebalances
func (n *Node) handleInternalUnderflow(mr *MergeMetadata, stack *BTStack) error {
	if n.Leaf {
		return BTreeError{Message: "Tried to merge leaf node"}
	}

	var mergeSibling *Node
	var err error
	var rightMerge bool

	path, err := stack.Parent()

	if err != nil || path == nil || (path != nil && path.n == nil) {
		if bTree.Root.Page.Header.PageId != n.Page.Header.PageId {
			// none root without parent
			return err
		}

		// is root node. return
		return nil
	}

	// determing sibling to merge with, default to right sibling
	if n.RightSibling > 0 {
		mergeSibling, err = buildPage(uint32(n.RightSibling))

		if err != nil {
			return err
		}

		rightMerge = true
	} else if n.LeftSibling > 0 {
		mergeSibling, err = buildPage(uint32(n.LeftSibling))

		if err != nil {
			return err
		}

		rightMerge = false
	} else {

		if bTree.Root.Page.Header.PageId == n.Page.Header.PageId {
			// Node is root, underflow Invariant allowed
			mr.merged = false
			mr.rebalanceKey = make([]byte, 0)
			return nil
		}

		// TODO: If not root, collapse into parent
		return BTreeError{Message: "No sibling to merge/borrow"}
	}

	mergeSibling.mu.Lock()

	if len(n.Children)+len(mergeSibling.Children) >= diskio.ORDER+1 {
		// SUM(A+B) >= N+1, borrow/redistribute
		totKeys := make([][]byte, 0)
		totChildren := make([]int32, 0)

		if rightMerge {
			totKeys = append(n.Keys, mergeSibling.Keys...)
			totChildren = append(n.Children, mergeSibling.Children...)

			midK := len(totKeys) / 2
			midC := len(totChildren) / 2

			n.Keys = totKeys[:midK]
			n.Children = totChildren[:midC]

			mr.rebalanceKey = mergeSibling.Keys[0]
		} else {
			totKeys = append(mergeSibling.Keys, n.Keys...)
			totChildren = append(mergeSibling.Children, n.Children...)

			midK := len(totKeys) / 2
			midC := len(totChildren) / 2

			mergeSibling.Keys = totKeys[:midK]
			mergeSibling.Children = totChildren[:midC]

			n.Keys = totKeys[midK:]
			n.Children = totChildren[midC:]

			mr.rebalanceKey = n.Keys[0]
		}

		// Clear
		totKeys = nil
		totChildren = nil

		n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

	} else {
		// merge
		if rightMerge {
			// move items from current node to right sibling
			mergeSibling.Keys = append(n.Keys, append([][]byte{path.n.Keys[path.idx]}, mergeSibling.Keys...)...)
			mergeSibling.Children = append(n.Children, mergeSibling.Children...)

			// update links
			if n.LeftSibling > 0 {
				leftSib, err := buildPage(uint32(n.LeftSibling))

				if err != nil {
					return err
				}

				leftSib.mu.Lock()
				leftSib.RightSibling = n.RightSibling
				mergeSibling.LeftSibling = n.LeftSibling

				leftSib.Page.Sync(leftSib.Keys, leftSib.Values, leftSib.Children, uint32(leftSib.RightSibling), uint32(leftSib.LeftSibling))

				leftSib.mu.Unlock()
			} else {
				// since curr node will be deleted, set to 0
				mergeSibling.LeftSibling = 0
			}
		} else {
			// move items from current node to left sibling
			mergeSibling.Keys = append(mergeSibling.Keys, append([][]byte{path.n.Keys[path.idx-1]}, n.Keys...)...)
			mergeSibling.Children = append(mergeSibling.Children, n.Children...)

			// update sibling links
			if n.RightSibling > 0 {
				rightSib, err := buildPage(uint32(n.RightSibling))

				if err != nil {
					return err
				}

				rightSib.mu.Lock()
				rightSib.LeftSibling = n.LeftSibling
				mergeSibling.RightSibling = n.RightSibling

				rightSib.Page.Sync(rightSib.Keys, rightSib.Values, rightSib.Children, uint32(rightSib.RightSibling), uint32(rightSib.LeftSibling))

				rightSib.mu.Unlock()
			} else {
				mergeSibling.RightSibling = 0
			}
		}

		// mark curr node for deletion
		err = n.Page.MarkAsDead()

		if err != nil {
			return err
		}

		mr.merged = true

		n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))
	}

	mr.rightMerge = rightMerge
	mergeSibling.mu.Unlock()

	return nil
}

func InsertValue(keys [][]byte, vals [][]byte) (bool, error) {
	for i, _ := range keys {
		if len(keys[i]) >= 4 {
			log.Printf("%d Inserting {%v:%v} .................................................................\n", i, binary.LittleEndian.Uint32(keys[i]), string(vals[i]))
		} else {
			log.Printf("%d Inserting {%v:%v} .................................................................\n", i, keys[i], string(vals[i]))

		}
		// Get root page and retrieve/create node

		// stack
		st := BTStack{
			stack: make(map[int]*TraversePath),
		}

		insertRes := SplitResponse{}
		var nodePageId int32

		// fmt.Println("(InsertValue) ROOT => ", bTree.Root)
		if bTree.Root == nil {
			fmt.Println("(InsertValue) NO ROOT NODE. SETTING...")
			rootNode := Node{
				Keys:   [][]byte{keys[i]},
				Leaf:   true,
				Values: [][]byte{vals[i]},
			}

			bTree.mu.Lock()
			bTree.Root = &rootNode

			// Assign page & persist
			bTree.Root.assignPage(true)
			bTree.mu.Unlock()

			//return true, nil
		} else {
			fmt.Println("(InsertValue) ROOT NODE FOUND...")

			bTree.mu.RLock()
			nodePageId = bTree.Root.Page.Header.PageId
			bTree.mu.RUnlock()

			// handle insertion
			for nodePageId != 0 {
				// Construct Node from page
				pge, err := diskio.BPool.FetchPage(uint32(nodePageId))

				if err != nil {
					return false, err
				}

				// create node
				node, err := loadNode(pge)

				fmt.Println("NODE KEYS *************> ", node.Keys)

				if err != nil {
					return false, err
				}

				// update root ptr
				bTree.mu.Lock()
				if nodePageId == bTree.Root.Page.Header.PageId {
					bTree.Root = node
				}
				bTree.mu.Unlock()

				// insert
				fmt.Printf("(%d)********INSERTING KEY: %v\nINSERTING VAL:%v\n", nodePageId, keys[i], vals[i])
				nodePageId, err = node.insert(keys[i], vals[i], &insertRes, &st)

				if err != nil {
					return false, err
				}
			}

			// Handle propagating splits
			for insertRes.NewNodeId != 0 {
				fmt.Println("HANDLING PROPAGATING SPLITS..............")
				// When there's a value to be promoted
				if st.Count > 0 {
					parent, err := st.Pop()

					if err != nil {
						return false, err
					}

					parent.n.mu.Lock()
					_, err = helpers.InsertToList[[]byte](&parent.n.Keys, int(parent.idx), insertRes.PromotedSeparatorKey)

					if err != nil {
						return false, err
					}

					_, err = helpers.InsertToList[int32](&parent.n.Children, int(parent.idx+1), int32(insertRes.NewNodeId))

					if err != nil {
						return false, err
					}

					if len(parent.n.Keys) > diskio.ORDER-1 {
						// handle overflow(internal node)
						newRightNode, promoted, err := parent.n.split()

						if err != nil {
							return false, err
						}

						// persist new right node
						_, err = newRightNode.assignPage(false)

						if err != nil {
							return false, err
						}

						// update sibling pointers
						var oldRightSibling *Node
						newRightNode.LeftSibling = parent.n.Page.Header.PageId
						newRightNode.RightSibling = parent.n.Page.Header.RightSibling

						if parent.n.Page.Header.RightSibling != 0 {
							// load node update sibling link
							oldRightSibling, err = buildPage(uint32(parent.n.Page.Header.RightSibling))

							if err != nil {
								return false, err
							}

							oldRightSibling.mu.Lock()
							oldRightSibling.LeftSibling = newRightNode.Page.Header.PageId

							oldRightSibling.Page.Sync(oldRightSibling.Keys, oldRightSibling.Values, oldRightSibling.Children, uint32(oldRightSibling.RightSibling), uint32(oldRightSibling.LeftSibling))
							oldRightSibling.mu.Unlock()
						}

						fmt.Println("NEW RIGHT NODE ====> ", newRightNode)
						fmt.Println("NEW RIGHT NODE PAGE ====> ", newRightNode.Page)
						fmt.Println("NEW RIGHT NODE PAGE HEADER ====> ", newRightNode.Page.Header)
						parent.n.RightSibling = newRightNode.Page.Header.PageId

						r := SplitResponse{
							PromotedSeparatorKey: promoted,
							NewNodeId:            uint32(newRightNode.Page.Header.PageId),
						}

						// set new Insert response
						insertRes = r

						// Sync new child and its right sibling
						newRightNode.Page.Sync(newRightNode.Keys, newRightNode.Values, newRightNode.Children, uint32(newRightNode.RightSibling), uint32(newRightNode.LeftSibling))
					} else {
						// reset insertres
						insertRes.NewNodeId = 0
						insertRes.PromotedSeparatorKey = make([]byte, 0)
					}

					// sync parent
					parent.n.Page.Sync(parent.n.Keys, parent.n.Values, parent.n.Children, uint32(parent.n.RightSibling), uint32(parent.n.LeftSibling))
					fmt.Printf("PARENT ID: %d\nKEYS %v\nVALS %v\nPAGEIDS: %v\n", parent.n.Page.Header.PageId, parent.n.Keys, parent.n.Values, parent.n.Children)
					parent.n.mu.Unlock()

				} else {
					// create and assign new root node.
					log.Println("ADDING NEW ROOT -----------------------> ")
					newRoot, err := createNode([][]byte{insertRes.PromotedSeparatorKey}, false, true)

					if err != nil {
						return false, err
					}

					newRoot.Children = append(newRoot.Children, bTree.Root.Page.Header.PageId)
					newRoot.Children = append(newRoot.Children, int32(insertRes.NewNodeId))

					log.Println("ADDED NEW ROOT -----------------------> ", newRoot)
					log.Println("NEW ROOT CHILDREN COUNT -----------------------> ", len(newRoot.Children))

					log.Println("NEW ROOT CHILDREN -----------------------> ", newRoot.Children)

					newRoot.assignPage(true)

					log.Println("NEW ROOT PAGEID -----------------------> ", newRoot.Page.Header.PageId)
					log.Println("NEW ROOT PAGE -----------------------> ", newRoot.Page)
					log.Println("NEW ROOT PAGE HEADER -----------------------> ", newRoot.Page.Header)

					bTree.mu.Lock()
					bTree.Root = newRoot
					bTree.mu.Unlock()

					// sync
					// newRoot.Page.Sync(newRoot.Keys, newRoot.Values, newRoot.Children)
					// reset value
					insertRes.NewNodeId = 0
				}
			}

			// clear stack if not empty
			if st.Count > 0 {
				st.Clear()
			}
		}
	}

	return true, nil
}

func DeleteValue(keys [][]byte) (bool, error) {
	for i, _ := range keys {
		st := BTStack{
			stack: make(map[int]*TraversePath),
		}

		mergeMetadata := MergeMetadata{}

		var nodePageId int32

		if bTree.Root == nil {
			return false, BTreeError{Message: "BTree not initialized."}
		}

		nodePageId = bTree.Root.Page.Header.PageId

		for nodePageId > 0 {
			// construct node
			pge, err := diskio.BPool.FetchPage(uint32(nodePageId))

			if err != nil {
				return false, err
			}

			// create node
			node, err := loadNode(pge)

			fmt.Println("NODE KEYS *************> ", node.Keys)

			if err != nil {
				return false, err
			}

			// delete
			fmt.Printf("(%d) DELETING KEY: %v\n", nodePageId, keys[i])

			nodePageId, err = node.deleteValue(keys[i], &st, &mergeMetadata)

			fmt.Printf("NODEPAGEID: %d\n", nodePageId)

			if err != nil {
				return false, err
			}
		}

		// Handle propagating merges and updating parent keys
		// if key redistribution, new key needs to be set on parent
		// if merge, delete separator key
		for mergeMetadata.merged || len(mergeMetadata.rebalanceKey) > 0 {
			path, err := st.Pop()

			if err != nil {
				return false, err
			}

			path.n.mu.Lock()
			keyIdx, err := helpers.InsertBinarySearch(path.n.Keys, keys[i], 0)

			if err != nil {
				return false, err
			}

			// handle redistribution
			if len(mergeMetadata.rebalanceKey) > 0 {
				// replace separator key with new one
				if mergeMetadata.rightMerge {
					path.n.Keys = append(path.n.Keys[:keyIdx], append([][]byte{mergeMetadata.rebalanceKey}, path.n.Keys[keyIdx:]...)...)
				} else {
					path.n.Keys = append(path.n.Keys[:keyIdx-1], append([][]byte{mergeMetadata.rebalanceKey}, path.n.Keys[keyIdx-1:]...)...)
				}
			} else if mergeMetadata.merged {
				// merge happened. Demote separator key and delete child pointer
				if mergeMetadata.rightMerge {
					path.n.Keys = append(path.n.Keys[:keyIdx], path.n.Keys[keyIdx+1:]...)
					path.n.Children = append(path.n.Children[:path.idx], path.n.Children[path.idx+1:]...)
				} else {
					path.n.Keys = append(path.n.Keys[:keyIdx-1], path.n.Keys[keyIdx:]...)
					path.n.Children = append(path.n.Children[:path.idx], path.n.Children[path.idx+1:]...)

				}
			}

			// reset metadata
			mergeMetadata.merged = false
			mergeMetadata.rebalanceKey = make([]byte, 0)

			// check underflow
			if len(path.n.Keys) < DEGREE && bTree.Root.Page.Header.PageId != path.n.Page.Header.PageId {
				// merge and set metadata
				err = path.n.handleInternalUnderflow(&mergeMetadata, &st)

				if err != nil {
					return false, nil
				}
			} else {
				// Sync
				err = path.n.Page.Sync(path.n.Keys, path.n.Values, path.n.Children, uint32(path.n.RightSibling), uint32(path.n.LeftSibling))

				if err != nil {
					return false, nil
				}
			}

			path.n.mu.Unlock()
		}

		return true, nil
	}

	return true, nil
}

// Gets pages from cache and buids Node
func buildPage(pageId uint32) (*Node, error) {
	pge, err := diskio.BPool.FetchPage(pageId)

	if err != nil {
		return nil, err
	}

	node, err := loadNode(pge)

	if err != nil {
		return nil, err
	}

	return node, nil
}

// Retrieves value with key
func Get(key []byte) ([]byte, error) {
	var nodePageId int32
	var val []byte

	if bTree.Root == nil {
		return nil, BTreeError{"Key Value store not initialized"}
	}

	nodePageId = bTree.Root.Page.Header.PageId

	for nodePageId > 0 {
		pge, err := buildPage(uint32(nodePageId))

		if err != nil {
			return nil, err
		}

		val, err = pge.search(key, &nodePageId)

		if err != nil {
			return nil, err
		}
	}

	return val, nil
}

// Retrieves value with key
func RangeSearch(minKey []byte, maxKey []byte, count int32) ([]RangeItem, error) {
	var nodePageId int32
	var items []RangeItem
	var rightSibId int32

	if bTree.Root == nil {
		return nil, BTreeError{"Key Value store not initialized"}
	}

	nodePageId = bTree.Root.Page.Header.PageId
	var pge *Node
	var err error

	for nodePageId > 0 {
		pge, err = buildPage(uint32(nodePageId))

		if err != nil {
			return nil, err
		}

		items, rightSibId, err = pge.rangeSearch(minKey, &nodePageId)

		if err != nil {
			return nil, err
		}
	}

	pge.mu.RLock()
	var rightSib *Node
	// get range
RangeLoop:
	for rightSibId > 0 {
		// 1. Get right child
		// 2. For each key, check if is more than max key
		// 3. Increment count
		// 4. Add key to val
		rightSib, err := buildPage(uint32(rightSibId))

		if err != nil {
			return nil, err
		}

		if !rightSib.Leaf {
			return nil, BTreeError{Message: "Encountered internal node during range scan."}
		}

		// unloack prev node afte acquiring a lock on it's right sibling
		rightSib.mu.RLock()
		pge.mu.RUnlock()

		var ri RangeItem
		for i, k := range rightSib.Keys {
			if len(maxKey) > 0 && bytes.Compare(maxKey, k) == 1 {
				// key greater than max key, break
				rightSib.mu.RUnlock()
				break RangeLoop
			}

			ri = RangeItem{
				Key: k,
				Val: rightSib.Values[i],
			}

			items = append(items, ri)

			if len(items) >= int(count) {
				rightSib.mu.RUnlock()
				break RangeLoop
			}

		}

		rightSibId = rightSib.RightSibling

		pge = rightSib

	}

	// Unlock right sibling
	if rightSib != nil {
		rightSib.mu.RUnlock()
	}

	return items, nil
}

// shutdown
func Shutdown() {
	diskio.DiskBTree.Close()
}
