package bp_tree

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	bf "github.com/oryankibandi/baobab/pkg/buffer_manager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/wal"
)

// TODO: Store root page ID instead of pointer to node
type BTree struct {
	Root  uint32 // Root Page ID
	mu    sync.RWMutex
	wal   *wal.WAL
	cache *bf.Cache
}

type Node struct {
	Keys         [][]byte // Keys in the Node
	Children     []int32  // Internal Node children
	Leaf         bool     // Whether Node is leaf
	Values       [][]byte // Values in leaf node
	LeftSibling  int32    // Left sibling in leaf node
	RightSibling int32    // right sibling in leaf node
	// Page         *diskio.Page // Associated on disk page
	PageId int32
	lsn    []byte       // Current LSN of the page
	mu     sync.RWMutex // Reader Writer Mutex
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
	ORDER  = DEGREE * 2
)

// var bTree *BTree

// Initialize a new instance if B+ Tree index
func Initialize[K cmp.Ordered](w *wal.WAL, c *bf.Cache) (*BTree, error) {

	if w == nil {
		panic("WAL is required")
	}

	if c == nil {
		panic("Cache is required")
	}

	rootPageId := c.GetRootPageId()

	bTree := &BTree{
		Root:  rootPageId,
		wal:   w,
		cache: c,
	}

	return bTree, nil
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

		fmt.Println("(INTERNAL) KEYS AFTER SPLIT => ", n.Keys)
		fmt.Println("(INTERNAL)(SPLIT) PROMOTED VALUE => ", promotedKey)
		return rightNode, promotedKey, nil
	}
}

// Creates a frame for a new node
func (n *Node) assignFrame(isRoot bool, cache *bf.Cache) (*bf.Frame, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.Leaf {
		fr, err := cache.CreateNewEntry(n.Keys, &n.Values, nil, isRoot)

		if err != nil {
			return nil, err
		}

		// set pageId on node
		n.PageId = int32(fr.GetKey())

		return fr, nil
	} else {
		fr, err := cache.CreateNewEntry(n.Keys, nil, &n.Children, isRoot)

		if err != nil {
			return nil, err
		}

		// set pageId on node
		n.PageId = int32(fr.GetKey())

		return fr, nil
	}
}

// Inserts key and value to node. If is internal node, return page ID of child page. If leaf node insert and return
func (bt *BTree) insert(n *Node, fr *bf.Frame, key []byte, val []byte, rp *SplitResponse, stack *BTStack) (uint32, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If empty (new node), add keys and values then sync
	if len(n.Keys) <= 0 && len(n.Values) <= 0 {
		n.Keys = append(n.Keys, key)
		n.Values = append(n.Values, val)

		// Sync to page
		err := bt.cache.SyncFrame(fr, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
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
		// Create BLOG entry in wal
		lsn, err := bt.wal.AddPutLog(uint32(n.PageId), key, val)

		if err != nil {
			panic(err)
		}

		n.lsn = lsn

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
			fmt.Println("INSERTED...")

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
		if len(n.Keys) > ORDER {
			log.Println("(insert) OVERFLOW DETECTED...")
			newRightNode, _, err := n.split()
			// fmt.Println("AFTER SPLIT-------->")
			// fmt.Println("KEYS ----> ", newRightNode.Keys)
			// fmt.Println("VALS ----> ", newRightNode.Values)

			if err != nil {
				return 0, err
			}

			// persist left node

			// remove old keys from left node
			// n.postSplitCleanup(idx)

			// create  page for right node
			newRightFr, err := newRightNode.assignFrame(false, bt.cache)

			if err != nil {
				panic(err)
			}
			// fmt.Println("AFTER ASSIGN PAGE-------->")
			// fmt.Println("KEYS ----> ", newRightNode.Keys)
			// fmt.Println("VALS ----> ", newRightNode.Values)
			newRightNode.mu.Lock()
			defer newRightNode.mu.Unlock()
			// fmt.Println("JUST AFTER LOCKING PAGE-------->")
			// fmt.Println("KEYS ----> ", newRightNode.Keys)
			// fmt.Println("VALS ----> ", newRightNode.Values)

			// update sibling links
			var oldRightSibling *Node
			var oldRightFr *bf.Frame
			newRightNode.LeftSibling = n.PageId
			newRightNode.RightSibling = n.RightSibling

			if n.RightSibling > 0 {
				// load node update sibling link
				oldRightFr, oldRightSibling, err = bt.buildPage(uint32(n.RightSibling))

				if err != nil {
					return 0, err
				}

				oldRightSibling.mu.Lock()
				oldRightSibling.LeftSibling = newRightNode.PageId
			}

			n.RightSibling = newRightNode.PageId

			rp.PromotedSeparatorKey = append([]byte(nil), newRightNode.Keys[0]...)
			// copy(rp.PromotedSeparatorKey, newRightNode.Keys[0])
			rp.NewNodeId = uint32(newRightNode.PageId)

			fmt.Println("NEW  NODE ID: ", rp.NewNodeId)
			fmt.Println("PROMOTED SEPRATOR KEY -> ", rp.PromotedSeparatorKey)
			fmt.Println("+---------------------SPLIT DONE ---------------------+")

			// newRightNode.Page.Sync(newRightNode.Keys, newRightNode.Values, newRightNode.Children, uint32(newRightNode.RightSibling), uint32(newRightNode.LeftSibling))

			err = bt.cache.SyncFrame(newRightFr, newRightNode.Keys, newRightNode.Values, newRightNode.Children, uint32(newRightNode.RightSibling), uint32(newRightNode.LeftSibling))

			if err != nil {
				panic(err)
			}

			// n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

			err = bt.cache.SyncFrame(fr, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

			if err != nil {
				panic(err)
			}

			if oldRightSibling != nil {
				fmt.Println("SYNCING OLD RIGHT SIBLING...")
				// oldRightSibling.Page.Sync(oldRightSibling.Keys, oldRightSibling.Values, oldRightSibling.Children, uint32(oldRightSibling.RightSibling), uint32(oldRightSibling.LeftSibling))

				err := bt.cache.SyncFrame(oldRightFr, oldRightSibling.Keys, oldRightSibling.Values, oldRightSibling.Children, uint32(oldRightSibling.RightSibling), uint32(oldRightSibling.LeftSibling))

				if err != nil {
					panic(err)
				}
				// oldRightFr.MarkDirty()
				err = bt.cache.MarkFrameDirty(oldRightFr)

				if err != nil {
					log.Panic(err)
				}

				oldRightSibling.mu.Unlock()
			}

			return 0, nil
		}

		// err = n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		bt.cache.SyncFrame(fr, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		if err != nil {
			panic(err)
		}

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
			return uint32(n.Children[idx]), nil
		} else {
			// Add to BTStack
			tn := TraversePath{
				n:   n,
				idx: uint32(idx + 1),
			}
			stack.Add(&tn)

			return uint32(n.Children[idx+1]), nil
		}
	}
}

func (n *Node) search(key []byte, nextNodeId *int32) ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Println("--------------------------------------------------")
	fmt.Println("NODE ID: ", n.PageId)
	fmt.Printf("SEARCH: %v\n", key)
	fmt.Println("KEYS: ", n.Keys)
	if n.Leaf {
		fmt.Println("VALS: ", n.Values)
	} else {
		fmt.Println("CHILDREN: ", n.Children)
	}
	fmt.Println("--------------------------------------------------")

	if len(n.Keys) <= 0 || (len(n.Values) <= 0 && len(n.Children) <= 0) {
		panic("Invalid node")
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
func (bt *BTree) deleteValue(n *Node, f *bf.Frame, key []byte, stack *BTStack, mr *MergeMetadata) (int32, error) {
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

		// Create BLOG entry in wal
		lsn, err := bt.wal.AddDelLog(uint32(n.PageId), key)

		if err != nil {
			panic(err)
		}

		n.lsn = lsn

		// delete Item
		n.Keys = append(n.Keys[:idx], n.Keys[idx+1:]...)
		n.Values = append(n.Values[:idx], n.Values[idx+1:]...)

		// Check for underflow
		if len(n.Keys) < DEGREE {
			fmt.Println("UNDERFLOW DETECTED------------------>")
			// merge
			err = bt.handleLeafUnderflow(mr, n, f)

			if err != nil {
				return -1, nil
			}

			fmt.Println("KEYS AFTER DELETION => ", n.Keys)
			fmt.Println("VALS AFTER DELETION => ", n.Values)

			// sync
			// err = n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

			// if err != nil {
			// 	return -1, err
			// }

			return -1, nil
		}

		// sync
		// err = n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		err = bt.cache.SyncFrame(f, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

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
func (bt *BTree) handleLeafUnderflow(mr *MergeMetadata, n *Node, fr *bf.Frame) error {
	if !n.Leaf {
		return BTreeError{Message: "(handleLeafUnderflow) Tried to merge internal node"}
	}

	var mergeSibling *Node
	var mergeFr *bf.Frame
	var rightMerge bool
	var err error

	// determing sibling to merge with, default to right sibling
	if n.RightSibling > 0 {
		fmt.Println("RIGHT MERGE")
		mergeFr, mergeSibling, err = bt.buildPage(uint32(n.RightSibling))

		if err != nil {
			return err
		}

		rightMerge = true
	} else if n.LeftSibling > 0 {
		fmt.Println("LEFT MERGE")
		mergeFr, mergeSibling, err = bt.buildPage(uint32(n.LeftSibling))

		if err != nil {
			return err
		}

		rightMerge = false
	} else {
		fmt.Println("NO SIBLINGS....")
		bt.mu.Lock()
		if bt.Root == uint32(n.PageId) {
			fmt.Println("IS ROOT NODE....")
			// Node is root, underflow Invariant allowed
			if len(n.Keys) <= 0 {
				// root node is empty, delete page
				fmt.Println("MARKING PAGE AS DEAD...")
				// err = n.Page.MarkAsDead()
				err = fr.MarkAsDead()

				if err != nil {
					return err
				}

				// clear
				fmt.Println("RESETTING ROOT NODE ==> ")
				bt.Root = 0
				// bt.Root.Page = nil
				// bt.Root = nil
			}

			mr.merged = false
			mr.rebalanceKey = make([]byte, 0)

			// err = n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

			bt.cache.SyncFrame(fr, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

			if err != nil {
				panic(fmt.Errorf("Unable to Sync frame: ", err))
			}

			bt.mu.Unlock()
			return nil
		} else {
			fmt.Println("NOT ROOT NODE...")
		}
		bt.mu.Unlock()

		// TODO: If not root, collapse into parent
		return BTreeError{Message: "No sibling to merge/borrow leaf node"}
	}

	mergeSibling.mu.Lock()
	if len(n.Keys)+len(mergeSibling.Keys) >= ORDER {
		// redistribute/borrow keys
		fmt.Println("(leafnode underflow) REDISTRIBUTING...")
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
			mr.rebalanceKey = append([]byte(nil), mergeSibling.Keys[0]...)
		} else {
			totKeys = append(mergeSibling.Keys, n.Keys...)
			totVals = append(mergeSibling.Values, n.Values...)

			mid := len(totKeys) / 2

			mergeSibling.Keys = totKeys[:mid]
			mergeSibling.Values = totVals[:mid]

			n.Keys = totKeys[mid:]
			n.Values = totVals[mid:]

			// set rebalance key
			mr.rebalanceKey = append([]byte(nil), n.Keys[0]...)
		}

		// Clear
		totKeys = nil
		totVals = nil

		// n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		err = bt.cache.SyncFrame(fr, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		if err != nil {
			panic(fmt.Errorf("Unable to Sync frame: ", err))
		}

		// mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		err = bt.cache.SyncFrame(mergeFr, mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		if err != nil {
			panic(fmt.Errorf("Unable to Sync frame: ", err))
		}

		// mark mergeSibling frame as dirty
		err = bt.cache.MarkFrameDirty(mergeFr)

		if err != nil {
			log.Panic(err)
		}
		// mergeFr.MarkDirty()

		mergeSibling.mu.Unlock()
	} else {
		// merge
		if rightMerge {
			fmt.Println("RIGHT MERGE...")
			mergeSibling.Keys = append(n.Keys, mergeSibling.Keys...)
			mergeSibling.Values = append(n.Values, mergeSibling.Values...)

			// update sibling links
			if n.LeftSibling > 0 {
				// get left sibling
				fmt.Println("UPDATING LEFT SIBLING POINTERS...")
				frame, leftSib, err := bt.buildPage(uint32(n.LeftSibling))
				if err != nil {
					return err
				}

				leftSib.mu.Lock()
				leftSib.RightSibling = n.RightSibling
				mergeSibling.LeftSibling = n.LeftSibling
				leftSib.mu.Unlock()

				// leftSib.Page.Sync(leftSib.Keys, leftSib.Values, leftSib.Children, uint32(leftSib.RightSibling), uint32(leftSib.LeftSibling))
				err = bt.cache.SyncFrame(frame, leftSib.Keys, leftSib.Values, leftSib.Children, uint32(leftSib.RightSibling), uint32(leftSib.LeftSibling))

				if err != nil {
					panic(err)
				}

				// frame.MarkDirty()
				err = bt.cache.MarkFrameDirty(frame)

				if err != nil {
					log.Panic(err)
				}

				bt.cache.ReleaseFrame(frame)
			}
		} else {
			fmt.Println("LEFT MERGE...")
			mergeSibling.Keys = append(mergeSibling.Keys, n.Keys...)
			mergeSibling.Values = append(mergeSibling.Values, n.Values...)

			// update sibling links
			if n.RightSibling > 0 {
				fmt.Println("UPDATING RIGHT SIBLING POINTERS...")
				// get left sibling
				frame, rightSib, err := bt.buildPage(uint32(n.RightSibling))

				if err != nil {
					return err
				}

				rightSib.mu.Lock()
				rightSib.LeftSibling = n.LeftSibling
				mergeSibling.RightSibling = n.RightSibling
				rightSib.mu.Unlock()

				// rightSib.Page.Sync(rightSib.Keys, rightSib.Values, rightSib.Children, uint32(rightSib.RightSibling), uint32(rightSib.LeftSibling))
				err = bt.cache.SyncFrame(frame, rightSib.Keys, rightSib.Values, rightSib.Children, uint32(rightSib.RightSibling), uint32(rightSib.LeftSibling))

				if err != nil {
					panic(err)
				}

				// frame.MarkDirty()
				err = bt.cache.MarkFrameDirty(frame)

				if err != nil {
					log.Panic(err)
				}

				bt.cache.ReleaseFrame(frame)
			}
		}

		mr.merged = true
		// update siblings

		// mark node as deleted
		fmt.Println("MARKING LEAF PAGE AS DEAD...")
		// err = n.Page.MarkAsDead()
		err = fr.MarkAsDead()

		if err != nil {
			return err
		}

		// Sync to page
		// n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		err = bt.cache.SyncFrame(fr, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		if err != nil {
			panic(err)
		}

		// mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		err = bt.cache.SyncFrame(mergeFr, mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		if err != nil {
			panic(err)
		}

		// mergeFr.MarkDirty()
		err = bt.cache.MarkFrameDirty(mergeFr)

		if err != nil {
			log.Panic(err)
		}
		mergeSibling.mu.Unlock()
	}

	mr.rightMerge = rightMerge

	return nil
}

// Handle underflow of an internal node's children - merges or rebalances
func (bt *BTree) handleInternalUnderflow(n *Node, fr *bf.Frame, mr *MergeMetadata, stack *BTStack) error {
	if n.Leaf {
		return BTreeError{Message: "Tried to merge leaf node"}
	}

	var mergeSibling *Node
	var mergeFr *bf.Frame
	var err error
	var rightMerge bool

	path, err := stack.Parent()

	if err != nil || path == nil || (path != nil && path.n == nil) {
		if stack.root != uint32(n.PageId) {
			// non-root node without parent
			return err
		}

		// is root node. return
		return nil
	}

	// determing sibling to merge with, default to right sibling
	if n.RightSibling > 0 {
		mergeFr, mergeSibling, err = bt.buildPage(uint32(n.RightSibling))

		if err != nil {
			return err
		}

		rightMerge = true
	} else if n.LeftSibling > 0 {
		mergeFr, mergeSibling, err = bt.buildPage(uint32(n.LeftSibling))

		if err != nil {
			return err
		}

		rightMerge = false
	} else {

		bt.mu.Lock()
		if stack.root == uint32(n.PageId) {
			// Node is root, underflow Invariant allowed
			mr.merged = false
			mr.rebalanceKey = make([]byte, 0)
			return nil
		}

		// TODO: If not root, collapse into parent
		// schedule curr root for deletion
		fmt.Println("NO SIBLINGS FOUND ===> ")
		fmt.Println("RIGHT SIBLING ==> ", n.RightSibling)
		fmt.Println("LEFT SIBLING ====> ", n.LeftSibling)
		fmt.Println("OLD ROOT ID => ", bt.Root)

		// oldRoot := stack.root
		rootfr, rootNode, err := bt.buildPage(stack.root)

		if err != nil {
			return err
		}

		err = rootfr.MarkAsDead()

		if err != nil {
			return err
		}

		// Set new root
		bt.cache.SetNewRoot(fr)
		// err = n.Page.SetAsRoot()
		bt.Root = fr.Key
		fmt.Println("NEW SET ROOT ID => ", bt.Root)
		bt.mu.Unlock()

		if err != nil {
			return err
		}

		// err = oldRoot.Page.Sync(oldRoot.Keys, oldRoot.Values, oldRoot.Children, uint32(oldRoot.RightSibling), uint32(oldRoot.LeftSibling))

		err = bt.cache.SyncFrame(rootfr, rootNode.Keys, rootNode.Values, rootNode.Children, uint32(rootNode.RightSibling), uint32(rootNode.LeftSibling))

		if err != nil {
			return err
		}

		mr.merged = false
		mr.rebalanceKey = make([]byte, 0)

		return nil
	}

	mergeSibling.mu.Lock()

	if len(n.Children)+len(mergeSibling.Children) >= ORDER+1 {
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

			mr.rebalanceKey = append([]byte(nil), mergeSibling.Keys[0]...)
		} else {
			totKeys = append(mergeSibling.Keys, n.Keys...)
			totChildren = append(mergeSibling.Children, n.Children...)

			midK := len(totKeys) / 2
			midC := len(totChildren) / 2

			mergeSibling.Keys = totKeys[:midK]
			mergeSibling.Children = totChildren[:midC]

			n.Keys = totKeys[midK:]
			n.Children = totChildren[midC:]

			mr.rebalanceKey = append([]byte(nil), n.Keys[0]...)
		}

		// Clear
		totKeys = nil
		totChildren = nil

		// n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
		err = bt.cache.SyncFrame(fr, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		if err != nil {
			return err
		}

		// mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		err = bt.cache.SyncFrame(mergeFr, mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		if err != nil {
			return err
		}
		// update fr
		err = bt.cache.MarkFrameDirty(mergeFr)

		if err != nil {
			log.Panic(err)
		}
		// mergeFr.MarkDirty()

	} else {
		// merge
		if rightMerge {
			// move items from current node to right sibling
			mergeSibling.Keys = append(n.Keys, append([][]byte{path.n.Keys[path.idx]}, mergeSibling.Keys...)...)
			mergeSibling.Children = append(n.Children, mergeSibling.Children...)

			// update links
			if n.LeftSibling > 0 {
				leftSibFr, leftSib, err := bt.buildPage(uint32(n.LeftSibling))

				if err != nil {
					return err
				}

				leftSib.mu.Lock()
				leftSib.RightSibling = n.RightSibling
				mergeSibling.LeftSibling = n.LeftSibling

				// leftSib.Page.Sync(leftSib.Keys, leftSib.Values, leftSib.Children, uint32(leftSib.RightSibling), uint32(leftSib.LeftSibling))

				err = bt.cache.SyncFrame(leftSibFr, leftSib.Keys, leftSib.Values, leftSib.Children, uint32(leftSib.RightSibling), uint32(leftSib.LeftSibling))

				if err != nil {
					return err
				}
				// leftSibFr.MarkDirty()
				err = bt.cache.MarkFrameDirty(leftSibFr)

				if err != nil {
					log.Panic(err)
				}

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
				rightSibFr, rightSib, err := bt.buildPage(uint32(n.RightSibling))

				if err != nil {
					return err
				}

				rightSib.mu.Lock()
				rightSib.LeftSibling = n.LeftSibling
				mergeSibling.RightSibling = n.RightSibling

				// rightSib.Page.Sync(rightSib.Keys, rightSib.Values, rightSib.Children, uint32(rightSib.RightSibling), uint32(rightSib.LeftSibling))

				err = bt.cache.SyncFrame(rightSibFr, rightSib.Keys, rightSib.Values, rightSib.Children, uint32(rightSib.RightSibling), uint32(rightSib.LeftSibling))

				if err != nil {
					panic(err)
				}
				// rightSibFr.MarkDirty()
				err = bt.cache.MarkFrameDirty(rightSibFr)

				if err != nil {
					log.Panic(err)
				}

				rightSib.mu.Unlock()
			} else {
				mergeSibling.RightSibling = 0
			}
		}

		// mark curr node for deletion
		fmt.Println("MARKING INTERNAL PAGE AS DEAD...")
		err = fr.MarkAsDead()

		if err != nil {
			panic(err)
		}

		// err = n.Page.MarkAsDead()

		if err != nil {
			return err
		}

		mr.merged = true

		// n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		err = bt.cache.SyncFrame(fr, n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		if err != nil {
			panic(err)
		}

		// mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		err = bt.cache.SyncFrame(mergeFr, mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))

		if err != nil {
			panic(err)
		}
		// mergeFr.MarkDirty()

		err = bt.cache.MarkFrameDirty(mergeFr)

		if err != nil {
			log.Panic(err)
		}
	}

	mr.rightMerge = rightMerge
	mergeSibling.mu.Unlock()

	return nil
}

func (bt *BTree) InsertValue(keys [][]byte, vals [][]byte) (bool, error) {
	for i, _ := range keys {
		if len(keys[i]) >= 4 {
			log.Printf("%d Inserting {%v:%v} .................................................................\n", i, binary.LittleEndian.Uint32(keys[i]), string(vals[i]))
		} else {
			log.Printf("%d Inserting {%v:%v} .................................................................\n", i, keys[i], string(vals[i]))

		}
		// Get root page and retrieve/create node

		// stack
		st := BTStack{
			stack: make(map[int]*TraversePath), // {pageId: Traverse path}
			root:  bt.Root,
		}

		insertRes := SplitResponse{}
		var nodePageId uint32

		// fmt.Println("(InsertValue) ROOT => ", bTree.Root)
		if bt.Root == 0 {
			log.Println("(InsertValue) NO ROOT NODE. SETTING...")
			rootNode := Node{
				Keys:   [][]byte{keys[i]},
				Leaf:   true,
				Values: [][]byte{vals[i]},
			}

			rootFr, err := rootNode.assignFrame(true, bt.cache)

			if err != nil {
				log.Println(err)
				return false, err
			}

			bt.mu.Lock()
			bt.Root = rootFr.Key

			fmt.Println("ROOT NODE ID: => ", bt.Root)

			// add entry to WAL
			lsn, err := bt.wal.AddPutLog(uint32(bt.Root), keys[i], vals[i])

			if err != nil {
				panic(err)
			}

			rootNode.lsn = lsn

			// sync to frame. New root node has no sibling, set to 0
			err = bt.cache.SyncFrame(rootFr, rootNode.Keys, rootNode.Values, rootNode.Children, 0, 0)

			if err != nil {
				panic(err)
			}

			bt.mu.Unlock()

			//return true, nil
		} else {
			fmt.Println("(InsertValue) ROOT NODE FOUND...")

			bt.mu.RLock()
			nodePageId = bt.Root
			bt.mu.RUnlock()
			fmt.Println("updated root node id")
			fmt.Println("ROOT NODE ID ==> ", nodePageId)

			var oldFrame *bf.Frame
			// handle insertion
			for nodePageId != 0 {
				// Construct Node from page
				fmt.Println("fetching page...")
				frame, node, err := bt.buildPage(nodePageId)

				// 				frame, err := bf.BCache.Get(uint32(nodePageId))
				// 				fmt.Println("Retrieved page...")
				//
				if err != nil {
					return false, err
				}

				//				// defer bf.BCache.ReleaseFrame(frame)
				//
				//				// create node
				//				node, err := loadNode(frame)
				//				fmt.Println("Loaded node ==> ")
				//
				//				fmt.Println("NODE KEYS *************> ", node.Keys)
				//
				//				if err != nil {
				//					return false, err
				//				}

				// release parent frame
				if oldFrame != nil {
					fmt.Println("RELEASING OLD FRAME....")
					err = bt.cache.ReleaseFrame(oldFrame)
					if err != nil {
						panic(err)
					}
				}

				// update root ptr
				bt.mu.Lock()
				if nodePageId == bt.Root {
					bt.Root = nodePageId
				}
				bt.mu.Unlock()

				// insert
				fmt.Printf("(%d)********INSERTING KEY: %v\nINSERTING VAL:%v\n", nodePageId, keys[i], vals[i])
				nodePageId, err = bt.insert(node, frame, keys[i], vals[i], &insertRes, &st)
				fmt.Println("INSERTED KEYS")

				if err != nil {
					bt.cache.ReleaseFrame(frame)

					if oldFrame != nil {
						bt.cache.ReleaseFrame(oldFrame)
					}
					return false, err
				}

				// if the right node was located,  nodePageId is 0
				// mark frame as dirty
				if nodePageId == 0 {
					// frame.MarkDirty()
					err = bt.cache.MarkFrameDirty(frame)

					if err != nil {
						log.Panic(err)
					}
				}

				oldFrame = frame
			}

			if oldFrame != nil {
				err := bt.cache.ReleaseFrame(oldFrame)

				if err != nil {
					panic(fmt.Sprintf("Unable to  release frame: ", err))
				}
			}

			// Handle propagating splits
			for insertRes.NewNodeId != 0 {
				fmt.Println("HANDLING PROPAGATING SPLITS..............")
				// If there's a value to be promoted, get last immediate parent in Breadcrumb stack
				if st.Count > 0 {
					parent, err := st.Pop()

					if err != nil {
						return false, err
					}

					parentFr, err := bt.cache.Get(uint32(parent.n.PageId))

					if err != nil {
						fmt.Println("Unable to pin frame")
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

					if len(parent.n.Keys) > ORDER-1 {
						// handle overflow(internal node)
						fmt.Println("INTERNAL NODE OVERFLOW........................")
						newRightNode, promoted, err := parent.n.split()

						if err != nil {
							return false, err
						}

						// persist new right node
						newRightFr, err := newRightNode.assignFrame(false, bt.cache)

						if err != nil {
							return false, err
						}

						// update sibling pointers
						newRightNode.mu.Lock()
						var oldRightSibling *Node
						var oldRightFr *bf.Frame
						newRightNode.LeftSibling = parent.n.PageId
						newRightNode.RightSibling = parent.n.RightSibling

						if parent.n.RightSibling > 0 {
							// load node update sibling link
							oldRightFr, oldRightSibling, err = bt.buildPage(uint32(parent.n.RightSibling))

							if err != nil {
								return false, err
							}

							oldRightSibling.mu.Lock()
							oldRightSibling.LeftSibling = newRightNode.PageId

							err = bt.cache.SyncFrame(oldRightFr, oldRightSibling.Keys, oldRightSibling.Values, oldRightSibling.Children, uint32(oldRightSibling.RightSibling), uint32(oldRightSibling.LeftSibling))

							if err != nil {
								log.Panic(err)
							}

							// mark frame as dirty
							// oldRightFr.MarkDirty()
							err = bt.cache.MarkFrameDirty(oldRightFr)

							if err != nil {
								log.Panic(err)
							}
							oldRightSibling.mu.Unlock()
						}

						fmt.Println("(INTERNAL) NEW RIGHT NODE ====> ", newRightNode)
						//						fmt.Println("(INTERNAL) NEW RIGHT NODE PAGE ====> ", newRightNode.Page)
						//						fmt.Println("(INTERNAL) NEW RIGHT NODE PAGE HEADER ====> ", newRightNode.Page.Header)
						fmt.Println("(INTERNAL) RECEIVED SEPARATOR KEY =====> ", promoted)
						parent.n.RightSibling = newRightNode.PageId

						r := SplitResponse{
							PromotedSeparatorKey: append([]byte(nil), promoted...),
							NewNodeId:            uint32(newRightNode.PageId),
						}

						// set new Insert response
						insertRes = r

						// Sync new child and its right sibling
						err = bt.cache.SyncFrame(newRightFr, newRightNode.Keys, newRightNode.Values, newRightNode.Children, uint32(newRightNode.RightSibling), uint32(newRightNode.LeftSibling))

						if err != nil {
							log.Panic(err)
						}

						newRightNode.mu.Unlock()
					} else {
						// reset insertres
						insertRes.NewNodeId = 0
						insertRes.PromotedSeparatorKey = make([]byte, 0)
					}

					// sync parent
					err = bt.cache.SyncFrame(parentFr, parent.n.Keys, parent.n.Values, parent.n.Children, uint32(parent.n.RightSibling), uint32(parent.n.LeftSibling))

					if err != nil {
						panic(err)
					}

					fmt.Printf("PARENT ID: %d\nKEYS %v\nVALS %v\nPAGEIDS: %v\n", parent.n.PageId, parent.n.Keys, parent.n.Values, parent.n.Children)
					parent.n.mu.Unlock()

					bt.cache.ReleaseFrame(parentFr)
				} else {
					// create and assign new root node.
					log.Println("ADDING NEW ROOT -----------------------> ")
					fmt.Println("PROMOTED SEPARATOR KEY --> ", insertRes.PromotedSeparatorKey)
					newRoot, err := createNode([][]byte{insertRes.PromotedSeparatorKey}, false, true)

					if err != nil {
						return false, err
					}

					fmt.Println("NEW ROOT PAGE ID => ", newRoot.PageId)

					newRoot.Children = append(newRoot.Children, int32(bt.Root))
					newRoot.Children = append(newRoot.Children, int32(insertRes.NewNodeId))

					log.Println("ADDED NEW ROOT -----------------------> ", newRoot)
					log.Println("NEW ROOT CHILDREN COUNT -----------------------> ", len(newRoot.Children))

					log.Println("NEW ROOT CHILDREN -----------------------> ", newRoot.Children)

					// assign frame
					rootFr, err := newRoot.assignFrame(true, bt.cache)

					if err != nil {
						log.Panic(err)
					}

					log.Println("NEW ROOT PAGEID -----------------------> ", newRoot.PageId)
					log.Println("NEW ROOT PAGE -----------------------> ", newRoot.PageId)
					log.Println("NEW ROOT PAGE HEADER -----------------------> ", newRoot.PageId)

					// sync
					err = bt.cache.SyncFrame(rootFr, newRoot.Keys, newRoot.Values, newRoot.Children, uint32(newRoot.RightSibling), uint32(newRoot.LeftSibling))

					if err != nil {
						panic(fmt.Errorf("Unable to Sync root frame: ", err))
					}

					// update new root in btree
					bt.mu.Lock()
					bt.Root = uint32(newRoot.PageId)
					bt.mu.Unlock()
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

func (bt *BTree) DeleteValue(keys [][]byte) (bool, error) {
	for i, _ := range keys {
		st := BTStack{
			stack: make(map[int]*TraversePath),
			root:  bt.Root,
		}

		mergeMetadata := MergeMetadata{}

		var nodePageId int32

		bt.mu.RLock()
		if bt.Root == 0 {
			return false, BTreeError{Message: "BTree not initialized."}
		}

		nodePageId = int32(bt.Root)
		bt.mu.RUnlock()
		var oldFrame *bf.Frame

		for nodePageId > 0 {
			// construct node
			frame, node, err := bt.buildPage(uint32(nodePageId))
			// frame, err := bt.cache.Get(uint32(nodePageId))

			if err != nil {
				return false, fmt.Errorf("Unable to delete val: ", err)
			}

			if oldFrame != nil {
				bt.cache.ReleaseFrame(oldFrame)
			}
			// create node
			// node, err := loadNode(frame)

			fmt.Println("NODE KEYS *************> ", node.Keys)

			if err != nil {
				bt.cache.ReleaseFrame(frame)
				if oldFrame != nil {
					bt.cache.ReleaseFrame(oldFrame)
				}
				return false, err
			}

			// delete
			fmt.Printf("(%d) DELETING KEY: %v\n", nodePageId, keys[i])

			nodePageId, err = bt.deleteValue(node, frame, keys[i], &st, &mergeMetadata)

			fmt.Printf("NODEPAGEID: %d\n", nodePageId)

			if err != nil {
				bt.cache.ReleaseFrame(frame)
				if oldFrame != nil {
					bt.cache.ReleaseFrame(oldFrame)
				}
				fmt.Println("UNABLE TO DELETE PAGE ==> ", err.Error())
				return false, err
			}

			// mark frame as dirty
			// frame.MarkDirty()
			err = bt.cache.MarkFrameDirty(frame)

			if err != nil {
				log.Panic(err)
			}

			oldFrame = frame

			//err = bf.BCache.ReleaseFrame(frame)

			//if err != nil {
			//return false, err
			//}
		}

		if oldFrame != nil {
			bt.cache.ReleaseFrame(oldFrame)
		}

		// Handle propagating merges and updating parent keys
		// if key redistribution, new key needs to be set on parent
		// if merge, delete separator key
		for mergeMetadata.merged || len(mergeMetadata.rebalanceKey) > 0 {
			fmt.Println("HANDLING PROPAGATING MERGES.....")
			path, err := st.Pop()

			if err != nil {
				return false, err
			}

			fr, err := bt.cache.Get(uint32(path.n.PageId))

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
				if len(path.n.Keys) == 1 {
					path.n.Keys[keyIdx] = mergeMetadata.rebalanceKey
				} else if mergeMetadata.rightMerge {
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
			if len(path.n.Keys) < DEGREE-1 && bt.Root != uint32(path.n.PageId) {
				// merge and set metadata
				err = bt.handleInternalUnderflow(path.n, fr, &mergeMetadata, &st)

				if err != nil {
					fmt.Println("ERR HANDLING INTERNAL UNDERFLOW ==> ", err)
					return false, err
				}
			} else if len(path.n.Keys) < DEGREE-1 && bt.Root == uint32(path.n.PageId) && len(path.n.Children) == 1 {
				// collapse node and set new root
				nodeFr, _, err := bt.buildPage(uint32(path.n.Children[0]))

				if err != nil {
					panic(err.Error())
				}

				bt.mu.Lock()
				bt.Root = uint32(path.n.Children[0])
				// node.Page.SetAsRoot()
				bt.cache.SetNewRoot(nodeFr)

				// mark old root for deletion
				// path.n.Page.MarkAsDead()
				err = nodeFr.MarkAsDead()

				if err != nil {
					panic(err)
				}

				err = bt.cache.SyncFrame(nodeFr, path.n.Keys, path.n.Values, path.n.Children, uint32(path.n.RightSibling), uint32(path.n.LeftSibling))

				if err != nil {
					fmt.Println("ERR SYNCING...")
					return false, err
				}

				bt.mu.Unlock()
			} else {
				// No underflow, Sync frame
				// err = path.n.Page.Sync(path.n.Keys, path.n.Values, path.n.Children, uint32(path.n.RightSibling), uint32(path.n.LeftSibling))

				pathFr, _, err := bt.buildPage(uint32(path.n.PageId))

				err = bt.cache.SyncFrame(pathFr, path.n.Keys, path.n.Values, path.n.Children, uint32(path.n.RightSibling), uint32(path.n.LeftSibling))

				if err != nil {
					fmt.Println("ERR SYNCING...")
					return false, err
				}
			}

			// mark frame as dirty
			if fr != nil {
				// fr.MarkDirty()
				err = bt.cache.MarkFrameDirty(fr)

				if err != nil {
					log.Panic(err)
				}
			}

			path.n.mu.Unlock()
			bt.cache.ReleaseFrame(fr)

		}

		st.Clear()

		return true, nil
	}

	return true, nil
}

// Create new node - does not include associated page
func createNode(separatorKeys [][]byte, isLeaf bool, isRoot bool) (*Node, error) {
	if !isLeaf && (len(separatorKeys) < DEGREE-1 && len(separatorKeys) > ORDER-1) {
		msg := fmt.Sprintf("Keys must be between %d to %d", DEGREE-1, ORDER-1)
		return nil, BTreeError{Message: msg}
	}

	if isLeaf && (len(separatorKeys) < DEGREE && len(separatorKeys) > ORDER) {
		msg := fmt.Sprintf("Keys must be from %d to %d", DEGREE, ORDER)
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
func loadNode(fr *bf.Frame) (*Node, error) {
	if fr == nil {
		return nil, BTreeError{Message: "Frame not provided"}
	}

	minPge, err := fr.GetMinPage()

	if err != nil {
		return nil, err
	}

	internalNode := fr.Internal()

	var n Node

	if internalNode {

		n = Node{
			Keys:     minPge.Keys,
			Children: minPge.Children,
			Leaf:     !internalNode,
			//			Page:         page,
			PageId:       int32(minPge.PageId),
			RightSibling: minPge.RightSibling,
			LeftSibling:  minPge.LeftSibling,
		}
	} else {
		n = Node{
			Keys:   minPge.Keys,
			Values: minPge.Vals,
			Leaf:   !internalNode,
			//			Page:         page,
			RightSibling: minPge.RightSibling,
			LeftSibling:  minPge.LeftSibling,
			PageId:       int32(minPge.PageId),
		}
	}

	return &n, nil
}

// Gets pages from cache and builds Node
func (bt *BTree) buildPage(pageId uint32) (*bf.Frame, *Node, error) {
	frame, err := bt.cache.Get(pageId)

	if err != nil {
		return nil, nil, err
	}

	node, err := loadNode(frame)

	if err != nil {
		return nil, nil, err
	}

	return frame, node, nil
}

// Retrieves value with key
func (bt *BTree) Get(key []byte) ([]byte, error) {
	var nodePageId int32
	var val []byte

	bt.mu.RLock()
	if bt.Root == 0 {
		bt.mu.RUnlock()
		return nil, BTreeError{"Key Value store not initialized"}
	}

	nodePageId = int32(bt.Root)
	fmt.Println("ROOT PAGE ID ==> ", nodePageId)
	bt.mu.RUnlock()

	var oldFrame *bf.Frame

	for nodePageId > 0 {
		// pge, err := buildPage(uint32(nodePageId))
		fr, err := bt.cache.Get(uint32(nodePageId))

		if err != nil {
			return nil, err
		}

		pge, err := loadNode(fr)

		if err != nil {
			return nil, err
		}

		if oldFrame != nil {
			fmt.Println("RELEASING OLD FRAME....")
			err = bt.cache.ReleaseFrame(oldFrame)

			if err != nil {
				panic(err)
			}
		}

		val, err = pge.search(key, &nodePageId)

		if err != nil {
			bt.cache.ReleaseFrame(fr)

			if oldFrame != nil {
				bt.cache.ReleaseFrame(oldFrame)
			}

			return nil, err
		}

		// err = bf.BCache.ReleaseFrame(fr)

		// if err != nil {
		// 	fmt.Println(fmt.Sprintf("ERROR on ReleaseFrame => ", err))
		// 	panic(err)
		// }
		oldFrame = fr
	}

	if oldFrame != nil {
		err := bt.cache.ReleaseFrame(oldFrame)

		if err != nil {
			panic(fmt.Sprintf("Unable to  release frame: ", err))
		}
	}

	return val, nil
}

// Retrieves value with key
func (bt *BTree) RangeSearch(minKey []byte, maxKey []byte, count int32) ([]RangeItem, error) {
	var nodePageId int32
	var items []RangeItem
	var rightSibId int32

	if bt.Root == 0 {
		return nil, BTreeError{"Key Value store not initialized"}
	}

	nodePageId = int32(bt.Root)
	var pge *Node
	var err error

	for nodePageId > 0 {
		_, pge, err = bt.buildPage(uint32(nodePageId))

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
		_, rightSib, err := bt.buildPage(uint32(rightSibId))

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
func (bt *BTree) Shutdown() error {
	err := bt.cache.Close()

	if err != nil {
		return err
	}

	return nil
}
