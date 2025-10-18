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
}

type Node struct {
	Keys         [][]byte     // Keys in the Node
	Children     []int32      // Internal Node children
	Leaf         bool         // Whether Node is leaf
	Values       [][]byte     // Values in leaf node
	LeftSibling  int32        // Left sibling in leaf node
	RightSibling int32        // right sibling in leaf node
	Page         *diskio.Page // Associated on disk page
	mu           sync.RWMutex
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

func (n *Node) split() (*Node, error) {
	//n.mu.Lock()
	//defer n.mu.Unlock()

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
		return rightNode, nil
	} else {
		// None leaf node split
		mid := len(n.Keys) / 2
		// promotedKey := n.Keys[mid]
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
		return rightNode, nil
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
		if len(n.Keys) > (2 * DEGREE) {
			log.Println("(insert) OVERFLOW DETECTED...")
			newRightNode, err := n.split()

			if err != nil {
				return 0, err
			}

			// persist left node

			// remove old keys from left node
			// n.postSplitCleanup(idx)

			// create  page for right node
			newRightNode.assignPage(false)

			// update sibling links
			var oldRightSibling *Node
			newRightNode.LeftSibling = n.Page.Header.PageId
			newRightNode.RightSibling = n.Page.Header.RightSibling

			if n.Page.Header.RightSibling != 0 {
				// load node update sibling link
				oldRightSiblingPage, err := diskio.BPool.FetchPage(uint32(n.Page.Header.RightSibling))

				if err != nil {
					return 0, err
				}

				oldRightSibling, err = loadNode(oldRightSiblingPage)

				if err != nil {
					return 0, err
				}

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
				oldRightSibling.Page.Sync(oldRightSibling.Keys, oldRightSibling.Values, oldRightSibling.Children, uint32(oldRightSibling.RightSibling), uint32(oldRightSibling.LeftSibling))
			}

			return 0, nil
		}

		// Sync to page
		//		if bytes.Compare(key, kAtIdx) == -1 {
		//			err = n.Page.Sync(n.Keys, n.Values, n.Children, idx, false)
		//		} else if bytes.Compare(key, kAtIdx) == 0 {
		//			err = n.Page.Sync(n.Keys, n.Values, n.Children, idx, true)
		//		} else {
		//			err = n.Page.Sync(n.Keys, n.Values, n.Children, idx+1, false)
		//		}
		err = n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))

		fmt.Println("/////// KEYS AFTER INSERT => ", n.Keys)

		if err != nil {
			return 0, err
		}

		//if bytes.Compare(key, kAtIdx) == -1 {
		//	err = n.Page.Persist(idx, false)
		//} else if bytes.Compare(key, kAtIdx) == 0 {
		//	err = n.Page.Persist(idx, true)
		//} else {
		//	err = n.Page.Persist(idx+1, false)
		//}

		//fmt.Println("/////// KEYS AFTER PERSIST => ", n.Keys)

		//if err != nil {
		//	return 0, err
		//}

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

// Deletes key and associated value from BTree. If n is an internal node, returns pageID of child to follow, -1 if error and 0 if delete was successful or key doesn't exist.
func (n *Node) deleteValue(key []byte, stack *BTStack, mr *MergeMetadata) (int32, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.Keys) <= 0 || (len(n.Values) <= len(n.Children)) {
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
			var mergeSibling *Node
			var rightMerge bool
			// Load sibling node and check if redistribution is possible
			path, err := stack.Parent()

			if err != nil || path == nil || (path != nil && path.n == nil) {
				if bTree.Root.Page.Header.PageId != n.Page.Header.PageId {
					// none root without parent
					return -1, err
				}

				// is root node. return
				return 1, nil
			}

			// check if is the right most child
			path.n.mu.RLock()
			if int(path.idx) >= len(path.n.Children)-1 || n.RightSibling <= 0 {
				// merge with left sibling
				p, err := diskio.BPool.FetchPage(uint32(n.LeftSibling))

				if err != nil {
					return -1, err
				}

				mergeSibling, err = loadNode(p)

				if err != nil {
					return -1, err
				}

				rightMerge = false
			} else {
				// merge with right sibling
				p, err := diskio.BPool.FetchPage(uint32(n.LeftSibling))

				if err != nil {
					return -1, err
				}

				mergeSibling, err = loadNode(p)

				if err != nil {
					return -1, err
				}

				rightMerge = true
			}
			path.n.mu.RUnlock()

			mergeSibling.mu.Lock()
			if len(n.Keys)+len(mergeSibling.Keys) >= DEGREE {
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
					// update separator key in parent
					//path.n.mu.Lock()
					//if bytes.Compare(key, path.n.Keys[path.idx]) == -1 {
					//	path.n.Keys[path.idx] = mergeSibling.Keys[0]
					//} else {
					//	path.n.Keys[path.idx-1] = mergeSibling.Keys[0]
					//}
					//path.n.mu.Unlock()
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
							return -1, err
						}

						leftSib, err := loadNode(pge)

						if err != nil {
							return -1, err
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
							return -1, err
						}

						rightSib, err := loadNode(pge)

						if err != nil {
							return -1, err
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

				// Sync to page
				n.Page.Sync(n.Keys, n.Values, n.Children, uint32(n.RightSibling), uint32(n.LeftSibling))
				mergeSibling.Page.Sync(mergeSibling.Keys, mergeSibling.Values, mergeSibling.Children, uint32(mergeSibling.RightSibling), uint32(mergeSibling.LeftSibling))
				mergeSibling.mu.Unlock()
			}

			mr.rightMerge = rightMerge

			return 1, nil
		}

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

func InsertValue(keys [][]byte, vals [][]byte) (bool, error) {
	for i, _ := range keys {
		log.Printf("%d Inserting {%v:%v} .................................................................\n", i, binary.LittleEndian.Uint32(keys[i]), string(vals[i]))
		// Get root page and retrieve/create node

		// stack
		st := BTStack{
			stack: make(map[int]*TraversePath),
		}

		insertRes := SplitResponse{}
		var nodePageId int32

		fmt.Println("(InsertValue) ROOT => ", bTree.Root)
		if bTree.Root == nil {
			fmt.Println("(InsertValue) NO ROOT NODE. SETTING...")
			// No root node. Set root node
			//		if len(keys) < DEGREE || len(vals) < DEGREE {
			//			return false, BTreeError{Message: fmt.Sprintf("At leasst %d keys and %d values are required.", DEGREE, DEGREE+1)}
			//		}
			//
			rootNode := Node{
				Keys:   [][]byte{keys[i]},
				Leaf:   true,
				Values: [][]byte{vals[i]},
			}

			bTree.Root = &rootNode

			// Assign page & persist
			bTree.Root.assignPage(true)

			//return true, nil
		} else {
			fmt.Println("(InsertValue) ROOT NODE FOUND...")

			nodePageId = bTree.Root.Page.Header.PageId

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

					if len(parent.n.Keys) > DEGREE*2 {
						// handle overflow
						newRightNode, err := parent.n.split()

						if err != nil {
							return false, err
						}

						// remove old keys from left node
						// parent.n.postSplitCleanup(int(parent.idx))

						// persist new right node
						newRightNode.assignPage(false)

						r := SplitResponse{
							PromotedSeparatorKey: newRightNode.Keys[0],
							NewNodeId:            uint32(newRightNode.Page.Header.PageId),
						}

						// set new Insert response
						insertRes = r

						// Sync new child
						newRightNode.Page.Sync(newRightNode.Keys, newRightNode.Values, newRightNode.Children, uint32(newRightNode.RightSibling), uint32(newRightNode.LeftSibling))

					} else {
						// reset insertres
						insertRes.NewNodeId = 0
						insertRes.PromotedSeparatorKey = make([]byte, 0)
					}

					// sync parent
					parent.n.Page.Sync(parent.n.Keys, parent.n.Values, parent.n.Children, uint32(parent.n.RightSibling), uint32(parent.n.LeftSibling))

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

					bTree.Root = newRoot

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

			if err != nil {
				return false, err
			}
		}

		//TODO: Handle propagating merges and updating parent keys

		return true, nil
	}

	return true, nil
}
