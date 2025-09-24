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
	PromotedSeparatorKey []byte // Key to promote
	NewNodeId            uint32 // Child Reference to add.
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
			Children: make([]int32, 2*DEGREE+1),
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
	n.mu.Lock()
	defer n.mu.Unlock()

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

		rightNode.assignPage(false)
		i := SplitResponse{
			PromotedSeparatorKey: rightKeys[0],
			NewNodeId:            uint32(rightNode.Page.Header.PageId),
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

		rightNode.assignPage(false)

		i := SplitResponse{
			PromotedSeparatorKey: promotedKey,
			NewNodeId:            uint32(rightNode.Page.Header.PageId),
		}

		return i, nil
	}
}

// Assign a page to node
func (n *Node) assignPage(isRoot bool) (*Node, error) {
	if n.Page != nil {
		return n, nil
	}

	if n.Leaf {
		pge, err := diskio.New(n.Keys, &n.Values, nil, isRoot)

		if err != nil {
			return nil, err
		}

		n.Page = pge
		return n, nil

	} else {
		pge, err := diskio.New(n.Keys, nil, &n.Children, isRoot)

		if err != nil {
			return nil, err
		}

		n.Page = pge
		return n, nil
	}
}

// Inserts key and value to node. If is internal node, return page ID of child page. If leaf node insert and return
func (n *Node) insert(key []byte, val []byte, rp *SplitResponse, stack *BTStack) (int32, error) {
	// TODO: cater for internal nodes
	// If empty (new node), add the values
	if len(n.Keys) <= 0 && len(n.Values) <= 0 {
		n.Keys = append(n.Keys, key)
		n.Values = append(n.Values, val)

		// Sync to page
		err := n.Page.Sync(n.Keys, n.Values, n.Children, 0, false)
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
		n.mu.Lock()
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
		n.mu.Unlock()

		// Check overflow
		if len(n.Keys) > (2 * DEGREE) {
			log.Println("(insert) SPLIT DETECTED...")
			r, err := n.split()

			if err != nil {
				return 0, err
			}

			rp = &r

			return 0, nil
		}

		// Sync to page
		if bytes.Compare(key, kAtIdx) == -1 {
			err = n.Page.Sync(n.Keys, n.Values, n.Children, idx, false)
		} else if bytes.Compare(key, kAtIdx) == 0 {
			err = n.Page.Sync(n.Keys, n.Values, n.Children, idx, true)
		} else {
			err = n.Page.Sync(n.Keys, n.Values, n.Children, idx+1, false)
		}

		fmt.Println("/////// KEYS AFTER INSERT => ", n.Keys)

		if err != nil {
			return 0, err
		}

		if bytes.Compare(key, kAtIdx) == -1 {
			err = n.Page.Persist(idx, false)
		} else if bytes.Compare(key, kAtIdx) == 0 {
			err = n.Page.Persist(idx, true)
		} else {
			err = n.Page.Persist(idx+1, false)
		}

		fmt.Println("/////// KEYS AFTER PERSIST => ", n.Keys)

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
					path, err := st.Pop()

					if err != nil {
						return false, err
					}

					path.n.mu.Lock()
					_, err = helpers.InsertToList[[]byte](&path.n.Keys, int(path.idx), insertRes.PromotedSeparatorKey)

					if err != nil {
						return false, err
					}

					_, err = helpers.InsertToList[int32](&path.n.Children, int(path.idx+1), int32(insertRes.NewNodeId))

					if err != nil {
						return false, err
					}

					if len(path.n.Keys) > DEGREE*2 {
						// handle overflow
						newRightNode, err := path.n.split()

						if err != nil {
							return false, err
						}

						// set new Insert response
						insertRes = newRightNode

					} else {
						// reset insertres
						insertRes.NewNodeId = 0
						insertRes.PromotedSeparatorKey = make([]byte, 0)
					}

				} else {
					// create and assign new root node.
					newRoot, err := createNode([][]byte{insertRes.PromotedSeparatorKey}, false, true)

					if err != nil {
						return false, err
					}

					newRoot.assignPage(true)

					bTree.Root = newRoot
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
