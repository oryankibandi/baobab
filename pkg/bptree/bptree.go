// package bptree implements a b+ tree
package bptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/oryankibandi/baobab/pkg/buffermanager"
	bm "github.com/oryankibandi/baobab/pkg/buffermanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	pgr "github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

type BpTree struct {
	meta          *bm.Frame
	root          atomic.Uint32
	buffermanager *bm.BufferManager
	// a node's order represents the minimum number of keys it can have
	// if a node has order d, it has d minimum no. of keys, a max of
	// 2d keys and 2d+1 pointers.
	order    uint32
	numPages uint64
	wal      *wal.WAL

	mu sync.RWMutex
}

func (bp *BpTree) updateRootPage(pid uint32) error {
	bp.meta.Acquire(false)
	defer bp.meta.Release(false)
	buff, _, e := bp.meta.RawBufferSlice()

	if e != nil {
		return e
	}
	binary.LittleEndian.PutUint32((*buff)[:4], pid)
	bp.meta.MarkDirty()

	// ref new root page
	_, _, err := bp.buffermanager.Get(pid)
	if err != nil {
		return err
	}

	// unref old root page
	currRoot, _, err := bp.buffermanager.Get(bp.root.Load())
	if err != nil {
		return err
	}

	currRoot.Unreference()
	bp.root.Store(pid)
	return nil
}

// split splits an overflowed frame and redistributes the keys and values/ptrs
// returns the promoted separator key, new right node pid and error if any
func (bp *BpTree) split(fr *[]byte) (sepKey []byte, newFramePid uint32, e error) {
	isInternal := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)
	// check if full
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount <= 2*pgr.ORDER {
		return nil, 0, BTreeError{Message: "provided frame has not overflown"}
	}

	// request new frame from buffermanager
	newFr, err := bp.buffermanager.NewFrame(isInternal, false)
	if err != nil {
		return nil, 0, err
	}

	defer newFr.Unreference()
	newFr.Acquire(false)
	defer newFr.Release(false)
	newFrBuff, _, err := newFr.RawBufferSlice()
	if err != nil {
		panic(err.Error())
	}

	// move items from left node to right(new) node and update upper and lower offsets
	var seperatorKey []byte
	var rightNodeRightPtr uint32
	// var cellPtr [pgr.CELL_POINTER_SIZE_BYTE]byte
	var cellOff uint32
	var cellEndOff uint32
	var cellKeySize uint32
	var cellValSize uint32
	var cellSize uint32
	var newFrCellPtrIdx uint32
	var newFrCellOffset uint32 = pgr.PAGE_SIZE_BYTES - pgr.LOWER_PADDING_BYTES
	leftNodeUpperOffset := binary.LittleEndian.Uint32((*fr)[25:29])
	for i := uint32(pgr.ORDER); i < itemCount; i++ {
		cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+5])

		// read cell sizes
		cellKeySize = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
		cellValSize = binary.LittleEndian.Uint32((*fr)[cellOff+5 : cellOff+9])
		cellEndOff = cellOff + (13 + cellKeySize + cellValSize)
		cellSize = 13 + cellKeySize + cellValSize

		if i == pgr.ORDER {
			// first sep key promoted to parent
			seperatorKey = make([]byte, cellKeySize)
			copy(seperatorKey, (*fr)[cellOff+13:cellOff+13+cellKeySize])
			if isInternal {
				// store old right child metadata and update to new right child(curr cell page pointer)
				rightNodeRightPtr = binary.LittleEndian.Uint32((*fr)[39:43])
				copy((*fr)[39:43], (*fr)[cellOff+9:cellOff+13])
				continue
			}
		}

		// update cell offset in new frame cell pointer
		newFrCellOffset -= cellSize
		// binary.LittleEndian.PutUint32(cellPtr[1:], uint32(newFrCellOffset))
		if i == pgr.ORDER {
			newFrCellPtrIdx = 0
		} else {
			if isInternal {
				newFrCellPtrIdx = i - pgr.ORDER - 1
			} else {
				newFrCellPtrIdx = i - pgr.ORDER
			}
		}
		binary.LittleEndian.PutUint32((*newFrBuff)[(pgr.HEADER_SIZE_BYTES+(newFrCellPtrIdx*pgr.CELL_POINTER_SIZE_BYTE))+1:pgr.HEADER_SIZE_BYTES+(newFrCellPtrIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+5], newFrCellOffset)

		// copy cell to new frame
		copy((*newFrBuff)[newFrCellOffset:newFrCellOffset+cellSize], (*fr)[cellOff:cellEndOff])

		// update upper offset
		binary.LittleEndian.PutUint32((*newFrBuff)[25:29], newFrCellOffset)
		leftNodeUpperOffset += cellSize
		binary.LittleEndian.PutUint32((*fr)[25:29], leftNodeUpperOffset)

		// update cell offset
		// newFrCellOffset -= cellSize

		// clear old cell pointer and cell offset
		clear((*fr)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE) : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE])
		clear((*fr)[cellOff:cellEndOff])
	}

	// update new node's right child
	binary.LittleEndian.PutUint32((*newFrBuff)[39:43], rightNodeRightPtr)

	// update numItems in each node/frame and lower offset
	binary.LittleEndian.PutUint32((*fr)[17:21], pgr.ORDER)
	binary.LittleEndian.PutUint32((*fr)[29:33], pgr.HEADER_SIZE_BYTES+(pgr.ORDER*pgr.CELL_POINTER_SIZE_BYTE))

	if isInternal {
		// new internal node will have itemcount-order-1 due to promoted key
		binary.LittleEndian.PutUint32((*newFrBuff)[17:21], itemCount-pgr.ORDER-1)
		binary.LittleEndian.PutUint32((*newFrBuff)[29:33], (pgr.HEADER_SIZE_BYTES + ((itemCount - pgr.ORDER - 1) * pgr.CELL_POINTER_SIZE_BYTE)))
	} else {
		binary.LittleEndian.PutUint32((*newFrBuff)[17:21], itemCount-pgr.ORDER)
		binary.LittleEndian.PutUint32((*newFrBuff)[29:33], (pgr.HEADER_SIZE_BYTES + ((itemCount - pgr.ORDER) * pgr.CELL_POINTER_SIZE_BYTE)))
	}

	// update sibling pointers
	rightSiblingPid := binary.LittleEndian.Uint32((*fr)[43:47])
	if rightSiblingPid > 0 {
		rightFrame, _, err := bp.buffermanager.Get(rightSiblingPid)
		if err != nil {
			return nil, 0, err
		}

		defer rightFrame.Unreference()
		rightFrame.Acquire(false)
		defer rightFrame.Release(false)

		// update right sibling's left sibling pointer
		siblBuff, _, err := rightFrame.RawBufferSlice()
		if err != nil {
			return nil, 0, err
		}
		copy((*siblBuff)[47:51], (*newFrBuff)[1:5])

		// update new frame's right sibling pointer
		copy((*newFrBuff)[43:47], (*siblBuff)[1:5])
	}

	// update new frame's left sibling pointer
	copy((*newFrBuff)[47:51], (*fr)[1:5])
	// update left frame's  right sibling pointer
	copy((*fr)[43:47], (*newFrBuff)[1:5])

	// defragment left node if fragmented
	defragmentNode(fr)

	// mark nodes dirty
	helpers.SetFlag(&(*fr)[0], []int{pgr.Dirty})
	helpers.SetFlag(&(*newFrBuff)[0], []int{pgr.Dirty})

	fmt.Println("PRINTING LEFT NODE AFTER SPLIT=========================> ", binary.LittleEndian.Uint32((*fr)[1:5]))
	printNodeContent(fr)
	fmt.Println("PRINTING RIGHT NODE AFTER SPLIT=========================", binary.LittleEndian.Uint32((*newFrBuff)[1:5]))
	printNodeContent(newFrBuff)
	fmt.Println("======================================================")
	fmt.Printf("PRINTING NEW NODE LOWER OFFSET --> %d\n", binary.LittleEndian.Uint32((*newFrBuff)[29:33]))
	fmt.Printf("PRINTING NEW NODE UPPER OFFSET --> %d\n", binary.LittleEndian.Uint32((*newFrBuff)[25:29]))
	fmt.Println("======================================================")
	fmt.Printf("PRINTING LEFT NODE LOWER OFFSET --> %d\n", binary.LittleEndian.Uint32((*fr)[29:33]))
	fmt.Printf("PRINTING LEFT NODE UPPER OFFSET --> %d\n", binary.LittleEndian.Uint32((*fr)[25:29]))
	fmt.Println("======================================================")
	fmt.Printf("PRINTING LEFT NODE CELL AFTER UPPER OFFSET\n")
	fmt.Printf("%v\n", (*fr)[binary.LittleEndian.Uint32((*fr)[25:29])-157:binary.LittleEndian.Uint32((*fr)[25:29])])
	fmt.Printf("PRINTING LEFT NODE CELL AT UPPER OFFSET\n")
	fmt.Printf("%v\n", (*fr)[binary.LittleEndian.Uint32((*fr)[25:29]):8176])
	fmt.Println("======================================================")

	return seperatorKey, binary.LittleEndian.Uint32((*newFrBuff)[1:5]), nil
}

// merge merges left node and right node.
// in the case that the keys can be redistributed, the nodes are
// rebalanced and the new seperator key will be returned.
// Latches for both nodes should be acquired before calling merge()
// Merge always merges the right node to the left node unless the
// underflowed node has no immediate left sibling.
// leftMerge is true if the underflown node is the left node(has no immediate left sibling).
// reurns newSepKey if rebalanced or error if any
func (bp *BpTree) merge(leftNode *[]byte, rightNode *[]byte, sepKey []byte, leftMerge bool) (newSepKey []byte, e error) {
	if leftNode == nil {
		return nil, BTreeError{Message: "no left node provided"}
	}

	if rightNode == nil {
		return nil, BTreeError{Message: "no right node provided"}
	}

	if helpers.BitIsSet(&(*leftNode)[0], pgr.IsInternal) != helpers.BitIsSet(&(*rightNode)[0], pgr.IsInternal) {
		return nil, BTreeError{Message: "different node types provided"}
	}

	internalNodeMerge := helpers.BitIsSet(&(*leftNode)[0], pgr.IsInternal)

	// 0. pull down seperator key
	// 1. check if nodes can be mergedi - if a node can hold N+1 pointers and
	//    total number of child pointers in both nodes is <= N+1
	// 2. if can rebalance, more n items from more populated node to the less populated node and
	//    return the left  most key of the right node as the seperator key & merge-false
	// 3. if cannot rebalance:
	//	- move keys from right node to left leftNode
	//	- move pointers from right node to left node
	//	- demote parent seperator key and add it to the keys
	//	- return deleted node pid and merge=true

	leftNodeItemCount := binary.LittleEndian.Uint32((*leftNode)[17:21])
	rightNodeItemCount := binary.LittleEndian.Uint32((*rightNode)[17:21])

	if leftNodeItemCount+1 >= pgr.ORDER+1 && rightNodeItemCount+1 >= pgr.ORDER+1 {
		// both nodes already above the threshold. No merge or rebalancing required
		return nil, BTreeError{Message: "Both nodes provided have not underflown"}
	}

	var deficit uint32
	if leftNodeItemCount+1+rightNodeItemCount+1 > (pgr.ORDER*2)+1 {
		// rebalance
		if leftNodeItemCount == rightNodeItemCount {
			// nodes balanced
			return nil, nil
		}

		var donorDirection bool // true if moving items from right to left node, else false
		if leftNodeItemCount > rightNodeItemCount {
			donorDirection = false
			deficit = uint32(pgr.ORDER - rightNodeItemCount)
		} else {
			donorDirection = true
			deficit = uint32(pgr.ORDER - leftNodeItemCount)
		}

		// 1. demote separator key
		if internalNodeMerge {
			err := bp.insertToFrame(rightNode, sepKey, 0, nil)
			if err != nil {
				return nil, err
			}
		}

		if !donorDirection {
			// var e error
			// left to right
			for deficit > 0 {
				// remove last item from left node
				lastKey, e := bp.getLastKey(leftNode)
				if e != nil {
					return nil, e
				}

				childPtr, v, e := bp.deleteFromNode(leftNode, lastKey, false)
				if e != nil {
					return nil, e
				}

				e = bp.insertToFrame(rightNode, lastKey, childPtr, v)
				if e != nil {
					return nil, e
				}
				deficit--
			}
		} else {
			// right to left
			for deficit > 0 {
				// remove first item from rightNode
				firstKey, e := bp.getFirstKey(rightNode)
				if e != nil {
					return nil, e
				}

				childPtr, v, e := bp.deleteFromNode(rightNode, firstKey, true)
				if e != nil {
					return nil, e
				}

				e = bp.insertToFrame(leftNode, firstKey, childPtr, v)
				if e != nil {
					return nil, e
				}
				deficit--
			}
		}

		// get the first key from the right node to be the seperator key
		newSeperatorKey, e := bp.getFirstKey(rightNode)
		if e != nil {
			return nil, e
		}

		if internalNodeMerge {
			// for internal nodes we delete the first key of the right
			// node and return it as the new seperator key
			var ptr uint32
			if !donorDirection {
				ptr, _, e = bp.deleteFromNode(rightNode, newSeperatorKey, true)
			} else {
				ptr, _, e = bp.deleteFromNode(rightNode, newSeperatorKey, false)
			}

			if e != nil {
				return nil, e
			}

			// ptr should be 0, since we demoted the seperator key without any child pointer
			if ptr != 0 {
				panic(fmt.Errorf("expected no pointer but got, %d", ptr))
			}
		}

		// mark both nodes dirty
		helpers.SetFlag(&(*leftNode)[0], []int{pgr.Dirty})
		helpers.SetFlag(&(*rightNode)[0], []int{pgr.Dirty})

		return newSeperatorKey, nil
	} else {
		// merge
		if leftMerge {
			// moving items from the left node to the right node

			// 1. demote separator key
			if internalNodeMerge {
				err := bp.insertToFrame(rightNode, sepKey, 0, nil)
				if err != nil {
					return nil, err
				}
			}

			rightNodeItemCount = binary.LittleEndian.Uint32((*rightNode)[17:21])
			deficit = rightNodeItemCount - leftNodeItemCount

			// move keys from left node to right node
			for range leftNodeItemCount {
				lastKey, err := bp.getLastKey(leftNode)
				if err != nil {
					return nil, err
				}

				ptr, v, err := bp.deleteFromNode(leftNode, lastKey, false)
				if err != nil {
					return nil, err
				}

				err = bp.insertToFrame(rightNode, lastKey, ptr, v)
				if err != nil {
					return nil, err
				}
			}

			// update sibling pointers
			copy((*rightNode)[47:51], (*leftNode)[47:51])

			// retrieve leftNode's left sibling and update it's right sibling pointer
			if lc := binary.LittleEndian.Uint32((*leftNode)[47:51]); lc != 0 {
				lSib, _, err := bp.buffermanager.Get(lc)
				if err != nil {
					panic(fmt.Sprintf("Unable to retrieve left node's left sibling: %s", err.Error()))
				}

				lSib.Acquire(false)
				lSibFr, _, err := lSib.RawBufferSlice()
				if err != nil {
					lSib.Release(false)
					panic(fmt.Sprintf("No buffer attached to frame retrieved: %s", err.Error()))
				}

				copy((*lSibFr)[43:47], (*leftNode)[43:47])
				lSib.Release(false)
				lSib.Unreference()
			}

			// mark left node as dead
			helpers.SetFlag(&(*leftNode)[0], []int{pgr.Dead, pgr.Dirty})

			// mark right node as dirty
			helpers.SetFlag(&(*rightNode)[0], []int{pgr.Dirty})
		} else {
			// moving items from right node to left node
			// 1. demote separator key
			if internalNodeMerge {
				err := bp.insertToFrame(rightNode, sepKey, 0, nil)
				if err != nil {
					return nil, err
				}
			}

			printNodeContent(rightNode)

			rightNodeItemCount = binary.LittleEndian.Uint32((*rightNode)[17:21])

			deficit = leftNodeItemCount - rightNodeItemCount

			// move keys from right node to left node
			for range rightNodeItemCount {
				firstKey, err := bp.getFirstKey(rightNode)
				if err != nil {
					return nil, err
				}

				ptr, v, err := bp.deleteFromNode(rightNode, firstKey, false)
				if err != nil {
					return nil, err
				}

				err = bp.insertToFrame(leftNode, firstKey, ptr, v)
				if err != nil {
					return nil, err
				}

			}

			// update sibling pointers
			copy((*leftNode)[43:47], (*rightNode)[43:47])

			// retrieve rightNode's right sibling and update it's left sibling pointer
			if rc := binary.LittleEndian.Uint32((*rightNode)[43:47]); rc != 0 {
				rSib, _, err := bp.buffermanager.Get(rc)
				if err != nil {
					panic(fmt.Sprintf("Unable to retrieve right node's right sibling: %s", err.Error()))
				}

				rSib.Acquire(false)
				rSibFr, _, err := rSib.RawBufferSlice()
				if err != nil {
					rSib.Release(false)
					panic(fmt.Sprintf("No buffer attached to frame retrieved: %s", err.Error()))
				}

				copy((*rSibFr)[47:51], (*rightNode)[47:51])
				rSib.Release(false)
				rSib.Unreference()
			}

			// mark right node as dead
			helpers.SetFlag(&(*rightNode)[0], []int{pgr.Dead, pgr.Dirty})

			// mark left node as dirty
			helpers.SetFlag(&(*leftNode)[0], []int{pgr.Dirty})
		}

		//
		return nil, nil
	}
}

// insertToFrame inserts key and value/childPtr to a frame and shifts
// cellpointers if necessary.
func (bp *BpTree) insertToFrame(fr *[]byte, key []byte, childPtr uint32, val []byte) error {
	if fr == nil {
		return BTreeError{Message: "No frame provided"}
	}

	if key == nil {
		return BTreeError{Message: "No key provided"}
	}

	internal := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)

	if !internal && val == nil {
		return BTreeError{Message: "No value provided"}
	}

	keyLen := len(key)
	valLen := len(val)
	cellSize := 13 + keyLen + valLen
	upperOffset := binary.LittleEndian.Uint32((*fr)[25:29])
	cellStartOff := upperOffset - uint32(cellSize)
	fmt.Printf("CELLSIZE -> %d\tCELLSTARTOFF -> %d\tUPPEROFFSET -> %d\n", cellSize, cellStartOff, upperOffset)
	fmt.Printf("PRINTING CELLDATA AFTER UPPER OFFSET --------->\n")
	fmt.Printf("%v\n", (*fr)[upperOffset-157:upperOffset])
	fmt.Printf("PRINTING CELLDATA BEFORE UPPER OFFSET --------->\n")
	fmt.Printf("%v\n", (*fr)[upperOffset:8176])
	// set new upper offset
	// binary.LittleEndian.PutUint32((*fr)[25:29], cellStartOff)

	// find appropriate index to add key and shift keys if need be.
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])

	var cOff uint32
	var ptrOff uint32
	// var cKSize uint32
	var insertIdx uint32 = itemCount

	idx, e := findInsertionIdx(fr, key, 0, itemCount-1)
	if e != nil {
		return e
	}

	if idx < 0 {
		return BTreeError{Message: "Unable to insert index"}
	}

	insertIdx = uint32(idx)
	ptrOff = insertIdx*pgr.CELL_POINTER_SIZE_BYTE + pgr.HEADER_SIZE_BYTES
	cOff = binary.LittleEndian.Uint32((*fr)[ptrOff+1:])
	fmt.Printf("PRINTING CELL CONTENTS BEFORE UPDATE inserting -> %s -----------------------------------\n", key)
	fmt.Printf("Insert IDX -> %d\tItem Count -> %d\n", insertIdx, itemCount)
	printNodeContent(fr)

	// write cell contents
	binary.LittleEndian.PutUint32((*fr)[cellStartOff+1:cellStartOff+5], uint32(keyLen))
	binary.LittleEndian.PutUint32((*fr)[cellStartOff+5:cellStartOff+9], uint32(valLen))
	copy((*fr)[cellStartOff+13:cellStartOff+13+uint32(keyLen)], key)
	fmt.Printf("PRINTING CELL CONTENTS MID-UPDATE inserting -> %s -----------------------------------\n", key)
	printNodeContent(fr)
	if internal {
		if insertIdx == itemCount {
			// set childPtr as right child pointer in header and move the previous
			// right child pointer to the same cell as the new key
			// +------+------+------+------+
			// |  K1  |  K2  |  K3  |      |
			// +------+------+------+  P4  + <- right child ptr(in header)
			// |  P1  |  P2  |  P3  |      |
			// +------+------+------+------+

			copy((*fr)[cellStartOff+9:cellStartOff+13], (*fr)[39:43])
			binary.LittleEndian.PutUint32((*fr)[39:43], childPtr)
		} else {
			// If no child pointer provided, leave the childPtr slot empty
			// This happens briefly during merging/rebalancing when the separator key is demoted.
			// It's not permanent and the operation leaves no empty child pointers.
			if childPtr != 0 {
				// add child ptr at current index as pointer to our new cell
				copy((*fr)[cellStartOff+9:cellStartOff+13], (*fr)[cOff+9:cOff+13])
				// write new child ptr to cell at curr idx.
				binary.LittleEndian.PutUint32((*fr)[cOff+9:cOff+13], childPtr)
			}
		}
	} else {
		copy((*fr)[cellStartOff+13+uint32(keyLen):cellStartOff+13+uint32(keyLen)+uint32(valLen)], val)
	}

	fmt.Printf("PRINTING CELL CONTENTS BEFORE CELL POINTER INSERTION -> %s -----------------------------------\n", key)
	printNodeContent(fr)
	// insert cell pointer
	lowOff := binary.LittleEndian.Uint32((*fr)[29:33])
	fmt.Printf("LOW OFFSET --> %d\n", lowOff)
	fmt.Printf("FIRST 100 BYTES -> %v\n", (*fr)[:100])
	if insertIdx == itemCount {
		fmt.Printf("insertIdx == itemCount\n")
		// add item to end of ptr list
		if internal && childPtr != 0 {
			helpers.SetFlag(&(*fr)[lowOff], []int{pgr.HasChildPtr})
		}
		binary.LittleEndian.PutUint32((*fr)[lowOff+1:lowOff+5], cellStartOff)
	} else {
		fmt.Printf("INSERT IDX --> %d\n", insertIdx)
		fmt.Printf("ITEM COUNT --> %d\n", itemCount)
		fmt.Printf("Copying to destination [%d:%d]\n", pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE, lowOff+pgr.CELL_POINTER_SIZE_BYTE)
		fmt.Printf("Copying from  [%d:%d]\n", pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE), lowOff)
		printNodeContent(fr)
		// shift items to the right to create space for new cell pointer
		copy((*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE:lowOff+pgr.CELL_POINTER_SIZE_BYTE], (*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff])

		if internal && childPtr != 0 {
			helpers.SetFlag(&(*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)], []int{pgr.HasChildPtr})
		}

		binary.LittleEndian.PutUint32((*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+1:pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE], cellStartOff)
	}
	fmt.Printf("PRINTING CELL CONTENTS AFTER UPDATE-----------------------------------")

	// update itemcount, lowoffset and upperoffset
	binary.LittleEndian.PutUint32((*fr)[17:21], itemCount+1)
	binary.LittleEndian.PutUint32((*fr)[29:33], lowOff+pgr.CELL_POINTER_SIZE_BYTE)
	binary.LittleEndian.PutUint32((*fr)[25:29], cellStartOff)
	printNodeContent(fr)
	fmt.Printf("FRAME --> %v\n", *fr)

	return nil
}

// insertToNewRoot inserts to new root a key and child pointers.
// This occurs after a split and a new root page is created.
//
//	+-----------+------+------+------------+
//	|    key    |      |      |            |
//	+-----------+------+------+  rightPtr  +
//	|  leftPtr  |      |      |            |
//	+-----------+------+------+------------+
func (bp *BpTree) insertToNewRoot(rootBuff *[]byte, key []byte, leftPtr uint32, rightPtr uint32) error {
	if key == nil {
		return BTreeError{Message: "No key provided"}
	}

	if leftPtr == 0 {
		return BTreeError{Message: "Left child pointer is required"}
	}

	if rightPtr == 0 {
		return BTreeError{Message: "Right child pointer is required"}
	}

	kLen := len(key)
	cellSize := 13 + kLen
	cellOff := pgr.PAGE_SIZE_BYTES - (pgr.LOWER_PADDING_BYTES + cellSize)

	// add key and left ptr to cell
	binary.LittleEndian.PutUint32((*rootBuff)[cellOff+1:cellOff+5], uint32(kLen))
	binary.LittleEndian.PutUint32((*rootBuff)[cellOff+9:cellOff+13], leftPtr)
	copy((*rootBuff)[cellOff+13:cellOff+13+kLen], key)

	// add cell pointer
	binary.LittleEndian.PutUint32((*rootBuff)[pgr.HEADER_SIZE_BYTES+1:pgr.HEADER_SIZE_BYTES+5], uint32(cellOff))

	// add right child pointer to header & update page metadata
	binary.LittleEndian.PutUint32((*rootBuff)[39:43], rightPtr)

	binary.LittleEndian.PutUint32((*rootBuff)[17:21], 1)                                                // item count
	binary.LittleEndian.PutUint32((*rootBuff)[25:29], uint32(cellOff))                                  // upper offset
	binary.LittleEndian.PutUint32((*rootBuff)[29:33], pgr.HEADER_SIZE_BYTES+pgr.CELL_POINTER_SIZE_BYTE) // lower offset

	return nil
}

// deleteFromNode - deletes a key from a node/frame/page by deleting cell pointers and rearranging them to occupy any holes left
// if the node is a non-leaf node, the child pointer is returned.
// returns deleted key's pointer or value or error if any
func (bp *BpTree) deleteFromNode(fr *[]byte, key []byte, leftMerge bool) (ptr uint32, val []byte, e error) {
	if fr == nil {
		return 0, nil, BTreeError{Message: "No frame provided"}
	}

	if key == nil {
		return 0, nil, BTreeError{Message: "No frame provided"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount == 0 {
		return 0, nil, BTreeError{Message: "Frame has no keys"}
	}

	internal := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)

	var delIdx int32 = -1
	var cOff uint32
	var ptrOff uint32

	idx, e := findKeyIndex(fr, key, 0, itemCount-1)
	if e != nil {
		return 0, nil, e
	}
	if idx < 0 {
		return 0, nil, BTreeError{Message: "Could not find key to delete"}
	}

	delIdx = idx
	ptrOff = (uint32(delIdx) * pgr.CELL_POINTER_SIZE_BYTE) + pgr.HEADER_SIZE_BYTES
	cOff = binary.LittleEndian.Uint32((*fr)[ptrOff+1 : ptrOff+5])

	if !internal {
		vLen := binary.LittleEndian.Uint32((*fr)[cOff+5 : cOff+9])
		kLen := binary.LittleEndian.Uint32((*fr)[cOff+1 : cOff+5])
		val := (*fr)[cOff+13+kLen : cOff+13+kLen+vLen]

		// remove cell ptr
		lowOff := binary.LittleEndian.Uint32((*fr)[29:33])
		clear((*fr)[(delIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES : delIdx*pgr.CELL_POINTER_SIZE_BYTE+pgr.HEADER_SIZE_BYTES+pgr.CELL_POINTER_SIZE_BYTE])
		binary.LittleEndian.PutUint32((*fr)[29:33], uint32(lowOff-pgr.CELL_POINTER_SIZE_BYTE))
		if delIdx != int32(itemCount-1) {
			// need to shift pointers to cover the gap
			copy((*fr)[pgr.HEADER_SIZE_BYTES+(delIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff], (*fr)[pgr.HEADER_SIZE_BYTES+pgr.CELL_POINTER_SIZE_BYTE+(delIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff+pgr.CELL_POINTER_SIZE_BYTE])
		}

		// decrement item count
		binary.LittleEndian.PutUint32((*fr)[17:21], itemCount-1)

		return 0, val, nil
	}

	// internal node
	// for internal nodes, deletion is usualy as a result of a merge propagating from lower levels.
	// since we mostly merge right sibling to the left sibling, the right child pointer of a key will need to be
	// removed. This right child pointer of a key will be stored in the next cells or if it's the right most key,
	// it will be stored in the header.
	var deletedChildPtr uint32
	hasChildPtr := helpers.BitIsSet(&(*fr)[(delIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES], pgr.HasChildPtr)
	lowOff := binary.LittleEndian.Uint32((*fr)[29:33])
	clear((*fr)[(delIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES : delIdx*pgr.CELL_POINTER_SIZE_BYTE+pgr.HEADER_SIZE_BYTES+pgr.CELL_POINTER_SIZE_BYTE])
	binary.LittleEndian.PutUint32((*fr)[29:33], uint32(lowOff-pgr.CELL_POINTER_SIZE_BYTE))

	if delIdx == int32(itemCount-1) {
		// last item being deleted
		// set the cell's child ptr as the right most child in header
		if !leftMerge {
			cPtr := binary.LittleEndian.Uint32((*fr)[cOff+9 : cOff+13])
			deletedChildPtr = binary.LittleEndian.Uint32((*fr)[39:43])
			binary.LittleEndian.PutUint32((*fr)[39:43], cPtr)
		} else {
			deletedChildPtr = binary.LittleEndian.Uint32((*fr)[cOff+9 : cOff+13])
		}
	} else {
		if (!leftMerge && hasChildPtr) || (leftMerge && !hasChildPtr) {
			currCellPtr := binary.LittleEndian.Uint32((*fr)[cOff+9 : cOff+13])
			nextCellOff := binary.LittleEndian.Uint32((*fr)[ptrOff+pgr.CELL_POINTER_SIZE_BYTE+1 : ptrOff+pgr.CELL_POINTER_SIZE_BYTE+5])
			// store cell pointer that will be deleted
			deletedChildPtr = binary.LittleEndian.Uint32((*fr)[nextCellOff+9 : nextCellOff+13])
			binary.LittleEndian.PutUint32((*fr)[nextCellOff+9:nextCellOff+13], currCellPtr)
		} else {
			deletedChildPtr = binary.LittleEndian.Uint32((*fr)[cOff+9 : cOff+13])
		}

		// shift cell pointers
		copy((*fr)[pgr.HEADER_SIZE_BYTES+(delIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff], (*fr)[pgr.HEADER_SIZE_BYTES+pgr.CELL_POINTER_SIZE_BYTE+(delIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff+pgr.CELL_POINTER_SIZE_BYTE])
	}

	// decrement item count
	binary.LittleEndian.PutUint32((*fr)[17:21], itemCount-1)

	return deletedChildPtr, nil, nil
}

// getFirstKey returns the first key of the frame or error if any.
// atleast a shared latch must be acquired before calling this function
func (bp *BpTree) getFirstKey(fr *[]byte) (k []byte, err error) {
	if fr == nil {
		return nil, BTreeError{Message: "No frame provided"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount == 0 {
		return nil, BTreeError{Message: "Frame has not been provided. "}
	}

	cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+1 : pgr.HEADER_SIZE_BYTES+5])
	if cellOff == 0 || cellOff > pgr.PAGE_SIZE_BYTES {
		return nil, BTreeError{Message: fmt.Sprintf("Invalid cell offset: %d", cellOff)}
	}

	kLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
	return (*fr)[cellOff+13 : cellOff+13+kLen], nil
}

// getLastKey returns the last key of the frame or error if any.
// atleast a shared latch must be acquired before calling this function
func (bp *BpTree) getLastKey(fr *[]byte) (k []byte, err error) {
	if fr == nil {
		return nil, BTreeError{Message: "No frame provided"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount == 0 {
		return nil, BTreeError{Message: "Frame has not been provided. "}
	}
	cellPtrIdx := itemCount - 1
	cellPtrOffset := pgr.HEADER_SIZE_BYTES + (cellPtrIdx * pgr.CELL_POINTER_SIZE_BYTE)
	cellOff := binary.LittleEndian.Uint32((*fr)[cellPtrOffset+1 : cellPtrOffset+5])
	if cellOff == 0 || cellOff > pgr.PAGE_SIZE_BYTES {
		return nil, BTreeError{Message: fmt.Sprintf("Invalid cell offset: %d", cellOff)}
	}

	kLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
	return (*fr)[cellOff+13 : cellOff+13+kLen], nil
}

// getKeyAtIdx retrieves key at provided index
func (bp *BpTree) getKeyAtIdx(fr *[]byte, idx uint32) (key []byte, e error) {
	if fr == nil {
		return nil, BTreeError{Message: "No frame buffer provided"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if idx > itemCount-1 {
		return nil, BTreeError{Message: fmt.Sprintf("Invalid index provided: %d", idx)}
	}

	cOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5])

	kLen := binary.LittleEndian.Uint32((*fr)[cOff+1 : cOff+5])
	k := (*fr)[cOff+13 : cOff+13+kLen]

	return k, nil
}

// getKeyAtIdx retrieves key at provided index
func (bp *BpTree) getPtrAtIdx(fr *[]byte, idx uint32) (ptr uint32, e error) {
	fmt.Printf("(getPtrAtIdx) IDX ==> %d\n", idx)
	if fr == nil {
		return 0, BTreeError{Message: "No frame buffer provided"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	fmt.Printf("(getPtrAtIdx) ITEMCOUNT --> %d\n", itemCount)
	if idx > itemCount {
		return 0, BTreeError{Message: fmt.Sprintf("Invalid index provided: %d", idx)}
	}

	if idx == itemCount {
		// right most child pointer.
		return binary.LittleEndian.Uint32((*fr)[39:43]), nil
	}

	cOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5])
	childPtr := binary.LittleEndian.Uint32((*fr)[cOff+9 : cOff+13])
	fmt.Printf("(getPtrAtIdx) COFF --> %d\n", cOff)
	fmt.Printf("(getPtrAtIdx) CHILDPTR --> %d\n", childPtr)

	return childPtr, nil
}

// Insert - inserts a new key and value to the index ahd performs splits incase of an overflow
// returns error if any
func (bp *BpTree) Insert(key []byte, val []byte) error {
	var currNode *buffermanager.Frame
	var currNodeBuff *[]byte
	var prevNode *buffermanager.Frame
	var err error
	var isLeaf bool

	rootPid := bp.root.Load()
	if rootPid == 0 {
		bp.mu.Lock()
		// no root set yet
		currNode, err = bp.buffermanager.NewFrame(false, true)
		if err != nil {
			return err
		}

		currNode.Acquire(false)
		currNodeBuff, _, err := currNode.RawBufferSlice()
		if err != nil {
			currNode.Release(false)
			currNode.Unreference()
			return err
		}

		err = bp.insertToFrame(currNodeBuff, key, 0, val)
		if err != nil {
			currNode.Release(false)
			currNode.Unreference()
			return err
		}

		// set node as new root
		bp.root.Store(currNode.GetPage().PageId)
		bp.mu.Unlock()
		currNode.Release(false)
		currNode.Unreference()

		return nil
	}

	// initialize BTStack
	btPath, err := NewBTStack(rootPid)
	if err != nil {
		panic(err.Error())
	}
	defer btPath.Clear()

	var treeHeight uint32 = 0
	// add root node to path
	btPath.Add(&TraversePath{pid: rootPid, idx: 0, height: treeHeight})
	treeHeight++

	currNode, _, err = bp.buffermanager.Get(rootPid)
	if err != nil {
		return err
	}

	// acquire exlusive latch
	currNode.Acquire(false)

	currNodeBuff, _, err = currNode.RawBufferSlice()
	if err != nil {
		return err
	}

	isLeaf = !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal)

	var idx int32
	var ptr uint32
	for !isLeaf {
		if prevNode != nil {
			prevNode.Release(false)
			prevNode.Unreference()
		}

		// itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
		_, ptr, err = findKeyPath(currNodeBuff, key)
		if err != nil {
			currNode.Release(false)
			currNode.Unreference()
			return err
		}

		if ptr == 0 {
			panic("retrieved invalid child ptr pid: 0")
		}

		// add node to traverse path
		// btPath.Add(&TraversePath{pid: ptr, idx: uint32(idx), height: treeHeight})
		// treeHeight++

		prevNode = currNode
		currNode, _, err = bp.buffermanager.Get(ptr)
		if err != nil {
			prevNode.Release(false)
			prevNode.Unreference()
			return err
		}

		if currNode == nil {
			panic(fmt.Sprintf("No node with pid %d retrieved", ptr))
		}

		// acquire node's exclusive latch
		currNode.Acquire(false)

		currNodeBuff, _, err = currNode.RawBufferSlice()
		if err != nil {
			prevNode.Release(false)
			prevNode.Unreference()
			return err
		}

		// check if is a leaf node
		isLeaf = !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal)

		if !isLeaf {
			// add node to traverse path
			btPath.Add(&TraversePath{pid: ptr, idx: uint32(idx), height: treeHeight})
			treeHeight++
		}
	}

	// obtained leaf node
	itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
	possibleOverflow := itemCount >= (pgr.ORDER * 2)
	if !possibleOverflow && prevNode != nil {
		// no possible overflow, release previous node's latch
		prevNode.Release(false)
		prevNode.Unreference()
	}

	insertionIdx, err := findInsertionIdx(currNodeBuff, key, 0, itemCount-1)
	if err != nil {
		if possibleOverflow && prevNode != nil {
			prevNode.Release(false)
			prevNode.Unreference()
		}

		currNode.Release(false)
		currNode.Unreference()
		return err
	}

	keyAtIdx, err := bp.getKeyAtIdx(currNodeBuff, uint32(insertionIdx))
	if bytes.Equal(keyAtIdx, key) {
		// key already exists, no possible overflow, release prev node latch if had not.
		if possibleOverflow && prevNode != nil {
			prevNode.Release(false)
			prevNode.Unreference()
		}
		// update currently value
		// if new value is larger, create new cell and update offset else perform an in-place update
		prevUpperOff := binary.LittleEndian.Uint32((*currNodeBuff)[25:29])
		kLen := len(key)
		vLen := len(val)
		cOff := binary.LittleEndian.Uint32((*currNodeBuff)[pgr.HEADER_SIZE_BYTES+(insertionIdx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(insertionIdx*pgr.CELL_POINTER_SIZE_BYTE)+5])
		oldVlen := binary.LittleEndian.Uint32((*currNodeBuff)[cOff+5 : cOff+9])

		if vLen > int(oldVlen) {
			newUpperOff := prevUpperOff - uint32(13+kLen+vLen)
			// write cell content
			copy((*currNodeBuff)[newUpperOff:newUpperOff+5], (*currNodeBuff)[cOff:cOff+5])
			binary.LittleEndian.PutUint32((*currNodeBuff)[newUpperOff+5:newUpperOff+9], uint32(vLen))
			copy((*currNodeBuff)[newUpperOff+13:newUpperOff+13+uint32(kLen)], (*currNodeBuff)[cOff+13:cOff+13+uint32(kLen)])
			copy((*currNodeBuff)[newUpperOff+13+uint32(kLen):newUpperOff+13+uint32(kLen)+uint32(vLen)], val)

			// update cellOffset and upper offset
			binary.LittleEndian.PutUint32((*currNodeBuff)[pgr.HEADER_SIZE_BYTES+(insertionIdx*pgr.CELL_POINTER_SIZE_BYTE)+1:pgr.HEADER_SIZE_BYTES+(insertionIdx*pgr.CELL_POINTER_SIZE_BYTE)+5], newUpperOff)
			binary.LittleEndian.PutUint32((*currNodeBuff)[25:29], newUpperOff)
		} else {
			if vLen != int(oldVlen) {
				binary.LittleEndian.PutUint32((*currNodeBuff)[cOff+5:cOff+9], uint32(vLen))
			}

			// copy new value
			copy((*currNodeBuff)[cOff+13+uint32(kLen):cOff+13+uint32(kLen)+uint32(vLen)], val)
		}

		currNode.Release(false)
		currNode.Unreference()

		return nil
	} else {
		// no key exists
		err = bp.insertToFrame(currNodeBuff, key, 0, val)
		if err != nil {
			if possibleOverflow {
				prevNode.Release(false)
				prevNode.Unreference()
			}

			currNode.Release(false)
			currNode.Unreference()
			return err
		}

		if !possibleOverflow {
			currNode.Release(false)
			currNode.Unreference()

			return nil
		}

		// split overflown node and propagate splits
		for possibleOverflow {
			fmt.Printf("possible overflow loop.....\n")
			newSepKey, newFramePid, err := bp.split(currNodeBuff)
			fmt.Println("split done...")
			if err != nil {
				fmt.Println("Error encountered...")
				if prevNode != nil {
					prevNode.Release(false)
					prevNode.Unreference()
				}
				currNode.Release(false)
				currNode.Unreference()
				return err
			}

			// add seperatorkey to parent keys and newFramePid to ptrs
			fmt.Println("Pop() from btPath....")
			parentPath := btPath.Pop()
			fmt.Printf("Pop() result -> %v\n", parentPath)
			if parentPath == nil || binary.LittleEndian.Uint32((*currNodeBuff)[1:5]) == parentPath.pid {
				// no parent or only one level, create new root node
				fmt.Printf("No parent, creating new root\nPARENT -> %d\t  rootPID -> %d\tJUST SPLIT NODE PID -> %d\n", parentPath.pid, bp.root.Load(), binary.LittleEndian.Uint32((*currNodeBuff)[1:5]))
				newRoot, err := bp.buffermanager.NewFrame(true, true)
				if err != nil {
					panic(err)
				}

				newRoot.Acquire(false)
				newRootBuff, _, err := newRoot.RawBufferSlice()
				if err != nil {
					panic(err)
				}
				fmt.Printf("New Root with pid: %d is set as internal node --> %t\n", binary.LittleEndian.Uint32((*newRootBuff)[1:5]), helpers.BitIsSet(&(*newRootBuff)[0], pgr.IsInternal))

				fmt.Printf("NEW SEPARATOR KEY AFTER SPLIT --> %s\n", newSepKey)
				err = bp.insertToNewRoot(newRootBuff, newSepKey, binary.LittleEndian.Uint32((*currNodeBuff)[1:5]), newFramePid)
				if err != nil {
					panic(err)
				}

				// set new root
				bp.root.Store(binary.LittleEndian.Uint32((*newRootBuff)[1:5]))
				fmt.Println("set new root")
				fmt.Println("PRINTING NEW ROOT NODE AFTER SPLIT=================: ", binary.LittleEndian.Uint32((*newRootBuff)[1:5]))
				printNodeContent(newRootBuff)

				// release latches
				fmt.Println("Releasing root latches...")
				newRoot.Release(false)
				newRoot.Unreference()

				fmt.Println("Releasing currNode latches...")
				currNode.Release(false)
				currNode.Unreference()

				possibleOverflow = false
			} else {
				// add new sepkey and frame pid to parent
				if parentPath.height == treeHeight-1 {
					// parent is "prevNode", latches already acquired
					fmt.Printf("parent is \"prevNode\"\n")
					prevNodeBuff, _, err := prevNode.RawBufferSlice()
					if err != nil {
						panic(fmt.Sprintf("Unable to get prev Node buffer: %s", err.Error()))
					}

					fmt.Printf("Inserting to parent")
					err = bp.insertToFrame(prevNodeBuff, newSepKey, newFramePid, nil)
					if err != nil {
						panic(fmt.Sprintf("Unable to add new separator key and child: %s", err.Error()))
					}

					fmt.Printf("Releasing \"currNode\" latches")
					// release current node latches
					currNode.Release(false)
					currNode.Unreference()

					// check for overflow
					if binary.LittleEndian.Uint32((*prevNodeBuff)[17:21]) <= pgr.ORDER {
						possibleOverflow = false
						prevNode.Release(false)
						prevNode.Unreference()
						prevNode = nil
					} else {
						currNode = prevNode
						prevNode = nil
					}
				} else {
					// parent is in a higher level in the tree.
					parentNode, _, err := bp.buffermanager.Get(parentPath.pid)
					if err != nil {
						panic(fmt.Sprintf("Unable to retrieve parent from buffermanager: %s", err.Error()))
					}

					fmt.Printf("Acquiring lock for the parent node with pid %d...\n", parentPath.pid)
					prevNodeBuff, _, _ := prevNode.RawBufferSlice()
					fmt.Printf("prevNode pid => %d\n", binary.LittleEndian.Uint32((*prevNodeBuff)[1:5]))
					fmt.Printf("CurrNodePid ==> %d\n", binary.LittleEndian.Uint32((*currNodeBuff)[1:5]))
					fmt.Printf("treeHeight: %d\n", treeHeight)
					parentNode.Acquire(false)
					fmt.Printf("Acquired lock for the parent node...\n")
					parentNodeBuff, _, err := parentNode.RawBufferSlice()
					if err != nil {
						panic(fmt.Sprintf("Unable to get parent Node buffer: %s", err.Error()))
					}

					err = bp.insertToFrame(parentNodeBuff, newSepKey, newFramePid, nil)
					if err != nil {
						panic(fmt.Sprintf("Unable to add new separator key and child: %s", err.Error()))
					}

					// release current node latch
					currNode.Release(false)
					currNode.Unreference()

					// check for overflow
					if binary.LittleEndian.Uint32((*parentNodeBuff)[17:21]) <= pgr.ORDER {
						possibleOverflow = false
						parentNode.Release(false)
						parentNode.Unreference()
					} else {
						currNode = parentNode
					}
				}
			}

		}
		return nil
	}
}

func (bp *BpTree) Get(key []byte) (val []byte, e error) {
	var currNode *buffermanager.Frame
	var currNodeBuff *[]byte
	var err error
	var isLeaf bool

	rootPid := bp.root.Load()
	if rootPid == 0 {
		return nil, nil
	}
	fmt.Printf("(GET) RootNodePID: %d\n", rootPid)

	currNode, _, err = bp.buffermanager.Get(rootPid)
	if err != nil {
		return nil, err
	}

	// acquire exlusive latch
	currNode.Acquire(true)

	currNodeBuff, _, err = currNode.RawBufferSlice()
	if err != nil {
		return nil, err
	}

	fmt.Printf("Printing root nde at start of search---------------------\n")
	printNodeContent(currNodeBuff)
	fmt.Printf("----------------------------------------------\n")

	isLeaf = !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal)
	fmt.Printf("root node is leaf node --> %t\n", isLeaf)

	var idx int32
	var ptr uint32
	for !isLeaf {
		// itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
		_, ptr, err = findKeyPath(currNodeBuff, key)
		if err != nil {
			currNode.Release(true)
			currNode.Unreference()
			return nil, err
		}

		if ptr == 0 {
			currNode.Release(true)
			currNode.Unreference()
			return nil, nil
		}

		currNode, _, err = bp.buffermanager.Get(ptr)
		if err != nil {
			return nil, err
		}

		if currNode == nil {
			panic(fmt.Sprintf("No node with pid %d retrieved", ptr))
		}

		// acquire node's shared latch
		currNode.Acquire(true)

		currNodeBuff, _, err = currNode.RawBufferSlice()
		if err != nil {
			currNode.Release(true)
			return nil, err
		}

		fmt.Printf("Printing next node(%d) to search--------------\n", binary.LittleEndian.Uint32((*currNodeBuff)[1:5]))
		printNodeContent(currNodeBuff)
		fmt.Println("---------------------------------------")
		// check if is a leaf node
		isLeaf = !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal)
	}

	itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
	idx, err = findKeyIndex(currNodeBuff, key, 0, itemCount-1)
	if err != nil {
		return nil, err
	}

	if idx < 0 {
		fmt.Printf("No key found...\n")
		fmt.Printf("printing currnode ====== \n")
		fmt.Println(printNodeContent(currNodeBuff))
		return nil, nil
	}
	fmt.Printf("Found key at idx-> %d\n", idx)

	cOff := binary.LittleEndian.Uint32((*currNodeBuff)[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5])
	kLen := binary.LittleEndian.Uint32((*currNodeBuff)[cOff+1 : cOff+5])
	vLen := binary.LittleEndian.Uint32((*currNodeBuff)[cOff+5 : cOff+9])
	fmt.Printf("KLEN -> %d\tVLEN -> %d\n", kLen, vLen)
	value := make([]byte, vLen)
	copy(value, (*currNodeBuff)[cOff+13+kLen:cOff+13+kLen+vLen])

	currNode.Release(true)
	currNode.Unreference()

	return value, nil
}

// findInsertionIdx searches the frame cell pointers using binary search to
// find the index for 'searchKey'. It returns the index where the searchKey
// can be inserted.
// returns idx and error if any
func findInsertionIdx(fr *[]byte, searchKey []byte, startIdx uint32, endIdx uint32) (idx int32, e error) {
	if fr == nil {
		return -1, BTreeError{Message: "frame not provided"}
	}

	if searchKey == nil {
		return -1, BTreeError{Message: "No search key provided"}
	}

	if startIdx > endIdx {
		return -1, BTreeError{Message: fmt.Sprintf("(findInsertionIdx) Invalid start: %d and end: %d index", startIdx, endIdx)}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount == 0 {
		// empty node
		return 0, nil
	}

	if endIdx > itemCount-1 {
		return -1, BTreeError{Message: fmt.Sprintf("Invalid end index: %d, with itemcount: %d", endIdx, itemCount)}
	}

	arrLen := (endIdx - startIdx) + 1
	midPoint := startIdx + uint32(math.Round(float64(arrLen/2)))

	// get key at index
	cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
	keyLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
	key := (*fr)[cellOff+13 : cellOff+13+keyLen]

	// compare
	s := bytes.Compare(key, searchKey)

	if s == 0 {
		// found exact key
		return int32(midPoint), nil
	} else if s == 1 {
		// searchKey < key
		if itemCount == 1 {
			return 0, nil
		}
		// compare with item at previous index
		prevCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint-1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint-1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
		prevKeyLen := binary.LittleEndian.Uint32((*fr)[prevCellOff+1 : prevCellOff+5])
		prevKey := (*fr)[prevCellOff+13 : prevCellOff+13+prevKeyLen]
		if n := bytes.Compare(prevKey, searchKey); n < 0 {
			// searchKey > prevKey
			return int32(midPoint), nil
		} else {
			// searchKey < prevKey
			if arrLen == 2 {
				// check key at previous index instead of recursing
				midPoint--
				cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
				keyLen = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
				key = (*fr)[cellOff+13 : cellOff+13+keyLen]

				if s = bytes.Compare(key, searchKey); s >= 0 {
					return int32(midPoint), nil
				} else {
					return int32(midPoint + uint32(1)), nil
				}
			}
			return findInsertionIdx(fr, searchKey, startIdx, midPoint)
		}
	} else {
		// searchkey > key
		if itemCount == 1 {
			return 1, nil
		}

		if arrLen == 2 {
			// check item at next index
			nextCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
			nextKeyLen := binary.LittleEndian.Uint32((*fr)[nextCellOff+1 : nextCellOff+5])
			nextKey := (*fr)[nextCellOff+13 : nextCellOff+13+nextKeyLen]

			if n := bytes.Compare(nextKey, searchKey); n < 0 {
				if midPoint == endIdx {
					return int32(midPoint + 1), nil
				} else {
					return int32(midPoint + 2), nil
				}
			} else {
				if arrLen == 2 {
					panic("No suitable slot could be found")
				}
				return int32(midPoint + 1), nil
			}
		}

		return findInsertionIdx(fr, searchKey, midPoint, endIdx)
	}
}

// findKeyIndex searches the frame cell pointers using binary search to
// find the index for 'searchKey'. It returns the exact index of the searchKey.
// if item does not exist, returns -1 else idx, and error if any.
// used mostly with leaf nodes or when finding exact index of a key in an internal
// node
func findKeyIndex(fr *[]byte, searchKey []byte, startIdx uint32, endIdx uint32) (idx int32, e error) {
	fmt.Printf("Searching: %s, startIdx: %d, endIdx: %d\n", searchKey, startIdx, endIdx)
	if fr == nil {
		return -1, BTreeError{Message: "frame not provided"}
	}

	if searchKey == nil {
		return -1, BTreeError{Message: "No search key provided"}
	}

	if startIdx > endIdx {
		return -1, BTreeError{Message: fmt.Sprintf("(findKeyIndex) Invalid start: %d and end: %d index", startIdx, endIdx)}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if endIdx > itemCount-1 {
		return -1, BTreeError{Message: fmt.Sprintf("Invalid end index: %d, with itemcount: %d", endIdx, itemCount)}
	}

	if itemCount == 0 {
		return -1, nil
	}

	if itemCount == 1 {
		// check the only item
		cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+1 : pgr.HEADER_SIZE_BYTES+5])
		keyLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
		key := (*fr)[cellOff+13 : cellOff+13+keyLen]

		// compare
		if !bytes.Equal(key, searchKey) {
			return -1, nil
		} else {
			return 0, nil
		}
	}

	arrLen := (endIdx - startIdx) + 1
	midPoint := startIdx + uint32(math.Round(float64(arrLen/2)))

	// get key at index
	cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
	keyLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
	key := (*fr)[cellOff+13 : cellOff+13+keyLen]

	// compare
	s := bytes.Compare(key, searchKey)

	switch s {
	case 0:
		// found exact key
		return int32(midPoint), nil
	case 1:
		// searchKey < key
		if arrLen == 2 {
			// check key at previous index instead of recursing
			midPoint--
			cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
			keyLen = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
			key = (*fr)[cellOff+13 : cellOff+13+keyLen]

			if s = bytes.Compare(key, searchKey); s == 0 {
				return int32(midPoint), nil
			} else {
				return -1, nil
			}
		}
		return findKeyIndex(fr, searchKey, startIdx, midPoint)
	default:
		// searchKey > key
		if arrLen == 2 {
			// no item found
			return -1, nil
		}
		return findKeyIndex(fr, searchKey, midPoint, endIdx)
	}
}

// findKeyPath Searches for a key in an internal node and
// returns the child pointer at which to follow to retrieve the key.
// Used when traversing the B-Tree to find the next child pointer for a
// given key.
// returns sepKey, childPtr pid, or zero if no child found and error if any
func findKeyPath(fr *[]byte, searchKey []byte) (sepKey []byte, child uint32, e error) {
	if fr == nil {
		return nil, 0, BTreeError{Message: "frame not provided"}
	}

	internalNode := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)
	if !internalNode {
		return nil, 0, BTreeError{Message: "Expected internal node, got leaf node"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	var startIdx uint32 = 0
	var endIdx uint32 = itemCount - 1

	arrLen := itemCount
	midPoint := startIdx + uint32(math.Round(float64(arrLen/2)))

	// set max iterations
	maxIter := math.Round(math.Log(float64(itemCount))/math.Log(2)) + 1

	for i := range int(maxIter) {
		fmt.Printf("%d. ITEMCOUNT -> %d\tARRLEN -> %d\tMIDPOINT -> %d\tSTARTIDX: %d\tENDIDX: %d\n", i, itemCount, arrLen, midPoint, startIdx, endIdx)
		// get key at index
		cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
		keyLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
		key := (*fr)[cellOff+13 : cellOff+13+keyLen]

		// compare
		s := bytes.Compare(key, searchKey)

		switch s {
		case 0:
			// get pointer at midpoint+1
			fmt.Println("Found exact key...")
			// check if is the last key
			if itemCount-1 == midPoint {
				return key, binary.LittleEndian.Uint32((*fr)[39:43]), nil
			}
			actualCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
			ptr := binary.LittleEndian.Uint32((*fr)[actualCellOff+9 : actualCellOff+13])
			return key, ptr, nil
		case -1:
			// key < searchKey
			fmt.Println("key < searchKey")
			if arrLen == 1 {
				fmt.Println("arrLen == 1")
				if itemCount-1 == midPoint {
					return key, binary.LittleEndian.Uint32((*fr)[39:43]), nil
				}
				// get pointer at midpoint+1
				actualCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
				ptr := binary.LittleEndian.Uint32((*fr)[actualCellOff+9 : actualCellOff+13])
				return key, ptr, nil
			}

			startIdx = midPoint
			arrLen = (endIdx - startIdx) + 1
			midPoint = startIdx + uint32(math.Round(float64(arrLen/2)))
		default:
			// key > searchKey; s == 1
			fmt.Println("key > searchKey")
			if arrLen == 1 {
				fmt.Println("arrLen == 1")
				// get pointer at midpoint+1
				actualCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
				ptr := binary.LittleEndian.Uint32((*fr)[actualCellOff+9 : actualCellOff+13])
				return key, ptr, nil
			}

			if midPoint == endIdx {
				endIdx = midPoint - 1
			} else {
				endIdx = midPoint
			}
			arrLen = (endIdx - startIdx) + 1
			midPoint = startIdx + uint32(math.Round(float64(arrLen/2)))
		}
	}

	fmt.Println("Could not find appropriate child pointer")
	return nil, 0, nil
}

// defragmentNode defragments a node by getting rid of empty spaces and arranging all cells in contiguous memory location
func defragmentNode(fr *[]byte) error {
	if fr == nil {
		return BTreeError{Message: "No node provided."}
	}

	var temp []byte
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	var expectedCellOff uint32 = pgr.PAGE_SIZE_BYTES - pgr.LOWER_PADDING_BYTES

	for i := range itemCount {
		ptrOff := pgr.HEADER_SIZE_BYTES + (i * pgr.CELL_POINTER_SIZE_BYTE)
		off := binary.LittleEndian.Uint32((*fr)[ptrOff+1 : ptrOff+5])
		kLen := binary.LittleEndian.Uint32((*fr)[off+1 : off+5])
		vLen := binary.LittleEndian.Uint32((*fr)[off+5 : off+9])
		cellSize := 13 + kLen + vLen
		expectedCellOff -= cellSize
		if off != uint32(expectedCellOff) {
			if temp == nil {
				// initiate temp and copy page data
				temp = make([]byte, pgr.PAGE_SIZE_BYTES)
				copy(temp, (*fr))
			}

			// write cell to right offset in temp
			copy(temp[expectedCellOff:expectedCellOff+cellSize], (*fr)[off:off+cellSize])
			// update cell offset in pointer
			binary.LittleEndian.PutUint32(temp[ptrOff+1:ptrOff+5], expectedCellOff)
		}
	}

	if temp != nil {
		// update upper offset, copy over defragmented node
		binary.LittleEndian.PutUint32(temp[25:29], expectedCellOff)
		copy((*fr), temp)
	}

	return nil
}

// replaceSepKey replaces the provided seperator key in the node with newSepKey
// This function is called when two nodes are rebalanced and a separator key
// needs to be added to the parent node in place of the old one.
// fr should be an internal node.
// returns index of replaced key and error if any
func replaceSepKey(fr *[]byte, oldKey []byte, newKey []byte) (sepKeyIdx int32, err error) {
	if fr == nil {
		return -1, BTreeError{Message: "frame not provided"}
	}

	if oldKey == nil || len(oldKey) == 0 {
		return -1, BTreeError{Message: "No key provided"}
	}

	if newKey == nil || len(newKey) == 0 {
		return -1, BTreeError{Message: "No new separator key provided"}
	}

	isInternal := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)

	if !isInternal {
		return -1, BTreeError{Message: "Provided frame should be of an internal node."}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount == 0 {
		return -1, BTreeError{Message: "Node is empty"}
	}

	idx, e := findKeyIndex(fr, oldKey, 0, itemCount-1)
	fmt.Printf("FINDKEYIDX: %d\n", idx)
	if e != nil {
		return -1, e
	}

	if idx < 0 {
		return -1, BTreeError{Message: fmt.Sprintf("Key %v not found in node.", oldKey)}
	}

	// if new value is larger, create new cell and update offset else perform an in-place update
	prevUpperOff := binary.LittleEndian.Uint32((*fr)[25:29])
	newKLen := len(newKey)
	cOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5])
	oldKLen := binary.LittleEndian.Uint32((*fr)[cOff+1 : cOff+5])
	vLen := binary.LittleEndian.Uint32((*fr)[cOff+5 : cOff+9])

	if newKLen > int(oldKLen) {
		newUpperOff := prevUpperOff - uint32(13+uint32(newKLen)+vLen)
		// write cell content
		(*fr)[newUpperOff] = (*fr)[cOff]
		binary.LittleEndian.PutUint32((*fr)[newUpperOff+1:newUpperOff+5], uint32(newKLen))
		copy((*fr)[newUpperOff+5:newUpperOff+13], (*fr)[cOff+5:cOff+13])                                                             // valsize & child ptr
		copy((*fr)[newUpperOff+13:newUpperOff+13+uint32(newKLen)], newKey)                                                           // new key
		copy((*fr)[newUpperOff+13+uint32(newKLen):newUpperOff+13+uint32(newKLen)+vLen], (*fr)[cOff+13+oldKLen:cOff+13+oldKLen+vLen]) // val

		// update cellOffset and upper offset
		binary.LittleEndian.PutUint32((*fr)[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1:pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5], newUpperOff)
		binary.LittleEndian.PutUint32((*fr)[25:29], newUpperOff)
	} else {
		copy((*fr)[cOff+13:cOff+13+uint32(newKLen)], newKey)
		// if newKey < oldKey, we shift the cell to occupy the hole
		if newKLen < int(oldKLen) {
			// update key size and shift rest of the cell
			binary.LittleEndian.PutUint32((*fr)[cOff+1:cOff+5], uint32(newKLen))
			copy((*fr)[cOff+13+uint32(newKLen):cOff+13+uint32(newKLen)+vLen], (*fr)[cOff+13+uint32(oldKLen):cOff+13+uint32(oldKLen)+vLen])
		}
	}

	return idx, nil
}

func printNodeContent(fr *[]byte) string {
	if fr == nil {
		panic("no frame provided")
	}

	if l := len(*fr); l != pgr.PAGE_SIZE_BYTES {
		panic(fmt.Sprintf("Invalid frame size %d, expected %d bytes", l, pgr.PAGE_SIZE_BYTES))
	}

	isInternal := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])

	var keys [][]byte
	var vals [][]byte
	var ptrs []uint32

	var kLen uint32
	var valLen uint32
	for i := range itemCount {
		cellOff := binary.LittleEndian.Uint32((*fr)[(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+1 : (i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+5])

		kLen = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
		valLen = binary.LittleEndian.Uint32((*fr)[cellOff+5 : cellOff+9])

		keys = append(keys, (*fr)[cellOff+13:cellOff+13+kLen])

		if isInternal {
			ptrs = append(ptrs, binary.LittleEndian.Uint32((*fr)[cellOff+9:cellOff+13]))
		} else {
			vals = append(vals, (*fr)[cellOff+13+kLen:cellOff+13+kLen+valLen])
		}
	}

	if isInternal {
		ptrs = append(ptrs, binary.LittleEndian.Uint32((*fr)[39:43]))
	}

	return helpers.PrintBPTreeNode(keys, vals, ptrs)
}

// NewBpTree returns a new instance of a B+ Tree index
// requires a buffer manager and wal instance provided
func NewBpTree(buffMan *bm.BufferManager, wal *wal.WAL) (index *BpTree, e error) {
	if buffMan == nil {
		return nil, BTreeError{Message: "No buffer manager provided"}
	}

	if wal == nil {
		return nil, BTreeError{Message: "No wal provided"}
	}

	meta, _, err := buffMan.Get(0)
	if err != nil {
		return nil, err
	}

	bp := &BpTree{
		meta:          meta,
		buffermanager: buffMan,
		wal:           wal,
	}

	return bp, nil
}
