// package bptree implements a b+ tree
package bptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"

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

		// update cell offset
		// newFrCellOffset -= cellSize

		// clear old cell pointer and cell offset
		clear((*fr)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE) : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE])
		clear((*fr)[cellOff:cellEndOff])
	}

	// update new node's right child
	binary.LittleEndian.PutUint32((*newFrBuff)[39:43], rightNodeRightPtr)

	// update numItems in each node/frame
	binary.LittleEndian.PutUint32((*fr)[17:21], pgr.ORDER)

	if isInternal {
		// new internal node will have itemcount-order-1 due to promoted key
		binary.LittleEndian.PutUint32((*newFrBuff)[17:21], itemCount-pgr.ORDER-1)
	} else {
		binary.LittleEndian.PutUint32((*newFrBuff)[17:21], itemCount-pgr.ORDER)
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

	// mark nodes dirty
	helpers.SetFlag(&(*fr)[0], []int{pgr.Dirty})
	helpers.SetFlag(&(*newFrBuff)[0], []int{pgr.Dirty})

	return seperatorKey, binary.LittleEndian.Uint32((*newFrBuff)[1:5]), nil
}

// merge merges left node and right node.
// in the case that the keys can be redistributed, the nodes are
// rebalanced and the new seperator key will be returned.
// latches for both nodes should be acquired before calling merge()
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

	if leftNodeItemCount+1 >= (pgr.ORDER*2)+1 && rightNodeItemCount+1 >= (pgr.ORDER*2)+1 {
		// both nodes already above the threshold. No merge or rebalancing required
		return nil, nil
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
			deficit = uint32((pgr.ORDER * 2) - rightNodeItemCount)
		} else {
			donorDirection = true
			deficit = uint32((pgr.ORDER * 2) - leftNodeItemCount)
		}

		// 1. demote separator key
		err := bp.insertToFrame(rightNode, sepKey, 0, nil)
		if err != nil {
			return nil, err
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

				childPtr, e := bp.deleteFromNode(leftNode, lastKey, false)
				if e != nil {
					return nil, e
				}

				e = bp.insertToFrame(rightNode, lastKey, childPtr, nil)
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

				childPtr, e := bp.deleteFromNode(rightNode, firstKey, false)
				if e != nil {
					return nil, e
				}

				e = bp.insertToFrame(rightNode, firstKey, childPtr, nil)
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
		ptr, e := bp.deleteFromNode(rightNode, newSeperatorKey, false)
		if e != nil {
			return nil, e
		}
		// ptr should be 0, since we demoted the seperator key without any child pointer
		if ptr != 0 {
			panic(fmt.Errorf("expected no pointer but got, %d", ptr))
		}

		return newSeperatorKey, nil
	} else {
		// merge
		if leftMerge {
			// moving items from the left node to the right node
			deficit = (pgr.ORDER * 2) - leftNodeItemCount
			// 1. demote separator key
			err := bp.insertToFrame(rightNode, sepKey, 0, nil)
			if err != nil {
				return nil, err
			}

			// move keys from left node to right node
			for deficit > 0 {
				lastKey, err := bp.getLastKey(leftNode)
				if err != nil {
					return nil, err
				}

				ptr, err := bp.deleteFromNode(leftNode, lastKey, false)
				if err != nil {
					return nil, err
				}

				err = bp.insertToFrame(rightNode, lastKey, ptr, nil)
				if err != nil {
					return nil, err
				}

				deficit--
			}
		} else {
			// moving items from right node to left node
			deficit = (pgr.ORDER * 2) - rightNodeItemCount
			// 1. demote separator key
			err := bp.insertToFrame(rightNode, sepKey, 0, nil)
			if err != nil {
				return nil, err
			}

			// move keys from left node to right node
			for deficit > 0 {
				firstKey, err := bp.getFirstKey(rightNode)
				if err != nil {
					return nil, err
				}

				ptr, err := bp.deleteFromNode(rightNode, firstKey, false)
				if err != nil {
					return nil, err
				}

				err = bp.insertToFrame(leftNode, firstKey, ptr, nil)
				if err != nil {
					return nil, err
				}

				deficit--
			}
		}

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
	// set new upper offset
	binary.LittleEndian.PutUint32((*fr)[25:29], cellStartOff)

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

	// write cell contents
	binary.LittleEndian.PutUint32((*fr)[cellStartOff+1:cellStartOff+5], uint32(keyLen))
	binary.LittleEndian.PutUint32((*fr)[cellStartOff+5:cellStartOff+9], uint32(valLen))
	copy((*fr)[cellStartOff+13:cellStartOff+13+uint32(keyLen)], key)
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

	// insert cell pointer
	lowOff := binary.LittleEndian.Uint32((*fr)[29:33])
	if insertIdx == itemCount {
		// add item to end of ptr list
		if internal && childPtr != 0 {
			helpers.SetFlag(&(*fr)[lowOff], []int{pgr.HasChildPtr})
		}
		binary.LittleEndian.PutUint32((*fr)[lowOff+1:lowOff+5], cellStartOff)
	} else {
		// shift items to the right to create space for new cell pointer
		copy((*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE:lowOff+pgr.CELL_POINTER_SIZE_BYTE], (*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff])

		if internal && childPtr != 0 {
			helpers.SetFlag(&(*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)], []int{pgr.HasChildPtr})
		}

		binary.LittleEndian.PutUint32((*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+1:pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE], cellStartOff)
	}

	binary.LittleEndian.PutUint32((*fr)[17:21], itemCount+1)

	return nil
}

// deletes a key from a node/frame/page by deleting cell pointers and rearranging them to occupy any holes left
// if the node is a non-leaf node, the child pointer is returned.
func (bp *BpTree) deleteFromNode(fr *[]byte, key []byte, leftMerge bool) (ptr uint32, e error) {
	if fr == nil {
		return 0, BTreeError{Message: "No frame provided"}
	}

	if key == nil {
		return 0, BTreeError{Message: "No frame provided"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount == 0 {
		return 0, BTreeError{Message: "Frame has no keys"}
	}

	internal := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)

	var delIdx int32 = -1
	var cOff uint32
	var ptrOff uint32

	idx, e := findKeyIndex(fr, key, 0, itemCount-1)
	if e != nil {
		return 0, e
	}
	if idx < 0 {
		return 0, BTreeError{Message: "Could find key to delete"}
	}

	delIdx = idx
	ptrOff = uint32(delIdx)*pgr.CELL_POINTER_SIZE_BYTE + pgr.HEADER_SIZE_BYTES
	cOff = binary.LittleEndian.Uint32((*fr)[ptrOff+1:])

	if !internal {
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

		return 0, nil
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
		if !leftMerge && hasChildPtr {
			currCellPtr := binary.LittleEndian.Uint32((*fr)[cOff+9 : cOff+13])
			nextCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(ptrOff+pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(ptrOff+pgr.CELL_POINTER_SIZE_BYTE)+5])
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

	return deletedChildPtr, nil
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

	if startIdx >= endIdx {
		return -1, BTreeError{Message: fmt.Sprintf("Invalid start: %d and end: %d index", startIdx, endIdx)}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if endIdx > itemCount-1 {
		return -1, BTreeError{Message: "Invalid end index"}
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
		// compare with item at previous index
		prevCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint-1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint-1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
		prevKeyLen := binary.LittleEndian.Uint32((*fr)[prevCellOff+1 : prevCellOff+5])
		prevKey := (*fr)[prevCellOff+13 : prevCellOff+13+prevKeyLen]
		if n := bytes.Compare(key, prevKey); n < 0 {
			return int32(midPoint), nil
		} else {
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
			fmt.Printf("curr Key is greater than searchkey, calling findInsertionIdx(%d, %d)\n", startIdx, midPoint)
			return findInsertionIdx(fr, searchKey, startIdx, midPoint)
		}
	} else {
		// check item at next index
		nextCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
		nextKeyLen := binary.LittleEndian.Uint32((*fr)[nextCellOff+1 : nextCellOff+5])
		nextKey := (*fr)[nextCellOff+13 : nextCellOff+13+nextKeyLen]

		if n := bytes.Compare(key, nextKey); n < 0 {
			return int32(midPoint + 1), nil
		} else {
			if arrLen == 2 {
				panic("No suitable slot could be found")
			}
			fmt.Printf("curr Key is less than searchkey, calling findInsertionIdx(%d, %d)\n", midPoint, endIdx)
			return findInsertionIdx(fr, searchKey, midPoint, endIdx)
		}
	}
}

// findKeyIndex searches the frame cell pointers using binary search to
// find the index for 'searchKey'. It returns the exact index of the searchKey.
// if item does not exist, returns -1 else idx, and error if any.
func findKeyIndex(fr *[]byte, searchKey []byte, startIdx uint32, endIdx uint32) (idx int32, e error) {
	if fr == nil {
		return -1, BTreeError{Message: "frame not provided"}
	}

	if searchKey == nil {
		return -1, BTreeError{Message: "No search key provided"}
	}

	if startIdx >= endIdx {
		return -1, BTreeError{Message: fmt.Sprintf("Invalid start: %d and end: %d index", startIdx, endIdx)}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if endIdx > itemCount-1 {
		return -1, BTreeError{Message: "Invalid end index"}
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
		if arrLen == 2 {
			// check key at previous index instead of recursing
			midPoint--
			cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
			keyLen = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
			key = (*fr)[cellOff+13 : cellOff+13+keyLen]

			if s = bytes.Compare(key, searchKey); s == 0 {
				return int32(midPoint), nil
			}
		}
		return findKeyIndex(fr, searchKey, startIdx, midPoint)
	} else {
		if arrLen == 2 {
			// no item found
			return -1, nil
		}
		return findKeyIndex(fr, searchKey, midPoint, endIdx)
	}
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
