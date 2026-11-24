// package bptree implements a b+ tree
package bptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	bm "github.com/oryankibandi/baobab/pkg/buffermanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/logger"
	pgr "github.com/oryankibandi/baobab/pkg/pager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

const (
	// bit to set for a search that yields an exact match
	exactMatchBitPos = 7
)

type BpTree struct {
	meta          *bm.Frame
	root          atomic.Uint32
	buffermanager *bm.BufferManager
	numPages      uint64
	wal           *wal.WAL
	height        uint32
	lgger         *logger.BaobabLogger
	pkgName       string

	mu sync.RWMutex
}

// initTreeMetadata reads tree metadata from metadata page and sets values
// in BpTree struct
func (bp *BpTree) initTreeMetadata() error {
	if bp.meta == nil {
		return BTreeError{Message: "Meta page not initialized."}
	}

	bp.meta.Acquire(false)
	defer bp.meta.Release(false)
	buff, _, e := bp.meta.RawBufferSlice()

	if e != nil {
		return e
	}

	bp.root.Store(binary.LittleEndian.Uint32((*buff)[:4]))
	bp.height = binary.LittleEndian.Uint32((*buff)[8:12])
	bp.lgger.Write("bptree", "initTreeMetadata", logger.LevelDebug, "Initialized tree metadata....", nil)

	return nil
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
	// _, _, err := bp.buffermanager.Get(pid)
	// if err != nil {
	// 	return err
	// }

	// unref old root page
	// oldRoot, _, err := bp.buffermanager.Get(bp.root.Load())
	// if err != nil {
	// 	return err
	// }

	// oldRoot.Unreference()
	bp.root.Store(pid)
	return nil
}

// incrementTreeHeight increments height of the tree
func (bp *BpTree) incrementTreeHeight() error {
	bp.meta.Acquire(false)
	defer bp.meta.Release(false)
	buff, _, e := bp.meta.RawBufferSlice()

	if e != nil {
		return e
	}

	bp.mu.Lock()
	bp.height++
	newHeight := bp.height
	bp.mu.Unlock()
	binary.LittleEndian.PutUint32((*buff)[8:12], newHeight)
	bp.meta.MarkDirty()

	return nil
}

// decrementTreeHeight decrements height of the tree
func (bp *BpTree) decrementTreeHeight() error {
	bp.meta.Acquire(false)
	defer bp.meta.Release(false)
	buff, _, e := bp.meta.RawBufferSlice()

	if e != nil {
		return e
	}

	bp.mu.Lock()
	if bp.height > 0 {
		bp.height--
		newHeight := bp.height
		bp.mu.Unlock()
		binary.LittleEndian.PutUint32((*buff)[8:12], newHeight)
		bp.meta.MarkDirty()
	} else {
		bp.mu.Unlock()
		panic("Tried decrementing tree height with a height of 0")
	}

	return nil
}

// split splits an overflowed frame and redistributes the keys and values/ptrs
// returns the promoted separator key, new right node pid and error if any
func (bp *BpTree) split(fr *[]byte) (sepKey []byte, newFramePid uint32, e error) {
	bp.lgger.Write("bptree", "split()", logger.LevelDebug, "SPLIT()...", nil)
	bp.lgger.Write("bptree", "split()", logger.LevelDebug, fmt.Sprintf("Splitting node with Node ID => %d\n", binary.LittleEndian.Uint32((*fr)[1:5])), nil)
	isInternal := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)
	// check if full
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount <= 2*pgr.ORDER {
		return nil, 0, BTreeError{Message: fmt.Sprintf("provided frame has not overflown: node has %d items.", itemCount)}
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
	var leftNodeUpperOffset uint32
	for i := uint32(pgr.ORDER); i < itemCount; i++ {
		cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+5])

		// read cell sizes
		cellKeySize = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
		cellValSize = binary.LittleEndian.Uint32((*fr)[cellOff+5 : cellOff+9])
		cellSize = 13 + cellKeySize + cellValSize
		cellEndOff = cellOff + cellSize

		if i == pgr.ORDER {
			// first sep key promoted to parent
			seperatorKey = make([]byte, cellKeySize)
			copy(seperatorKey, (*fr)[cellOff+13:cellOff+13+cellKeySize])

			// set current cell offset as left node's upper offset after split as it will
			// be the last item in the page after split. If node is fragmented and there's
			// possibly other cells beyond this, the defragmentNode() fxn will handle that.
			leftNodeUpperOffset = cellEndOff
			bp.lgger.Write("bptree", "split", logger.LevelDebug, fmt.Sprintf("%d. CELLSIZE AT i = ORDER --> %d\tCELLOFF -> %d\n", i, cellSize, cellOff), nil)
			if isInternal {
				// store old right child metadata and update to new right child(curr cell page pointer)
				rightNodeRightPtr = binary.LittleEndian.Uint32((*fr)[39:43])
				copy((*fr)[39:43], (*fr)[cellOff+9:cellOff+13])
				continue
			}
		}

		// update cell offset in new frame cell pointer
		newFrCellOffset -= cellSize
		if i == pgr.ORDER {
			newFrCellPtrIdx = 0
		} else {
			if isInternal {
				newFrCellPtrIdx = i - pgr.ORDER - 1
			} else {
				newFrCellPtrIdx = i - pgr.ORDER
			}
		}

		// add cell pointer to new frame
		binary.LittleEndian.PutUint32((*newFrBuff)[(pgr.HEADER_SIZE_BYTES+(newFrCellPtrIdx*pgr.CELL_POINTER_SIZE_BYTE))+1:pgr.HEADER_SIZE_BYTES+(newFrCellPtrIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+5], newFrCellOffset)
		// set hasChildPtr flag
		if isInternal {
			helpers.SetFlag(&(*newFrBuff)[(pgr.HEADER_SIZE_BYTES+(newFrCellPtrIdx*pgr.CELL_POINTER_SIZE_BYTE))], []int{pgr.HasChildPtr})
		}

		// copy cell to new frame
		copy((*newFrBuff)[newFrCellOffset:newFrCellOffset+cellSize], (*fr)[cellOff:cellEndOff])

		// update upper offset
		binary.LittleEndian.PutUint32((*newFrBuff)[25:29], newFrCellOffset)
		// leftNodeUpperOffset += cellSize
		// binary.LittleEndian.PutUint32((*fr)[25:29], leftNodeUpperOffset)

		// update cell offset
		// newFrCellOffset -= cellSize

		// clear old cell pointer and cell offset
		clear((*fr)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE) : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE])
		clear((*fr)[cellOff:cellEndOff])
	}

	// set left node's upper offset
	if leftNodeUpperOffset == 0 {
		panic("Invalid left node upper offset: 0")
	}
	binary.LittleEndian.PutUint32((*fr)[25:29], leftNodeUpperOffset)

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

		// mark right sibling dirty
		helpers.SetFlag(&(*siblBuff)[0], []int{pgr.Dirty})
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

	return seperatorKey, binary.LittleEndian.Uint32((*newFrBuff)[1:5]), nil
}

// merge merges left node and right node.
// in the case that the keys can be redistributed, the nodes are
// rebalanced and the new seperator key will be returned.
// Latches for both nodes should be acquired before calling merge()
// Merge always merges the right node to the left node unless the
// underflowed node has no immediate left sibling.
// leftMerge is true if the underflown node is the left node(has no immediate left sibling).
// returns newSepKey if rebalanced or error if any
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
			// left to right
			bp.lgger.Write(bp.pkgName, "merge", logger.LevelDebug, "left to right", nil)
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
			bp.lgger.Write(bp.pkgName, "merge", logger.LevelDebug, "right to left", nil)
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
			// fmt.Println("(REBALANCE) PRINTING NODES BEFORE GETING SEP KEY...")
			// fmt.Printf("LEFT NODE ->\n")
			// fmt.Println(printNodeContent(leftNode))
			// fmt.Printf("RIGHT NODE ->\n")
			// fmt.Println(printNodeContent(rightNode))
			// // var ptr uint32
			// fmt.Printf("Donor direction --> %t\n", donorDirection)

			newSepKey, e = bp.promoteNewSeperatorKey(rightNode)

			if e != nil {
				return nil, e
			}

			// ptr should be 0, since we demoted the seperator key without any child pointer
			// if ptr != 0 {
			// 	panic(fmt.Errorf("expected no pointer but got, %d", ptr))
			// }
		}

		// mark both nodes dirty
		helpers.SetFlag(&(*leftNode)[0], []int{pgr.Dirty})
		helpers.SetFlag(&(*rightNode)[0], []int{pgr.Dirty})

		// fmt.Printf("(MERGE-AFTER REBALANCE) LEFT NODE\n")
		// fmt.Println(printNodeContent(leftNode))
		// fmt.Printf("(MERGE-AFTER REBALANCE) RIGHT NODE\n")
		// fmt.Println(printNodeContent(rightNode))
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

			// move the left node's right most child ptr to the right node's first cell/slot
			if internalNodeMerge {
				if binary.LittleEndian.Uint32((*leftNode)[39:43]) == 0 {
					panic("Invalid left node's child right child pointer pid \"0\"")
				}

				bp.updateFirstChildPtr(rightNode, (*leftNode)[39:43])

				// set "hasChildPtr" flag
				helpers.SetFlag(&(*rightNode)[pgr.HEADER_SIZE_BYTES+(leftNodeItemCount*pgr.CELL_POINTER_SIZE_BYTE)], []int{pgr.HasChildPtr})
			}

			// update sibling pointers
			bp.lgger.Write(bp.pkgName, "merge", logger.LevelDebug, fmt.Sprintf("setting right node's(%d) left sibling to %d\n", binary.LittleEndian.Uint32((*rightNode)[1:5]), (*leftNode)[47:51]), nil)
			copy((*rightNode)[47:51], (*leftNode)[47:51])

			// retrieve leftNode's left sibling and update it's right sibling pointer
			if lc := binary.LittleEndian.Uint32((*leftNode)[47:51]); lc != 0 {
				lSib, _, err := bp.buffermanager.Get(lc)
				if err != nil {
					panic(fmt.Sprintf("Unable to retrieve left node's left sibling with pid -> %d:\terr: %s", lc, err.Error()))
				}

				lSib.Acquire(false)
				lSibFr, _, err := lSib.RawBufferSlice()
				if err != nil {
					lSib.Release(false)
					panic(fmt.Sprintf("No buffer attached to frame retrieved: %s", err.Error()))
				}

				copy((*lSibFr)[43:47], (*leftNode)[43:47])
				// mark node dirty
				helpers.SetFlag(&(*lSibFr)[0], []int{pgr.Dirty})
				lSib.Release(false)
				lSib.Unreference()
			}

			// mark left node as dead and dirty
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

			// printNodeContent(rightNode)
			rightNodeItemCount = binary.LittleEndian.Uint32((*rightNode)[17:21])
			deficit = leftNodeItemCount - rightNodeItemCount

			// move keys from right node to left node
			for range rightNodeItemCount {
				firstKey, err := bp.getFirstKey(rightNode)
				if err != nil {
					return nil, err
				}

				ptr, v, err := bp.deleteFromNode(rightNode, firstKey, true)
				if err != nil {
					return nil, err
				}

				err = bp.insertToFrame(leftNode, firstKey, ptr, v)
				if err != nil {
					return nil, err
				}
			}

			// move the right node's right most child ptr to the left node's right most child ptr
			if internalNodeMerge {
				if binary.LittleEndian.Uint32((*rightNode)[39:43]) == 0 {
					panic("Invalid right node's child right child pointer pid \"0\"")
				}

				copy((*leftNode)[39:43], (*rightNode)[39:43])

				// set "hasChildPtr" on the last cell of the left node
				lastItemIdx := leftNodeItemCount + rightNodeItemCount - 1
				helpers.SetFlag(&(*leftNode)[pgr.HEADER_SIZE_BYTES+(lastItemIdx*pgr.CELL_POINTER_SIZE_BYTE)], []int{pgr.HasChildPtr})
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
				// mark node dirty
				helpers.SetFlag(&(*rSibFr)[0], []int{pgr.Dirty})
				rSib.Release(false)
				rSib.Unreference()
			}

			// mark right node as dead & dirty
			helpers.SetFlag(&(*rightNode)[0], []int{pgr.Dead, pgr.Dirty})

			// mark left node as dirty
			helpers.SetFlag(&(*leftNode)[0], []int{pgr.Dirty})
		}

		// fmt.Printf("(MERGE-AFTER MERGE) LEFT NODE\n")
		// fmt.Println(printNodeContent(leftNode))
		// fmt.Printf("(MERGE-AFTER MERGE) RIGHT NODE\n")
		// fmt.Println(printNodeContent(rightNode))
		//
		return nil, nil
	}
}

// insertToFrame inserts key and value/childPtr to a frame and shifts
// cellpointers if necessary.
func (bp *BpTree) insertToFrame(fr *[]byte, key []byte, childPtr uint32, val []byte) error {
	bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, "=========================================================================================================", nil)
	bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, fmt.Sprintf("INSERTING KEY -> %s\tCHILDPTR -> %d\tVAL -> %s\nINSERTING TO Node ID => %d\n", key, childPtr, val, binary.LittleEndian.Uint32((*fr)[1:5])), nil)
	bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, "=========================================================================================================", nil)
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
	bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, fmt.Sprintf("CELLSIZE -> %d\tCELLSTARTOFF -> %d\tUPPEROFFSET -> %d\n", cellSize, cellStartOff, upperOffset), nil)
	bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, fmt.Sprintf("LOWOFFSET --> %d\n", binary.LittleEndian.Uint32((*fr)[29:33])), nil)
	// set new upper offset
	// binary.LittleEndian.PutUint32((*fr)[25:29], cellStartOff)

	// find appropriate index to add key and shift keys if need be.
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])

	var cOff uint32
	var ptrOff uint32
	// var cKSize uint32
	var insertIdx uint32 = itemCount

	idxArr, e := findInsertionIdx(fr, key, 0, itemCount-1)
	if e != nil {
		return e
	}

	if idxArr == nil {
		return BTreeError{Message: "Unable to insert index"}
	}

	exactMatch := helpers.BitIsSet(&idxArr[0], exactMatchBitPos)
	insertIdx = binary.LittleEndian.Uint32(idxArr[1:])

	if exactMatch {
		bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, "EXACT MATCH...\n", nil)
		// update the value/ptr
		if internal {
			if insertIdx == itemCount-1 {
				bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, "insertIdx == itemCount...\n", nil)
				binary.LittleEndian.PutUint32((*fr)[39:43], childPtr)
			} else {
				bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, "insertIdx != itemCount..", nil)
				bp.lgger.Write(bp.pkgName, "insertToFrame", logger.LevelDebug, fmt.Sprintf("itemcount --> %d\t insertIdx --> %d\n", itemCount, insertIdx), nil)
				ptrOff = ((insertIdx + 1) * pgr.CELL_POINTER_SIZE_BYTE) + pgr.HEADER_SIZE_BYTES
				cOff = binary.LittleEndian.Uint32((*fr)[ptrOff+1 : ptrOff+5])
				binary.LittleEndian.PutUint32((*fr)[cOff+9:cOff+13], childPtr)
			}
		} else {
			ptrOff = (insertIdx * pgr.CELL_POINTER_SIZE_BYTE) + pgr.HEADER_SIZE_BYTES
			cOff = binary.LittleEndian.Uint32((*fr)[ptrOff+1 : ptrOff+5])
			oldKeyLen := int(binary.LittleEndian.Uint32((*fr)[cOff+1 : cOff+5]))
			oldValLen := int(binary.LittleEndian.Uint32((*fr)[cOff+5 : cOff+9]))

			if oldValLen >= valLen {
				// update in-place
				copy((*fr)[cOff+13+uint32(oldKeyLen):cOff+13+uint32(oldKeyLen)+uint32(valLen)], val)
				binary.LittleEndian.PutUint32((*fr)[cOff+5:cOff+9], uint32(valLen))
			} else {
				// write to new cell at upperoffset
				copy((*fr)[cellStartOff:cellStartOff+13+uint32(oldKeyLen)], (*fr)[cOff:cOff+13+uint32(oldKeyLen)])
				copy((*fr)[cellStartOff+13+uint32(oldKeyLen):cellStartOff+13+uint32(oldKeyLen)+uint32(valLen)], val)
				binary.LittleEndian.PutUint32((*fr)[cellStartOff+5:cellStartOff+9], uint32(valLen))

				// update cellOffset  and upper offset
				binary.LittleEndian.PutUint32((*fr)[ptrOff+1:ptrOff+5], cellStartOff)
				binary.LittleEndian.PutUint32((*fr)[25:29], cellStartOff)
			}
		}

		//
		if binary.LittleEndian.Uint32((*fr)[29:33]) == 51 {
			panic(fmt.Sprintf("CURRENT LOW OFFSET IS 51\n"))
		}

		// mark node dirty
		helpers.SetFlag(&(*fr)[0], []int{pgr.Dirty})

		return nil
	}

	ptrOff = (insertIdx * pgr.CELL_POINTER_SIZE_BYTE) + pgr.HEADER_SIZE_BYTES
	cOff = binary.LittleEndian.Uint32((*fr)[ptrOff+1 : ptrOff+5])
	prevCellHasChildPtr := helpers.BitIsSet(&(*fr)[ptrOff], pgr.HasChildPtr)
	// fmt.Printf("PRINTING CELL CONTENTS BEFORE UPDATE inserting -> %s -----------------------------------\n", key)
	// fmt.Printf("WRITE CELL CONTENTS AT OFFSET --> %d\n", cellStartOff)
	// fmt.Printf("Insert IDX -> %d\tItem Count -> %d\n", insertIdx, itemCount)
	// printNodeContent(fr)

	// write cell contents
	binary.LittleEndian.PutUint32((*fr)[cellStartOff+1:cellStartOff+5], uint32(keyLen))
	binary.LittleEndian.PutUint32((*fr)[cellStartOff+5:cellStartOff+9], uint32(valLen))
	copy((*fr)[cellStartOff+13:cellStartOff+13+uint32(keyLen)], key)
	// fmt.Printf("PRINTING CELL CONTENTS MID-UPDATE inserting -> %s -----------------------------------\n", key)
	// printNodeContent(fr)
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
				// add child ptr at current index as pointer to our new cell if its 'HasChildPtr' flag is set
				if prevCellHasChildPtr {
					copy((*fr)[cellStartOff+9:cellStartOff+13], (*fr)[cOff+9:cOff+13])
				}
				// write new child ptr to cell at curr idx.
				binary.LittleEndian.PutUint32((*fr)[cOff+9:cOff+13], childPtr)
			}
		}
	} else {
		copy((*fr)[cellStartOff+13+uint32(keyLen):cellStartOff+13+uint32(keyLen)+uint32(valLen)], val)
	}

	// fmt.Printf("PRINTING CELL CONTENTS BEFORE CELL POINTER INSERTION -> %s -----------------------------------\n", key)
	// fmt.Printf("PROVIDED PTR -> %d\n", childPtr)
	// printNodeContent(fr)
	// insert cell pointer
	lowOff := binary.LittleEndian.Uint32((*fr)[29:33])
	if insertIdx == itemCount {
		// add item to end of ptr list
		//if internal && childPtr != 0 {
		//	helpers.SetFlag(&(*fr)[lowOff], []int{pgr.HasChildPtr})
		//}
		binary.LittleEndian.PutUint32((*fr)[lowOff+1:lowOff+5], cellStartOff)
		if internal && (childPtr == 0) {
			helpers.UnsetFlag(&(*fr)[lowOff], []int{pgr.HasChildPtr})
		} else if internal && childPtr != 0 {
			helpers.SetFlag(&(*fr)[lowOff], []int{pgr.HasChildPtr})
		}
	} else {
		// printNodeContent(fr)
		// shift items to the right to create space for new cell pointer
		copy((*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE:lowOff+pgr.CELL_POINTER_SIZE_BYTE], (*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff])

		// if internal && childPtr != 0 {
		// 	helpers.SetFlag(&(*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)], []int{pgr.HasChildPtr})
		// }

		binary.LittleEndian.PutUint32((*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+1:pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE], cellStartOff)
		if internal && (childPtr == 0 || !prevCellHasChildPtr) {
			helpers.UnsetFlag(&(*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)], []int{pgr.HasChildPtr})

			if childPtr != 0 && !prevCellHasChildPtr {
				// current cell pointer now has no child ptr, set the 'hasChildPtr' flag on the prev cell
				helpers.SetFlag(&(*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE], []int{pgr.HasChildPtr})
			}
		} else if internal && childPtr != 0 {
			helpers.SetFlag(&(*fr)[pgr.HEADER_SIZE_BYTES+(insertIdx*pgr.CELL_POINTER_SIZE_BYTE)], []int{pgr.HasChildPtr})
		}
	}

	// update itemcount, lowoffset and upperoffset
	binary.LittleEndian.PutUint32((*fr)[17:21], itemCount+1)
	binary.LittleEndian.PutUint32((*fr)[29:33], lowOff+pgr.CELL_POINTER_SIZE_BYTE)
	binary.LittleEndian.PutUint32((*fr)[25:29], cellStartOff)
	// printNodeContent(fr)

	if binary.LittleEndian.Uint32((*fr)[29:33]) == 51 {
		panic(fmt.Sprintf("CURRENT LOW OFFSET IS 51\n"))
	}

	// mark node dirty
	helpers.SetFlag(&(*fr)[0], []int{pgr.Dirty})

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

	// add cell pointer and set HasChildPtr flag
	binary.LittleEndian.PutUint32((*rootBuff)[pgr.HEADER_SIZE_BYTES+1:pgr.HEADER_SIZE_BYTES+5], uint32(cellOff))
	helpers.SetFlag(&(*rootBuff)[pgr.HEADER_SIZE_BYTES], []int{pgr.HasChildPtr})

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

		// mark dirty
		helpers.SetFlag(&(*fr)[0], []int{pgr.Dirty})

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
	clear((*fr)[(delIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES : (delIdx*pgr.CELL_POINTER_SIZE_BYTE)+pgr.HEADER_SIZE_BYTES+pgr.CELL_POINTER_SIZE_BYTE])
	binary.LittleEndian.PutUint32((*fr)[29:33], uint32(lowOff-pgr.CELL_POINTER_SIZE_BYTE))

	// fmt.Printf("delIdx -> %d\tleftMerge-> %t\titemCount -> %d\thasChildPtr -> %t\n", delIdx, leftMerge, itemCount, hasChildPtr)
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

			if !hasChildPtr {
				// given that we take the next cells pointer and current cell has no pointer
				// we mark nextCell as not having a child ptr
				helpers.UnsetFlag(&(*fr)[ptrOff+pgr.CELL_POINTER_SIZE_BYTE], []int{pgr.HasChildPtr})
			}
		} else {
			if hasChildPtr {
				deletedChildPtr = binary.LittleEndian.Uint32((*fr)[cOff+9 : cOff+13])
			}
		}

		// shift cell pointers
		copy((*fr)[pgr.HEADER_SIZE_BYTES+(delIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff], (*fr)[pgr.HEADER_SIZE_BYTES+pgr.CELL_POINTER_SIZE_BYTE+(delIdx*pgr.CELL_POINTER_SIZE_BYTE):lowOff+pgr.CELL_POINTER_SIZE_BYTE])
	}

	// decrement item count
	binary.LittleEndian.PutUint32((*fr)[17:21], itemCount-1)

	// mark dirty
	helpers.SetFlag(&(*fr)[0], []int{pgr.Dirty})

	return deletedChildPtr, nil, nil
}

// promoteNewSeperatorKey - returns the left most key of the frame.
// if frame is an internal frame, delete key from node
func (bp *BpTree) promoteNewSeperatorKey(fr *[]byte) (newSepKey []byte, e error) {
	if fr == nil {
		return nil, BTreeError{Message: "No frame provided."}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount <= 0 {
		return nil, BTreeError{Message: "No entries in frame."}
	}

	isInternal := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)
	hasChildPtr := helpers.BitIsSet(&(*fr)[pgr.HEADER_SIZE_BYTES], pgr.HasChildPtr)

	if isInternal && hasChildPtr {
		panic("'hasChildPtr' flag should be unset")
	}

	lowOff := binary.LittleEndian.Uint32((*fr)[29:33])
	cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+1 : pgr.HEADER_SIZE_BYTES+5])
	kLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
	sepKey := make([]byte, kLen)

	copy(sepKey, (*fr)[cellOff+13:cellOff+13+kLen])

	if isInternal {
		// delete key from node. Shift ptrs to the left.
		copy((*fr)[pgr.HEADER_SIZE_BYTES:lowOff-pgr.CELL_POINTER_SIZE_BYTE], (*fr)[pgr.HEADER_SIZE_BYTES+pgr.CELL_POINTER_SIZE_BYTE:lowOff])
		clear((*fr)[lowOff-pgr.CELL_POINTER_SIZE_BYTE : lowOff])

		// update low offset, item count
		binary.LittleEndian.PutUint32((*fr)[29:33], lowOff-pgr.CELL_POINTER_SIZE_BYTE)
		binary.LittleEndian.PutUint32((*fr)[17:21], itemCount-1)
	}

	return sepKey, nil
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

// getFirstChildPtr returns the first child ptr of the frame or error if any.
// atleast a shared latch must be acquired before calling this function
func (bp *BpTree) getFirstChildPtr(fr *[]byte) (k []byte, err error) {
	if fr == nil {
		return nil, BTreeError{Message: "No frame provided"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount == 0 {
		return nil, BTreeError{Message: "Frame has not been provided. "}
	}

	cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+1 : pgr.HEADER_SIZE_BYTES+5])
	if cellOff == 0 || cellOff > pgr.PAGE_SIZE_BYTES {
		return nil, BTreeError{Message: fmt.Sprintf("(getFirstChildPtr) Invalid cell offset: %d", cellOff)}
	}

	return (*fr)[cellOff+9 : cellOff+13], nil
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

// updateFirstChildPtr updates the first child ptr of the frame and returns error if any.
// X-latch on the frame must be acquired before calling this function
func (bp *BpTree) updateFirstChildPtr(fr *[]byte, ptr []byte) error {
	if fr == nil {
		return BTreeError{Message: "No frame provided"}
	}

	cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+1 : pgr.HEADER_SIZE_BYTES+5])
	if cellOff == 0 || cellOff > pgr.PAGE_SIZE_BYTES {
		return BTreeError{Message: fmt.Sprintf("(updateFirstChildPtr) Invalid cell offset: %d", cellOff)}
	}

	copy((*fr)[cellOff+9:cellOff+13], ptr)

	// set "hasChildPtrFlag"
	helpers.SetFlag(&(*fr)[pgr.HEADER_SIZE_BYTES], []int{pgr.HasChildPtr})

	return nil
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
	if fr == nil {
		return 0, BTreeError{Message: "No frame buffer provided"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if idx > itemCount {
		return 0, BTreeError{Message: fmt.Sprintf("Invalid index provided: %d", idx)}
	}

	if idx == itemCount {
		// right most child pointer.
		return binary.LittleEndian.Uint32((*fr)[39:43]), nil
	}

	cOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5])
	childPtr := binary.LittleEndian.Uint32((*fr)[cOff+9 : cOff+13])

	return childPtr, nil
}

// Insert - inserts a new key and value to the index ahd performs splits incase of an overflow
// returns error if any
func (bp *BpTree) Insert(key []byte, val []byte) error {
	var currNode *bm.Frame
	var currNodeBuff *[]byte
	var prevNode *bm.Frame
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
		bp.height++
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
	var ptrArr []byte
	for !isLeaf {
		if prevNode != nil {
			prevNode.Release(false)
			prevNode.Unreference()
		}

		// itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
		_, ptrArr, err = findKeyPath(currNodeBuff, key)
		if err != nil {
			currNode.Release(false)
			currNode.Unreference()
			return err
		}

		if ptrArr == nil {
			panic("No child pointer returned.")
		}

		ptr = binary.LittleEndian.Uint32(ptrArr[4:])

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

	insertionIdxArr, err := findInsertionIdx(currNodeBuff, key, 0, itemCount-1)
	if err != nil {
		if possibleOverflow && prevNode != nil {
			prevNode.Release(false)
			prevNode.Unreference()
		}

		currNode.Release(false)
		currNode.Unreference()
		return err
	}

	if insertionIdxArr == nil {
		if possibleOverflow && prevNode != nil {
			prevNode.Release(false)
			prevNode.Unreference()
		}

		currNode.Release(false)
		currNode.Unreference()
		return BTreeError{Message: "Unable to insert to node"}
	}

	insertionIdx := binary.LittleEndian.Uint32(insertionIdxArr[1:])

	keyAtIdx, err := bp.getKeyAtIdx(currNodeBuff, uint32(insertionIdx))
	if bytes.Equal(keyAtIdx, key) {
		bp.lgger.Write(bp.pkgName, "Insert", logger.LevelDebug, "key already exists, no possible overflow...", nil)
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

		// mark node dirty
		// helpers.SetFlag(&(*currNodeBuff)[0], []int{pgr.Dirty})
		currNode.MarkDirty()

		currNode.Release(false)
		currNode.Unreference()

		return nil
	} else {
		bp.lgger.Write(bp.pkgName, "Insert", logger.LevelDebug, "key does not exist, possible overflow...", nil)
		// no key exists
		err = bp.insertToFrame(currNodeBuff, key, 0, val)
		bp.lgger.Write(bp.pkgName, "Insert", logger.LevelDebug, fmt.Sprintf("Post insert to frame. Possible Overflow --> %t...\n", possibleOverflow), nil)
		if err != nil {
			bp.lgger.Write(bp.pkgName, "Insert", logger.LevelError, err.Error(), nil)
			if possibleOverflow {
				prevNode.Release(false)
				prevNode.Unreference()
			}

			currNode.Release(false)
			currNode.Unreference()
			return err
		}

		// FIX: Remove debug code below
		if binary.LittleEndian.Uint32((*currNodeBuff)[29:33]) == 51 {
			panic("CURR NODE LOW OFF IS 51 AFTER ADDING A KEY/VAL")
		}

		if !possibleOverflow {
			currNode.Release(false)
			currNode.Unreference()

			return nil
		}

		// split overflown node and propagate splits
		var i uint32 = 0
		h := bp.height
		for possibleOverflow && i < h {

			newSepKey, newFramePid, err := bp.split(currNodeBuff)
			if err != nil {
				bp.lgger.Write(bp.pkgName, "Insert", logger.LevelError, err.Error(), nil)
				if prevNode != nil {
					prevNode.Release(false)
					prevNode.Unreference()
				}
				currNode.Release(false)
				currNode.Unreference()
				return err
			}

			// FIX: Remove debug code
			if binary.LittleEndian.Uint32((*currNodeBuff)[29:33]) == 51 {
				panic("CURR NODE LOW OFF IS 51 AFTER ADDING A KEY/VAL")
			}

			// add seperatorkey to parent keys and newFramePid to ptrs
			parentPath := btPath.Pop()
			if parentPath == nil || binary.LittleEndian.Uint32((*currNodeBuff)[1:5]) == parentPath.pid {
				// no parent or only one level, create new root node
				bp.lgger.Write(bp.pkgName, "Insert", logger.LevelDebug, fmt.Sprintf("No parent, creating new root\nrootPID -> %d\tJUST SPLIT NODE PID -> %d\n", bp.root.Load(), binary.LittleEndian.Uint32((*currNodeBuff)[1:5])), nil)
				newRoot, err := bp.buffermanager.NewFrame(true, true)
				if err != nil {
					panic(err)
				}

				newRoot.Acquire(false)
				newRootBuff, _, err := newRoot.RawBufferSlice()
				if err != nil {
					panic(err)
				}

				err = bp.insertToNewRoot(newRootBuff, newSepKey, binary.LittleEndian.Uint32((*currNodeBuff)[1:5]), newFramePid)
				if err != nil {
					panic(err)
				}

				// set new root
				bp.updateRootPage(binary.LittleEndian.Uint32((*newRootBuff)[1:5]))
				err = bp.incrementTreeHeight()
				if err != nil {
					panic(err)
				}
				// bp.root.Store(binary.LittleEndian.Uint32((*newRootBuff)[1:5]))
				// printNodeContent(newRootBuff)

				// release latches
				newRoot.Release(false)
				newRoot.Unreference()

				currNode.Release(false)
				currNode.Unreference()

				possibleOverflow = false
			} else {
				// add new sepkey and frame pid to parent
				if parentPath.height == treeHeight-1 {
					// parent is "prevNode", latches already acquired
					prevNodeBuff, _, err := prevNode.RawBufferSlice()
					if err != nil {
						panic(fmt.Sprintf("Unable to get prev Node buffer: %s", err.Error()))
					}

					err = bp.insertToFrame(prevNodeBuff, newSepKey, newFramePid, nil)
					if err != nil {
						panic(fmt.Sprintf("Unable to add new separator key and child: %s", err.Error()))
					}

					// release current node latches
					currNode.Release(false)
					currNode.Unreference()

					// check for overflow
					possibleOverflow, err = nodeHasOverflown(prevNodeBuff)
					if err != nil {
						panic(err)
					}
					if !possibleOverflow {
						// possibleOverflow = false
						prevNode.Release(false)
						prevNode.Unreference()
						prevNode = nil
					} else {
						currNode = prevNode
						currNodeBuff = prevNodeBuff
						prevNode = nil
					}
				} else {
					// parent is in a higher level in the tree.
					parentNode, _, err := bp.buffermanager.Get(parentPath.pid)
					if err != nil {
						panic(fmt.Sprintf("Unable to retrieve parent from buffermanager: %s", err.Error()))
					}

					// prevNodeBuff, _, _ := prevNode.RawBufferSlice()
					parentNode.Acquire(false)
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
					possibleOverflow, err = nodeHasOverflown(parentNodeBuff)
					if err != nil {
						panic(err)
					}
					if !possibleOverflow {
						parentNode.Release(false)
						parentNode.Unreference()
					} else {
						currNode = parentNode
						currNodeBuff = parentNodeBuff
					}
				}
			}

			i++
		}
		return nil
	}
}

func (bp *BpTree) Get(key []byte) (val []byte, e error) {
	var currNode *bm.Frame
	var currNodeBuff *[]byte
	var err error
	var isLeaf bool

	rootPid := bp.root.Load()
	if rootPid == 0 {
		return nil, nil
	}
	bp.lgger.Write(bp.pkgName, "Get", logger.LevelDebug, fmt.Sprintf("(GET) RootNodePID: %d\n", rootPid), nil)

	currNode, _, err = bp.buffermanager.Get(rootPid)
	if err != nil {
		return nil, err
	}

	// acquire shared latch
	currNode.Acquire(true)

	currNodeBuff, _, err = currNode.RawBufferSlice()
	if err != nil {
		return nil, err
	}

	isLeaf = !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal)

	var idx int32
	var ptr uint32
	var ptrArr []byte
	for !isLeaf {
		// itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
		_, ptrArr, err = findKeyPath(currNodeBuff, key)

		if err != nil {
			currNode.Release(true)
			currNode.Unreference()
			return nil, err
		}

		if ptrArr == nil {
			panic("No child pointer found.")
		}

		ptr = binary.LittleEndian.Uint32(ptrArr[4:])

		if ptr == 0 {
			currNode.Release(true)
			currNode.Unreference()
			return nil, nil
		}

		// release currNode's shared latch before acquiring the child's latch
		currNode.Release(true)
		currNode.Unreference()

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
			currNode.Unreference()
			return nil, err
		}

		// check if is a leaf node
		isLeaf = !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal)
	}

	itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
	idx, err = findKeyIndex(currNodeBuff, key, 0, itemCount-1)
	if err != nil {
		return nil, err
	}

	if idx < 0 {
		currNode.Release(true)
		currNode.Unreference()
		return nil, nil
	}

	cOff := binary.LittleEndian.Uint32((*currNodeBuff)[pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(idx*pgr.CELL_POINTER_SIZE_BYTE)+5])
	kLen := binary.LittleEndian.Uint32((*currNodeBuff)[cOff+1 : cOff+5])
	vLen := binary.LittleEndian.Uint32((*currNodeBuff)[cOff+5 : cOff+9])
	value := make([]byte, vLen)
	copy(value, (*currNodeBuff)[cOff+13+kLen:cOff+13+kLen+vLen])

	currNode.Release(true)
	currNode.Unreference()

	return value, nil
}

// Delete deletes key from node and returns the value associated with the key
// or error if any
func (bp *BpTree) Delete(key []byte) (val []byte, e error) {
	var currNode *bm.Frame
	var prevNode *bm.Frame
	var currNodeBuff *[]byte
	// var prevNodeBuff *[]byte
	var err error
	var isLeaf bool

	rootPid := bp.root.Load()
	if rootPid == 0 {
		return nil, nil
	}

	currNode, _, err = bp.buffermanager.Get(rootPid)
	if err != nil {
		return nil, err
	}

	// acquire exlusive latch
	currNode.Acquire(false)

	currNodeBuff, _, err = currNode.RawBufferSlice()
	if err != nil {
		return nil, err
	}

	// initialize BTStack
	btPath, err := NewBTStack(rootPid)
	if err != nil {
		panic(err.Error())
	}
	defer btPath.Clear()

	var treeHeight uint32 = 0
	// add root node to path
	// btPath.Add(&TraversePath{pid: rootPid, idx: 0, height: treeHeight})
	// treeHeight++

	isLeaf = !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal)

	var idx int32
	var ptr uint32
	var ptrArr []byte
	var sepKeys [][]byte
	for !isLeaf {
		if prevNode != nil {
			prevNode.Release(false)
			prevNode.Unreference()
		}

		// itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
		sepKeys, ptrArr, err = findKeyPath(currNodeBuff, key)
		if err != nil {
			currNode.Release(false)
			currNode.Unreference()
			return nil, err
		}

		if ptrArr == nil {
			panic("No  child pointer found")
		}

		ptr = binary.LittleEndian.Uint32(ptrArr[4:])

		if ptr == 0 {
			if len(sepKeys) > 0 {
				bp.lgger.Write(bp.pkgName, "Delete", logger.LevelError, fmt.Sprintf("sepKey -> %s\n", sepKeys[0]), nil)
			}
			if len(sepKeys) == 2 {
				bp.lgger.Write(bp.pkgName, "Delete", logger.LevelError, fmt.Sprintf("rightKey -> %s\n", sepKeys[1]), nil)
			}
			panic("retrieved invalid child ptr pid: 0")
		}

		idx = int32(binary.LittleEndian.Uint32(ptrArr[:4]))

		if len(sepKeys) > 1 {
			btPath.Add(&TraversePath{pid: binary.LittleEndian.Uint32((*currNodeBuff)[1:5]), idx: uint32(idx), height: treeHeight, sepKey: sepKeys[0], rightKey: sepKeys[1]})
		} else {
			btPath.Add(&TraversePath{pid: binary.LittleEndian.Uint32((*currNodeBuff)[1:5]), idx: uint32(idx), height: treeHeight, sepKey: sepKeys[0]})
		}
		treeHeight++

		prevNode = currNode
		// prevNodeBuff = currNodeBuff
		currNode, _, err = bp.buffermanager.Get(ptr)
		if err != nil {
			prevNode.Release(false)
			prevNode.Unreference()
			return nil, err
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
			return nil, err
		}

		// check if is a leaf node
		isLeaf = !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal)
	}

	// reached leaf node
	itemCount := binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
	possibleUnderflow := itemCount == pgr.ORDER
	if !possibleUnderflow && prevNode != nil {
		// no possible underflow, release previous node's latch
		prevNode.Release(false)
		prevNode.Unreference()
	}

	_, delVal, err := bp.deleteFromNode(currNodeBuff, key, false)
	if err != nil {
		if possibleUnderflow {
			prevNode.Release(false)
			prevNode.Unreference()
		}

		return nil, err
	}

	itemCount = binary.LittleEndian.Uint32((*currNodeBuff)[17:21])
	underflow := itemCount < pgr.ORDER
	isRoot := bp.root.Load() == binary.LittleEndian.Uint32((*currNodeBuff)[1:5])

	if !underflow || (underflow && isRoot) {
		currNode.Release(false)
		currNode.Unreference()
		return delVal, nil
	}

	var mergeSib *bm.Frame
	var mergeSibPid uint32
	var mergeSibBuff *[]byte
	var parent *bm.Frame
	var parentBuff *[]byte
	var newSepKey []byte
	var parSepKey []byte
	leftMerge := false
	i := 0
	for underflow && !isRoot {
		// merge
		// get idx of the paren't child pointer that points to curr node
		currNodeIdx, err := btPath.parentPtrIdx()
		if err != nil {
			panic(err.Error())
		}

		if currNodeIdx < 0 {
			panic("child pointer index not found")
		}

		if currNodeIdx > 0 {
			// check for left sibling.
			mergeSibPid = binary.LittleEndian.Uint32((*currNodeBuff)[47:51])
			if mergeSibPid == 0 {
				panic("No valid left sibling found")
			}
		} else {
			// first node. No left sibling.
			// check for right sibling
			mergeSibPid = binary.LittleEndian.Uint32((*currNodeBuff)[43:47])
			if mergeSibPid == 0 {
				panic("No sibling found")
			}
			leftMerge = true
		}

		mergeSib, _, err = bp.buffermanager.Get(mergeSibPid)
		if err != nil {
			return nil, err
		}

		if mergeSib == nil {
			panic(fmt.Sprintf("Unable to retrieve node with pid %d", mergeSibPid))
		}

		// acquire merge sibling X-latch
		mergeSib.Acquire(false)

		mergeSibBuff, _, err = mergeSib.RawBufferSlice()
		if err != nil {
			return nil, err
		}

		if mergeSibBuff == nil {
			panic("no buffer found for sibling node.")
		}

		// retrieve the appropriate seperator key. If first iteration, the immediate sep key
		// will be stored in "sepKey" mtherwise we retrieve it from btPath
		if i == 0 {
			i++
			if leftMerge {
				if len(sepKeys) <= 1 {
					panic("No right seperator key provided for the left merge")
				}
				parSepKey = sepKeys[1]
			} else {
				parSepKey = sepKeys[0]
			}
		} else {
			if leftMerge {
				parSepKey, err = btPath.parentRightSepKey()
				if err != nil {
					panic(fmt.Sprintf("Could not retrieve seperator key: %s", err.Error()))
				}
			} else {
				parSepKey, err = btPath.parentSepKey()
				if err != nil {
					panic(fmt.Sprintf("Could not retrieve seperator key: %s", err.Error()))
				}
			}
		}

		if parSepKey == nil {
			panic("No seperator key found in parent")
		}

		bp.lgger.Write(bp.pkgName, "Delete", logger.LevelDebug, fmt.Sprintf("Merging and deleting node %d ,left merge -> %t\n", binary.LittleEndian.Uint32((*currNodeBuff)[1:5]), leftMerge), nil)
		if leftMerge {
			newSepKey, err = bp.merge(currNodeBuff, mergeSibBuff, parSepKey, leftMerge)
			if err != nil {
				return nil, err
			}
		} else {
			newSepKey, err = bp.merge(mergeSibBuff, currNodeBuff, parSepKey, leftMerge)
			if err != nil {
				return nil, err
			}
		}
		// Release merge sibling's latch
		mergeSib.Release(false)
		mergeSib.Unreference()

		// retrieve parent
		// if at leaf node level, parent node is prevNode and is already latched
		if !helpers.BitIsSet(&(*currNodeBuff)[0], pgr.IsInternal) {
			parent = prevNode

			// release prevNode in BtStack
			path := btPath.Pop()
			fmt.Printf("Retrieved parent SepKey --> %s\n", path.sepKey)
			// sepKey = path.sepKey
		} else {
			parentPath := btPath.Pop()
			if parentPath.pid == 0 {
				panic("No parent found")
			}
			parent, _, err = bp.buffermanager.Get(parentPath.pid)
			if err != nil {
				return nil, err
			}

			if parent == nil {
				panic("retrieved parent node is nil.")
			}

			parent.Acquire(false)

			// set sepkey
			// sepKey = parentPath.sepKey
		}

		parentBuff, _, err = parent.RawBufferSlice()
		if err != nil {
			parent.Release(false)
			parent.Unreference()
			return nil, err
		}

		if newSepKey != nil {
			// nodes rebalanced, add the new separator key to parent node
			bp.lgger.Write(bp.pkgName, "Delete", logger.LevelDebug, "Nodes rebalanced...", nil)
			_, err = replaceSepKey(parentBuff, parSepKey, newSepKey)
			if err != nil {
				parent.Release(false)
				parent.Unreference()
				return nil, err
			}

			// release latches
			currNode.Release(false)
			currNode.Unreference()
			parent.Release(false)
			parent.Unreference()
			underflow = false
		} else {
			bp.lgger.Write(bp.pkgName, "Delete", logger.LevelDebug, "Nodes merged...", nil)
			// nodes merged.
			currRoot := bp.root.Load()
			parentPid := binary.LittleEndian.Uint32((*parentBuff)[1:5])
			parentItemCount := binary.LittleEndian.Uint32((*parentBuff)[17:21])
			if parentItemCount == 1 && parentPid == currRoot {
				// parent node will have only one item left after deleting merged child node.
				// collapse tree and set remaining child as new root
				bp.lgger.Write(bp.pkgName, "Delete", logger.LevelDebug, "collapse tree...", nil)
				var newRootPid uint32
				if leftMerge {
					// use right child
					newRootPid = binary.LittleEndian.Uint32((*parentBuff)[39:43])
				} else {
					// use first child
					offset := binary.LittleEndian.Uint32((*parentBuff)[pgr.HEADER_SIZE_BYTES+1 : pgr.HEADER_SIZE_BYTES+5])
					if offset == 0 {
						panic("Invalid cell offset")
					}

					newRootPid = binary.LittleEndian.Uint32((*parentBuff)[offset+9 : offset+13])
					if newRootPid == 0 {
						panic("Invalid new root pid")
					}
				}

				err = bp.updateRootPage(newRootPid)
				if err != nil {
					panic("Unable to update root page")
				}

				err = bp.decrementTreeHeight()
				if err != nil {
					currNode.Release(false)
					currNode.Unreference()
					parent.Release(false)
					parent.Unreference()
					return nil, err
				}

				parent.MarkDead()
				parent.Release(false)
				parent.Unreference()
				currNode.Release(false)
				currNode.Unreference()
				underflow = false
			} else if parentItemCount == 1 && parentPid != rootPid {
				bp.lgger.Write(bp.pkgName, "Delete", logger.LevelError, "Invalid state. Internal node with only one item.", nil)
				panic("Invalid state. Internal node with only one item.")
			} else {
				// delete key in parent node
				_, _, err = bp.deleteFromNode(parentBuff, parSepKey, leftMerge)
				if err != nil {
					currNode.Release(false)
					currNode.Unreference()
					parent.Release(false)
					parent.Unreference()
					return nil, err
				}

				itemCount = binary.LittleEndian.Uint32((*parentBuff)[17:21])
				underflow = itemCount < pgr.ORDER
				isRoot = bp.root.Load() == binary.LittleEndian.Uint32((*parentBuff)[1:5])

				currNode.Release(false)
				currNode.Unreference()

				if underflow && !isRoot {
					currNode = parent
					currNodeBuff = parentBuff

				} else {
					// release parent latches
					parent.Release(false)
					parent.Unreference()
				}
			}
		}

		leftMerge = false

	}

	return delVal, nil
}

// nodeHasOverflown returns true if node has overflown, else false
func nodeHasOverflown(fr *[]byte) (overflow bool, e error) {
	if fr == nil {
		return false, BTreeError{Message: "No node provided"}
	}
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	return itemCount > (pgr.ORDER * 2), nil
}

// findInsertionIdx searches the frame cell pointers using binary search to
// find the index for 'searchKey'. It returns the index where the searchKey
// can be inserted.
// returns a 5 byte array with the Most Signigicant Bit(MSB) of the first byte set to 1 if
// exact match found else 0, and error if any
func findInsertionIdx(fr *[]byte, searchKey []byte, startIdx uint32, endIdx uint32) (idx []byte, e error) {
	if fr == nil {
		return nil, BTreeError{Message: "frame not provided"}
	}

	if searchKey == nil {
		return nil, BTreeError{Message: "No search key provided"}
	}

	if startIdx > endIdx {
		return nil, BTreeError{Message: fmt.Sprintf("(findInsertionIdx) Invalid start: %d and end: %d index", startIdx, endIdx)}
	}

	var index [5]byte
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount == 0 {
		// empty node
		return index[:], nil
	}

	if endIdx > itemCount-1 {
		return nil, BTreeError{Message: fmt.Sprintf("Invalid end index: %d, with itemcount: %d", endIdx, itemCount)}
	}

	arrLen := (endIdx - startIdx) + 1
	midPoint := startIdx + uint32(math.Round(float64(arrLen/2)))
	maxIter := math.Round(math.Log(float64(itemCount))/math.Log(2)) + 1
	for range int(maxIter) {
		// get key at index
		cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
		keyLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
		key := (*fr)[cellOff+13 : cellOff+13+keyLen]

		// compare
		s := bytes.Compare(key, searchKey)

		switch s {
		case 0:
			// found exact key
			helpers.SetFlag(&index[0], []int{exactMatchBitPos})
			binary.LittleEndian.PutUint32(index[1:], midPoint)
			return index[:], nil
		case 1:
			// searchKey < key
			if itemCount == 1 {
				return index[:], nil
			}
			// compare with item at previous index
			prevCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint-1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint-1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
			prevKeyLen := binary.LittleEndian.Uint32((*fr)[prevCellOff+1 : prevCellOff+5])
			prevKey := (*fr)[prevCellOff+13 : prevCellOff+13+prevKeyLen]
			if n := bytes.Compare(prevKey, searchKey); n < 0 {
				// searchKey > prevKey
				binary.LittleEndian.PutUint32(index[1:], midPoint)
				return index[:], nil
			} else {
				// searchKey < prevKey
				if arrLen == 2 {
					// check key at previous index instead of recursing
					midPoint--
					cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
					keyLen = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
					key = (*fr)[cellOff+13 : cellOff+13+keyLen]

					if s = bytes.Compare(key, searchKey); s >= 0 {
						binary.LittleEndian.PutUint32(index[1:], midPoint)
						return index[:], nil
					} else {
						binary.LittleEndian.PutUint32(index[1:], midPoint+1)
						return index[:], nil
						// return int32(midPoint + uint32(1)), nil
					}
				}

				endIdx = midPoint
				arrLen = (endIdx - startIdx) + 1
				midPoint = startIdx + uint32(math.Round(float64(arrLen/2)))
				// return findInsertionIdx(fr, searchKey, startIdx, midPoint)
			}
		default:
			// searchkey > key
			if itemCount == 1 {
				binary.LittleEndian.PutUint32(index[1:], 1)
				return index[:], nil
				//  return 1, nil
			}

			if midPoint == endIdx {
				// search key is greater than the last item, return midpoint+1
				binary.LittleEndian.PutUint32(index[1:], midPoint+1)
				return index[:], nil

			}

			if arrLen == 2 {
				// check item at next index
				nextCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
				nextKeyLen := binary.LittleEndian.Uint32((*fr)[nextCellOff+1 : nextCellOff+5])
				nextKey := (*fr)[nextCellOff+13 : nextCellOff+13+nextKeyLen]

				if n := bytes.Compare(nextKey, searchKey); n < 0 {
					if midPoint == endIdx {
						binary.LittleEndian.PutUint32(index[1:], midPoint+1)
						return index[:], nil
						// return int32(midPoint + 1), nil
					} else {
						binary.LittleEndian.PutUint32(index[1:], midPoint+2)
						return index[:], nil
						// return int32(midPoint + 2), nil
					}
				} else {
					binary.LittleEndian.PutUint32(index[1:], midPoint+1)
					return index[:], nil
					//  return int32(midPoint + 1), nil
				}
			}

			startIdx = midPoint
			arrLen = (endIdx - startIdx) + 1
			midPoint = startIdx + uint32(math.Round(float64(arrLen/2)))
			// return findInsertionIdx(fr, searchKey, midPoint, endIdx)
		}
	}

	return nil, nil
}

// findKeyIndex searches the frame cell pointers using binary search to
// find the index for 'searchKey'. It returns the exact index of the searchKey.
// if item does not exist, returns -1 else idx, and error if any.
// used mostly with leaf nodes or when finding exact index of a key in an internal
// node
func findKeyIndex(fr *[]byte, searchKey []byte, startIdx uint32, endIdx uint32) (idx int32, e error) {
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
// returns:
//
//   - an array of seperator keys with the seperator key used
//     to get the child pointer at the first index and its  right neighbour
//     on index 1 if it exists.
//   - the child pointer array with the first 4 bytes
//     containing the index if the child pointer and the next 4 containing the
//     actual uint32 child pointer at which to follow to retrieve the key.
//   - error if any.
//
// Used when traversing the B+ Tree to find the next child pointer for a
// given key.
// returns sepKey, childPtr pid, or zero if no child found and error if any
func findKeyPath(fr *[]byte, searchKey []byte) (sepKey [][]byte, child []byte, e error) {
	if fr == nil {
		return nil, nil, BTreeError{Message: "frame not provided"}
	}

	internalNode := helpers.BitIsSet(&(*fr)[0], pgr.IsInternal)
	if !internalNode {
		return nil, nil, BTreeError{Message: "Expected internal node, got leaf node"}
	}

	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	var startIdx uint32 = 0
	var endIdx uint32 = itemCount - 1
	var rightKey []byte
	childPtrArr := make([]byte, 8)

	arrLen := itemCount
	midPoint := startIdx + uint32(math.Round(float64(arrLen/2)))

	// set max iterations
	maxIter := math.Round(math.Log(float64(itemCount))/math.Log(2)) + 1

	for range int(maxIter) {
		// get key at index
		cellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
		keyLen := binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
		key := (*fr)[cellOff+13 : cellOff+13+keyLen]

		// compare
		s := bytes.Compare(key, searchKey)

		switch s {
		case 0:
			// get pointer at midpoint+1
			// get key right of current key, used in left-merges
			if midPoint != itemCount-1 {
				cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+5])
				keyLen = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
				rightKey = (*fr)[cellOff+13 : cellOff+13+keyLen]
			}

			// check if is the last key
			if itemCount-1 == midPoint {
				copy(childPtrArr[:4], (*fr)[17:21]) // last pointer in the node
				copy(childPtrArr[4:], (*fr)[39:43])
				return [][]byte{key, rightKey}, childPtrArr, nil
			}
			actualCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
			// ptr := binary.LittleEndian.Uint32((*fr)[actualCellOff+9 : actualCellOff+13])
			binary.LittleEndian.PutUint32(childPtrArr[:4], midPoint+1)
			copy(childPtrArr[4:], (*fr)[actualCellOff+9:actualCellOff+13]) // copy child ptr
			return [][]byte{key, rightKey}, childPtrArr, nil
		case -1:
			// key < searchKey
			if arrLen == 1 {
				// get key right of current key
				if midPoint != itemCount-1 {
					cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+5])
					keyLen = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
					rightKey = (*fr)[cellOff+13 : cellOff+13+keyLen]
				}

				if itemCount-1 == midPoint {
					copy(childPtrArr[:4], (*fr)[17:21]) // last pointer in the node
					copy(childPtrArr[4:], (*fr)[39:43])
					return [][]byte{key, rightKey}, childPtrArr, nil
				}
				// get pointer at midpoint+1
				actualCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+((midPoint+1)*pgr.CELL_POINTER_SIZE_BYTE)+5])
				// ptr := binary.LittleEndian.Uint32((*fr)[actualCellOff+9 : actualCellOff+13])
				binary.LittleEndian.PutUint32(childPtrArr[:4], midPoint+1)
				copy(childPtrArr[4:], (*fr)[actualCellOff+9:actualCellOff+13]) // copy child ptr
				return [][]byte{key, rightKey}, childPtrArr, nil
			}

			startIdx = midPoint
			arrLen = (endIdx - startIdx) + 1
			midPoint = startIdx + uint32(math.Round(float64(arrLen/2)))
		default:
			// key > searchKey; s == 1
			// get key right of current key
			if midPoint != itemCount-1 {
				cellOff = binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE+5])
				keyLen = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
				rightKey = (*fr)[cellOff+13 : cellOff+13+keyLen]
			}

			if arrLen == 1 {
				// get pointer at midpoint
				actualCellOff := binary.LittleEndian.Uint32((*fr)[pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+1 : pgr.HEADER_SIZE_BYTES+(midPoint*pgr.CELL_POINTER_SIZE_BYTE)+5])
				//  ptr := binary.LittleEndian.Uint32((*fr)[actualCellOff+9 : actualCellOff+13])
				binary.LittleEndian.PutUint32(childPtrArr[:4], midPoint)
				copy(childPtrArr[4:], (*fr)[actualCellOff+9:actualCellOff+13]) // copy child ptr
				return [][]byte{key, key}, childPtrArr, nil
			}

			endIdx = midPoint - 1

			arrLen = (endIdx - startIdx) + 1
			midPoint = startIdx + uint32(math.Round(float64(arrLen/2)))
		}
	}

	return nil, nil, nil
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
	if e != nil {
		return -1, e
	}

	if idx < 0 {
		return -1, BTreeError{Message: fmt.Sprintf("Key %s not found in node.", oldKey)}
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

	// mark frame dirty
	helpers.SetFlag(&(*fr)[0], []int{pgr.Dirty})

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
func NewBpTree(buffMan *bm.BufferManager, wal *wal.WAL, lgger *logger.BaobabLogger) (index *BpTree, e error) {
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
		lgger:         lgger,
		pkgName:       "bptree",
	}

	err = bp.initTreeMetadata()
	if err != nil {
		return nil, err
	}

	return bp, nil
}
