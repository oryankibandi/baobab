// package bptree implements a b+ tree
package bptree

import (
	"bytes"
	"encoding/binary"
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
func (bp *BpTree) split(fr *[]byte, isInternal bool) (sepKey []byte, newFramePid uint32, e error) {
	// check if full
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])
	if itemCount <= 2*bp.order {
		panic("provided frame has not overflown")
	}

	// request new frame from buffermanager
	newFr, err := bp.buffermanager.NewFrame(isInternal, false)
	if err != nil {
		return nil, 0, err
	}

	defer newFr.Unreference()
	newFr.Acquire(false)
	defer newFr.Release(false)
	newFrBuff, err := newFr.ByteData()
	if err != nil {
		panic(err.Error())
	}

	// move items from left node to right(new) node and update upper and lower offsets
	var seperatorKey []byte
	var rightNodeRightPtr uint32
	var cellPtr [pgr.CELL_POINTER_SIZE_BYTE]byte
	var cellOff uint32
	var cellEndOff uint32
	var cellKeySize uint32
	var cellValSize uint32
	var cellSize uint32
	var newFrCellOffset uint32 = pgr.LOWER_PADDING_BYTES
	for i := bp.order; i < itemCount; i++ {
		copy(cellPtr[:], (*fr)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE):pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE])
		cellOff = binary.LittleEndian.Uint32(cellPtr[1:])

		// read cell sizes
		cellKeySize = binary.LittleEndian.Uint32((*fr)[cellOff+1 : cellOff+5])
		cellValSize = binary.LittleEndian.Uint32((*fr)[cellOff+5 : cellOff+9])
		cellEndOff = cellOff + (13 + cellKeySize + cellValSize)
		cellSize = 13 + cellKeySize + cellValSize

		if i == 0 {
			// first sep key promoted to parent
			copy(seperatorKey, (*fr)[13:13+cellKeySize])
			if isInternal {
				// store old right child metadata and update to new right child(curr cell page pointer)
				rightNodeRightPtr = binary.LittleEndian.Uint32((*fr)[39:43])
				copy((*fr)[39:43], (*fr)[cellOff+9:cellOff+13])
				continue
			}
		}
		// update cell offset in new frame cell pointer
		binary.LittleEndian.PutUint32(cellPtr[1:], uint32(newFrCellOffset))

		// move cell pointer to new frame
		copy((*newFrBuff)[(pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)):pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE], cellPtr[:])
		// copy cell to new frame
		copy((*newFrBuff)[newFrCellOffset-cellSize:newFrCellOffset], (*fr)[cellEndOff:cellOff])

		// update cell offset
		newFrCellOffset -= cellSize

		// clear old cell pointer and cell offset
		clear((*fr)[pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE) : pgr.HEADER_SIZE_BYTES+(i*pgr.CELL_POINTER_SIZE_BYTE)+pgr.CELL_POINTER_SIZE_BYTE])
		clear((*fr)[cellEndOff:cellOff])
	}

	// update new node's right child
	binary.LittleEndian.PutUint32((*newFrBuff)[39:43], rightNodeRightPtr)

	// update numItems in each node/frame
	binary.LittleEndian.PutUint32((*fr)[17:21], pgr.ORDER)
	binary.LittleEndian.PutUint32((*newFrBuff)[17:21], itemCount-pgr.ORDER)

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
		copy((*siblBuff)[47:51], (*newFrBuff)[1:4])

		// update new frame's right sibling pointer
		copy((*newFrBuff)[43:47], (*siblBuff)[1:4])
	}

	// update new frame's left sibling pointer
	copy((*newFrBuff)[47:51], (*fr)[1:4])
	// update left frame's  right sibling pointer
	copy((*fr)[43:47], (*newFrBuff)[1:4])

	return seperatorKey, binary.LittleEndian.Uint32((*newFrBuff)[1:4]), nil
}

// merge merges left node and right node.
// in the case that the keys can be redistributed, the nodes
// rebalanced and the new seperator key will be returned.
// latches for both nodes should be acquired before calling merge()
func (bp *BpTree) merge(leftNode *[]byte, rightNode *[]byte, sepKey []byte) (newSepKey []byte, e error) {
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

	if leftNodeItemCount+1 >= pgr.ORDER+1 && rightNodeItemCount+1 >= pgr.ORDER+1 {
		// both nodes already above the threshold. No merge or rebalancing required
		return nil, nil
	}

	if leftNodeItemCount+1+rightNodeItemCount+1 > (pgr.ORDER*2)+1 {
		// rebalance
		if leftNodeItemCount == rightNodeItemCount {
			// nodes balanced
			return nil, nil
		}

		var donor *[]byte
		var receiver *[]byte
		var donorDirection bool // true if moving  keys from right to left node, else false
		if leftNodeItemCount > rightNodeItemCount {
			donor = leftNode
			receiver = rightNode
			donorDirection = false
		} else {
			donor = rightNode
			receiver = leftNode
			donorDirection = true
		}

		deficit := math.Abs(float64(leftNodeItemCount - rightNodeItemCount))

		// demote separator key

		if !donorDirection {

		}
	} else {
		// merge
	}
}

// insertToFrame inserts key and value/childPtr to a frame and shifts
// cellpointers if necessary.
func insertToFrame(fr *[]byte, key []byte, childPtr uint32, val []byte) error {
	if fr == nil {
		return BTreeError{Message: "No frame provided"}
	}

	if key == nil {
		return BTreeError{Message: "No frame provided"}
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

	// find appropriate index to add key and shit keys if need be.
	itemCount := binary.LittleEndian.Uint32((*fr)[17:21])

	var cOff uint32
	var ptrOff uint32
	var cKSize uint32
	var insertIdx uint32 = itemCount
	for i := range itemCount {
		ptrOff = i*pgr.CELL_POINTER_SIZE_BYTE + pgr.HEADER_SIZE_BYTES
		cOff = binary.LittleEndian.Uint32((*fr)[ptrOff+1:])
		cKSize = binary.LittleEndian.Uint32((*fr)[cOff+1 : cOff+5])
		cKey := (*fr)[cOff+13 : cOff+13+cKSize]

		if s := bytes.Compare(cKey, key); s > 0 {
			insertIdx = i
			break
		}
	}

	// write cell contents
	binary.LittleEndian.PutUint32((*fr)[cellStartOff+1:cellStartOff+5], uint32(keyLen))
	binary.LittleEndian.PutUint32((*fr)[cellStartOff+5:cellStartOff+9], uint32(valLen))
	if insertIdx == itemCount {
		// set key as right child pointer in header and set the previous
		// right child pointer in the same cell
		// +------+------+------+------+
		// |  K1  |  K2  |  K3  |      |
		// +------+------+------+  P4  + <- right child ptr(in header)
		// |  P1  |  P2  |  P3  |      |
		// +------+------+------+------+

		copy((*fr)[cellStartOff+9:cellStartOff+13], (*fr)[39:43])
		binary.LittleEndian.PutUint32((*fr)[39:43], childPtr)
	} else {
		binary.LittleEndian.PutUint32((*fr)[cellStartOff+9:cellStartOff+13], childPtr)
	}
	copy((*fr)[cellStartOff+13:cellStartOff+13+uint32(keyLen)], key)

	if !internal {
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
