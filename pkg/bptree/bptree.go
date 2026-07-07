// package bptree implements a b+ tree
package bptree

import (
	"encoding/binary"
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
	// 1. update metadata page with current root page
	// 2. mark metadata page as dirty
	// 3. Get curr root page
	// 4. unref curr root page

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
