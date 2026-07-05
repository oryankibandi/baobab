// package bptree implements a b+ tree
package bptree

import (
	"encoding/binary"
	"sync/atomic"

	bm "github.com/oryankibandi/baobab/pkg/buffermanager"
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
