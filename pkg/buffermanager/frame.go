package buffermanager

import (
	// "fmt"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/oryankibandi/baobab/pkg/pager"
)

// Frame - a slot in the buffer that is used to store cached pages
type Frame struct {
	// 8K page. Memory initialized manually
	// 8224 bytes
	page pager.Page
	// lsn  [pager.LSN_SIZE_BYTE]byte

	isInternal atomic.Bool
	isDeleted  atomic.Bool

	dirty atomic.Bool

	key uint32

	parentEntry *clockentry

	// Mutex field. 24 bytes
	mu sync.RWMutex
}

func (f *Frame) isDirty() bool {
	return f.dirty.Load()
}

// Mark an entry/frame as dirty
func (f *Frame) MarkDirty() {
	f.dirty.Store(true)
	f.page.MarkDirty()
}

// Mark an entry/frame as clean
func (f *Frame) MarkClean() {
	f.dirty.Store(false)
	f.page.MarkClean()
}

// MarkDead mark frame as dead
func (f *Frame) MarkDead() {
	f.isDeleted.Store(true)
	f.dirty.Store(true)
	f.page.MarkAsDead()
}

// IsDead returns true if frame has been marked dead, else false
func (f *Frame) IsDead() bool {
	return f.isDeleted.Load()
}

func (f *Frame) getKey() uint32 {
	return f.key
}

func (f *Frame) GetPage() *pager.Page {
	// f.mu.RLock()
	// defer f.mu.RUnlock()

	return &f.page
}

// sets data on a frame/entry from a page
func (f *Frame) SetData(p *pager.Page, markDirty bool) error {
	isIntern, err := p.IsInternal()
	if err != nil {
		return err
	}

	// entry metadata
	f.isInternal.Store(isIntern)
	f.isDeleted.Store(false)

	// if f.parentEntry == nil {
	// 	panic("Parent entry not set")
	// }

	if f.parentEntry != nil {
		f.parentEntry.markOccupied()
	}

	if markDirty {
		f.dirty.Store(true)
	}

	f.key = uint32(p.PageId)
	//fmt.Printf("Key after setting -> %d\nPageId: %d\n", f.key, p.PageId)
	// fmt.Printf("SetData() setting data for key: %d  from pager on frame with key:-> %d\n", p.PageId, f.key)

	return nil
}

func (f *Frame) ByteData() (byteData *[pager.PAGE_SIZE_BYTES]byte, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	d, err := f.page.GetPageByteData()

	if err != nil {
		return nil, err
	}

	return d, nil
}

// zeros out the entry and resets all fields
func (f *Frame) Clear() error {
	if f == nil {
		return BufferManagerError{Message: "Frame is not set"}
	}
	callr := ""
	_, file, line, ok := runtime.Caller(1)
	if ok {
		callr = fmt.Sprintf("%s:%d", file, line)
	}
	fmt.Printf("(frame.clear() %s Clearing frame with pid %d\n", callr, f.key)
	f.dirty.Store(false)
	f.key = 0

	// clear page
	f.page.Clear()

	return nil
}

// RawBufferSlice - returns a pointer to a frame's page buffer and its size
func (f *Frame) RawBufferSlice() (buff *[]byte, size uint64, err error) {
	data, err := f.page.GetPageByteData()

	if err != nil {
		return nil, 0, err
	}

	if data == nil {
		return nil, 0, BufferManagerError{Message: "No allocated page for frame."}
	}

	dataSlice := data[:]

	return &dataSlice, uint64(pager.PAGE_SIZE_BYTES), nil
}

// Acquires a latch on a frame.
// returns an error if acquire takes too long (possible deadlock)
//
// if shared is true, The latch is shared else it is exclusive.
func (f *Frame) Acquire(shared bool) error {
	if shared {
		f.mu.RLock()
	} else {
		f.mu.Lock()
	}

	return nil
}

// Releases a latch on a frame.
// returns an error if release takes too long.
//
// Parameters:
//
//	shared - set to true if acquired latch is shared, else set to false.
func (f *Frame) Release(shared bool) error {
	if shared {
		f.mu.RUnlock()
	} else {
		f.mu.Unlock()
	}

	return nil
}

func (f *Frame) TestAddFrameDummyData() {
	f.page.TestAddDummyData()
}

func (f *Frame) Reference() {
	if f.parentEntry == nil {
		panic("Invalid frame, parent entry pointer not set.")
	}

	f.parentEntry.reference()
}

func (f *Frame) Unreference() {
	if f.parentEntry == nil {
		panic(fmt.Sprintf("Invalid frame, parent entry pointer for frame %d not set.", f.getKey()))
	}

	f.parentEntry.unref()
}

// returns the segment type of the parentEntry. Useful during testing.
func (f *Frame) getEntrySegType() SegmentType {
	if f.parentEntry == nil {
		panic("Invalid frame, parent entry pointer not set.")
	}

	return f.parentEntry.getSegType()
}
