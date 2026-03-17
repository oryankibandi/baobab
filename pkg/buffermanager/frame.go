package buffermanager

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	"github.com/oryankibandi/baobab/pkg/pager"
)

type metadata struct {
	key uint32
}

type counter struct {
	pinCount   atomic.Uint64
	unpinCount atomic.Uint64
}

type Frame struct {
	// 8K page. Memory initialized manually
	// x64 -> 8220 bytes, x86 8208 bytes
	page       pager.Page // 4 byte padding added in x64 systems
	lsn        [pager.LSN_SIZE_BYTE]byte
	isInternal atomic.Bool
	isDeleted  atomic.Bool
	dirty      atomic.Bool

	// reference bit. Set when an item is accessed and unset by clock hand when
	// looking for an item to evict
	ref atomic.Bool

	// access bit. set when an entry is accessed(pinned) and unset during unpinning
	// When this item is set the reference bit cannot be unset. The clock hand will
	// advance past an entry with it's access bit set
	acc atomic.Bool

	// Prev and Next links. Remain constant after initialization
	prev *Frame
	next *Frame

	counters counter
	// if its allocated
	isOccupied atomic.Bool

	//  metadata
	meta metadata

	// Pointer to manually allocated memory address used to free memory.
	// Remains constant after initialization.
	CPtr unsafe.Pointer

	// segment type
	segType SegmentType

	// Mutex field. In 32 bit systems it is 12 bytes in size
	// hence will add a padding and should be ordered as the last
	// item to make byte positioning predictable.
	mu sync.RWMutex
}

func (c *counter) addPinCount() {
	c.pinCount.Add(1)
}

func (c *counter) addUnpinCount() {
	c.unpinCount.Add(1)
}

func (c *counter) getTotalPins() uint64 {
	diff := c.pinCount.Load() - c.unpinCount.Load()

	return diff
}

func (c *counter) reset() {
	c.pinCount.Store(0)
	c.unpinCount.Store(0)
}

func (f *Frame) isPinned() bool {
	return f.acc.Load()
}

func (f *Frame) isDirty() bool {
	return f.dirty.Load()
}

// Sets the access bit and ref bit of an entry. Called when accessing an entry.
// The process that uses the entry data is required to call Unreference() when done
func (f *Frame) Reference() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ref.Store(true)
	f.acc.Store(true)

	// increment pin count
	f.counters.addPinCount()
}

func (f *Frame) SetNextLink(n *Frame) {
	if n == nil {
		panic("invalid nil entry provided.")
	}

	f.next = n
}

func (f *Frame) SetPrevLink(p *Frame) {
	if p == nil {
		panic("invalid nil entry provided.")
	}

	f.prev = p
}

func (f *Frame) GetNextLink() *Frame {
	return f.next
}

func (f *Frame) GetPrevLink() *Frame {
	return f.prev
}

// unreferences an entry. Reduces pin count and if no pins left, unsets access bit.
func (f *Frame) Unreference() {
	f.counters.addUnpinCount()

	f.mu.Lock()
	// check current count
	p := f.counters.getTotalPins()

	// If no pins, unset access bit
	if p == 0 {
		// unset access pin
		f.acc.Store(false)
	}

	f.mu.Unlock()
}

// Returns true if access bit is set, else false
func (f *Frame) accessBitSet() bool {
	return f.acc.Load()
}

// Returns true if access bit is set, else false
func (f *Frame) refBitSet() bool {
	return f.ref.Load()
}

// Mark an entry/frame as dirty
func (f *Frame) MarkDirty() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dirty.Store(true)
	f.page.MarkDirty()
}

// Mark an entry/frame as clean
func (f *Frame) MarkClean() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dirty.Store(false)
	f.page.MarkClean()
}

func (f *Frame) MarkOccupied() {
	f.isOccupied.Store(true)
}

// Unsets the reference bit. This is exclusively called by the clock replacement algorithm.
func (f *Frame) unsetRef() {
	f.ref.Store(false)
}

func (f *Frame) getKey() uint32 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.meta.key
}

func (f *Frame) getSegType() SegmentType {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.segType
}

func (f *Frame) GetPage() *diskmanager.Page {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return &f.page
}

// sets data on a frame/entry from a page
func (f *Frame) SetData(p *diskmanager.Page) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	isIntern, err := p.IsInternal()
	if err != nil {
		return err
	}

	// entry metadata
	f.isInternal.Store(isIntern)
	f.isDeleted.Store(false)
	f.dirty.Store(true)
	f.isOccupied.Store(true)

	pgelsn := p.GetLSN()
	copy(f.lsn[:], pgelsn[:])

	// entry clock metadata
	f.ref.Store(false)
	f.acc.Store(false)

	f.meta.key = uint32(p.PageId)

	// page data
	f.page = diskmanager.Page{}
	pData, err := p.GetPageByteData()
	if err != nil {
		return err
	}

	err = f.page.SetPageData(pData)
	if err != nil {
		return err
	}

	// pageId & Flags
	f.page.PageId = p.PageId
	f.page.Flags = p.Flags

	// clear page so that it can be garbage collected
	p = nil

	return nil
}

func (f *Frame) ByteData() (byteData *[diskmanager.PAGE_SIZE_BYTES]byte, err error) {
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

	f.mu.Lock()
	defer f.mu.Unlock()

	f.ref.Store(false)
	f.acc.Store(false)

	f.dirty.Store(false)
	f.meta.key = 0

	f.counters.reset()

	// mark as unallocated
	f.isOccupied.Store(false)
	f.segType = unassigned

	// clear page
	f.page.Clear()

	return nil
}

func (f *Frame) updateSegment(seg SegmentType) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.segType != seg {
		f.segType = seg
	}
}

// Returns a pointer to new entry
// To reduce pressure on the GC and improve performance, memory is allocated manually via calloc().
// This memory also needs to be freed after use to avoid memory leaks.
// In a storage engine's buffer manager, this memory will be initialized at startup and reused as blocks are paged-in and evicted.
func NewFrame() *Frame {
	p := manual.Alloc(unsafe.Sizeof(Frame{}))
	e := (*Frame)(p)
	e.CPtr = p

	return e
}

// Calls free() on manually allocated memory
func FreeFrame(e *Frame) error {
	if e == nil {
		return BufferManagerError{Message: "Null entry provided"}
	}

	if e.CPtr == nil {
		return BufferManagerError{Message: "No pointer to allocated heap memory."}
	}

	manual.FreeMem(e.CPtr)

	return nil
}
