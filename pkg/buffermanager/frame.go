package buffermanager

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	diskmanager "github.com/oryankibandi/baobab/pkg/diskmanager"
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
	page       diskmanager.Page // 8240 bytes
	lsn        [diskmanager.LSN_SIZE_BYTE]byte
	isInternal atomic.Bool
	isDeleted  atomic.Bool
	isDirty    atomic.Bool

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

	if diff < 0 {
		panic("Invalid pin count")
	}

	return diff
}

func (c *counter) reset() {
	c.pinCount.Store(0)
	c.unpinCount.Store(0)
}

// Sets the access bit and ref bit of an entry. Called when accessing an entry.
// The process that uses the entry data is required to call Unreference() when done
func (e *Frame) Reference() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ref.Store(true)
	e.acc.Store(true)

	// increment pin count
	e.counters.addPinCount()
}

func (e *Frame) SetNextLink(n *Frame) {
	if n == nil {
		panic("invalid nil entry provided.")
	}

	e.next = n
}

func (e *Frame) SetPrevLink(p *Frame) {
	if p == nil {
		panic("invalid nil entry provided.")
	}

	e.prev = p
}

func (e *Frame) GetNextLink() *Frame {
	return e.next
}

func (e *Frame) GetPrevLink() *Frame {
	return e.prev
}

// unreferences an entry. Reduces pin count and if no pins left, unsets access bit.
func (e *Frame) Unreference() {
	e.counters.addUnpinCount()

	e.mu.Lock()
	// check current count
	p := e.counters.getTotalPins()

	// If no pins, unset access bit
	if p == 0 {
		// unset access pin
		e.acc.Store(false)
	}

	e.mu.Unlock()
}

// Returns true if access bit is set, else false
func (e *Frame) accessBitSet() bool {
	return e.acc.Load()
}

// Returns true if access bit is set, else false
func (e *Frame) refBitSet() bool {
	return e.ref.Load()
}

// Mark an entry/frame as dirty
func (e *Frame) MarkDirty() {
	e.isDirty.Store(true)
}

// Mark an entry/frame as clean
func (e *Frame) MarkClean() {
	e.isDirty.Store(false)
}

// Unsets the reference bit. This is exclusively called by the clock replacement algorithm.
func (e *Frame) unsetRef() {
	e.ref.Store(false)
}

func (e *Frame) getKey() uint32 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.meta.key
}

func (e *Frame) getSegType() SegmentType {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.segType
}

func (e *Frame) GetPage() *diskmanager.Page {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return &e.page
}

// sets data on a frame/entry from a page
func (e *Frame) SetData(p *diskmanager.Page) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	isIntern, err := p.IsInternal()
	if err != nil {
		return err
	}

	// entry metadata
	e.isInternal.Store(isIntern)
	e.isDeleted.Store(false)
	e.isDirty.Store(true)
	e.isOccupied.Store(true)

	pgelsn := p.GetLSN()
	copy(e.lsn[:], pgelsn[:])

	// entry clock metadata
	e.ref.Store(false)
	e.acc.Store(false)

	e.meta.key = uint32(p.PageId)

	// page data
	e.page = diskmanager.Page{}
	pData, err := p.GetPageByteData()
	if err != nil {
		return err
	}

	err = e.page.SetPageData(pData)
	if err != nil {
		return err
	}

	// pageId & Flags
	e.page.PageId = p.PageId
	e.page.Flags = p.Flags

	// clear page so that it can be garbage collected
	p = nil

	return nil
}

func (e *Frame) ByteData() (byteData *[diskmanager.PAGE_SIZE_BYTES]byte, err error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	d, err := e.page.GetPageByteData()

	if err != nil {
		return nil, err
	}

	return d, nil
}

// zeros out the entry and resets all fields
func (e *Frame) Clear() error {
	if e == nil {
		return BufferManagerError{Message: "Frame is not set"}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.ref.Store(false)
	e.acc.Store(false)

	e.isDirty.Store(false)
	e.meta.key = 0

	e.counters.reset()

	// mark as unallocated
	e.isOccupied.Store(false)

	// clear page
	e.page.Clear()

	return nil
}

func (e *Frame) updateSegment(seg SegmentType) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.segType != seg {
		e.segType = seg
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
