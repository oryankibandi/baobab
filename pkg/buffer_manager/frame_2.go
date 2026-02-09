package buffermanager

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	diskio "github.com/oryankibandi/baobab/pkg/disk_io"
)

type metadata struct {
	key uint32
}

type counter struct {
	pinCount   atomic.Uint64
	unpinCount atomic.Uint64
}

type Entry struct { //alias: Frame
	isInternal atomic.Bool
	isDeleted  atomic.Bool
	isDirty    atomic.Bool
	lsn        [LSN_SIZE]byte

	// reference bit. Set when an item is accessed and unset by clock hand when
	// looking for an item to evict
	ref atomic.Bool

	// access bit. set when an entry is accessed(pinned) and unset during unpinning
	// When this item is set the reference bit cannot be unset. The clock hand will
	// advance past an entry with it's access bit set
	acc atomic.Bool

	// Prev and Next links. Remain constant after initialization
	prev *Entry
	next *Entry

	// if its allocated
	isOccupied atomic.Bool
	counters   counter
	//  metadata
	meta metadata

	// 8K page. Mamory initialized manually
	page diskio.Page

	// Pointer to menually allocated memory address used to free memory.
	// Remains constant after initialization.
	CPtr unsafe.Pointer

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
func (e *Entry) Reference() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ref.Store(true)
	e.acc.Store(true)

	// increment pin count
	e.counters.addPinCount()
}

func (e *Entry) SetNextLink(n *Entry) {
	if n == nil {
		panic("invalid nil entry provided.")
	}

	e.next = n
}

func (e *Entry) SetPrevLink(p *Entry) {
	if p == nil {
		panic("invalid nil entry provided.")
	}

	e.prev = p
}

func (e *Entry) GetNextLink() *Entry {
	return e.next
}

func (e *Entry) GetPrevLink() *Entry {
	return e.prev
}

// unreferences an entry. Reduces pin count and if no pins left, unsets access bit.
func (e *Entry) Unreference() {
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
func (e *Entry) accessBitSet() bool {
	return e.acc.Load()
}

// Returns true if access bit is set, else false
func (e *Entry) refBitSet() bool {
	return e.ref.Load()
}

// Mark an entry/frame as dirty
func (e *Entry) MarkDirty() {
	e.isDirty.Store(true)
}

// Mark an entry/frame as clean
func (e *Entry) MarkClean() {
	e.isDirty.Store(false)
}

// Unsets the reference bit. This is exclusively called by the clock replacement algorithm.
func (e *Entry) unsetRef() {
	e.ref.Store(false)
}

func (e *Entry) getKey() uint32 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.meta.key
}

func (e *Entry) GetPage() *diskio.Page {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return &e.page
}

// sets data on a fram from a page entry
func (e *Entry) SetData(p *diskio.Page) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	isIntern, err := p.IsInternal()

	if err != nil {
		return err
	}

	e.isInternal.Store(isIntern)

	e.isDeleted.Store(false)
	e.isDirty.Store(true)
	copy(e.lsn[:], p.GetLSN())

	e.ref.Store(false)
	e.acc.Store(false)

	e.isOccupied.Store(true)

	e.meta.key = uint32(p.Header.PageId)

	e.page = diskio.Page{
		Header: diskio.PageHeader{},
	}

	// copy header details
	e.page.Header.Flags = p.Header.Flags
	e.page.Header.PageId = p.Header.PageId
	e.page.Header.Items = p.Header.Items
	e.page.Header.FreeSpace = p.Header.FreeSpace
	e.page.Header.UpperOffset = p.Header.UpperOffset
	e.page.Header.LowerOffset = p.Header.LowerOffset
	e.page.Header.MagicNumber = p.Header.MagicNumber
	e.page.Header.Checksum = p.Header.Checksum
	e.page.Header.RightChild = p.Header.RightChild
	e.page.Header.RightSibling = p.Header.RightSibling
	e.page.Header.LeftSibling = p.Header.LeftSibling
	copy(e.page.Header.LSN, p.Header.LSN)

	// copy page data
	pData, err := p.GetPageByteData()

	if err != nil {
		return err
	}

	err = e.page.SetPageData(pData)

	if err != nil {
		return err
	}

	// assigning e.page only copies normal primitives, for strings
	// and slices only a pointer to their headers are copied.
	e.page.GetLSN()

	return nil

}

// func (e *Entry) GetData() [ENTRY_SIZE]byte {
// 	var d [ENTRY_SIZE]byte
//
// 	e.mu.RLock()
// 	defer e.mu.RUnlock()
//
// 	copy(d[:], e.Data[:])
//
// 	return d
// }

// zeros out the entry and resets all fields
func (e *Entry) Clear() error {
	if e == nil {
		return BufferManagerError{Message: "Entry is not set"}
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

// Returns a pointer to new entry
// To reduce pressure on the GC and improve performance, memory is allocated manually via calloc().
// This memory also needs to be freed after use to avoid memory leaks.
// In a storage engine's buffer manager, this memory will be initialized at startup and reused as blocks are paged-in and evicted.
func NewEntry() *Entry {
	p := manual.Alloc(unsafe.Sizeof(Entry{}))

	e := (*Entry)(p)

	e.CPtr = p

	return e
}

// Calls free() on manually allocated memory
func FreeEntry(e *Entry) error {
	if e == nil {
		return BufferManagerError{Message: "Null entry provided"}
	}

	if e.CPtr == nil {
		return BufferManagerError{Message: "No pointer to allocated heap memory."}
	}

	manual.FreeMem(e.CPtr)

	return nil
}
