package buffermanager

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
)

const (
	// max number of times we loop the clock entries to find a suitable candidate
	// This gives us a chance to find slots that were given a second chance
	// by the clock sweep algorithm
	MAX_LOOP    = 2
	MAX_RETRIES = 5
)

type clockentry struct {
	entry Frame // 8264 bytes
	// reference bit. Set when an item is accessed and unset by clock hand when
	// looking for an item to evict
	ref bool
	// access bit. set when an entry is accessed(pinned) and unset during unpinning
	// When the acc bit is set the reference bit cannot be unset.
	// The clock hand will advance past an entry with it's access bit set
	acc bool
	// true if this frame is reserved for something like the metadata page
	// This ensures the clock hand passes over this frame when looking for
	// an eviction candidate
	reserved bool

	// if its allocated
	isOccupied bool

	// counters
	pinCount   atomic.Uint64
	unpinCount atomic.Uint64

	segtype SegmentType

	mu sync.RWMutex
}

type clock struct {
	// index clock hand points to
	Head int
	// reserved entry for metadata page
	Reserved *clockentry

	// max number of items allowed in circular buffer
	capacity uint64
	mu       sync.RWMutex

	// Pool of unassigned cache slots. Freed slots are also added here
	bPool []clockentry
}

// advances the clock hand, finds a valid entry.
// loops around the clock buffer MAX_LOOP times to find entries
// that may have not been unpinned in the first run. If no slot is found,
// we wait for a period of time to allow other goroutines to free some frames
// then retry (exponential backoff). If we reach MAX_RETRIES,
// we simply return nil.
// Returns the entry without clearing the entry, or nil if none found.
// Each returned candidate is referenced to prevent eviction by a different
// goroutine.
func (clk *clock) EvictWithoutClearing(seg SegmentType) (evicted *clockentry) {
	for j := range MAX_RETRIES {
		start := time.Now()
		clk.mu.Lock()
		clkCap := clk.capacity

		for i := 0; i < int(clkCap)*MAX_LOOP; i++ {
			// check if reserved
			if clk.bPool[clk.Head].reserved {
				clk.Head = (clk.Head + 1) % int(clkCap)
				continue
			}

			if clk.bPool[clk.Head].acc {
				// access bit set, advance clock hand
				clk.Head = (clk.Head + 1) % int(clkCap)
				continue
			}

			if s := clk.bPool[clk.Head].segtype; s != seg {
				clk.Head = (clk.Head + 1) % int(clkCap)
				continue
			}

			if clk.bPool[clk.Head].ref {
				// ref bit set, unset it
				clk.bPool[clk.Head].ref = false

				clk.Head = (clk.Head + 1) % int(clkCap)
				continue
			} else {
				// both access bit and reference bit unset
				e := &clk.bPool[clk.Head]
				// reference frame so it doesn't get evicted/accese d by other goroutines i.e. bgwriter
				e.ref = true

				// advance clock hand
				clk.Head = (clk.Head + 1) % int(clkCap)

				clk.mu.Unlock()
				// end := time.Since(start)
				// slog.Info(fmt.Sprintf("(%v) Evicted in %v in %d cycle(s).", seg, end, (i/int(clkCap))+1))
				// fmt.Printf("Evicted %d\n", e.entry.getKey())
				return e
			}
		}

		clk.mu.Unlock()
		end := time.Since(start)
		sleepDur := time.Millisecond * time.Duration((j+1)*10)
		slog.Info(fmt.Sprintf("(%v) Evict failed in %v, retrying in %v..", seg, end, sleepDur))
		time.Sleep(sleepDur)
	}
	// unable to find suitable entry. All entries referenced
	return nil
}

// resets an entry after is has been evicted so it's ready
// for the next user.
func (clk *clock) clearEntry(e *clockentry) error {
	if e == nil {
		return BufferManagerError{Message: "clearentry: No entry provided"}
	}

	err := e.entry.Clear()
	if err != nil {
		return err
	}

	e.acc = false
	e.ref = false
	e.isOccupied = false

	e.pinCount.Store(0)
	e.unpinCount.Store(0)

	e.segtype = unassigned

	return nil
}

// Returns a reference to the frame at the current clock head. clock head changes
// whenever clock hand progresses
func (clk *clock) clockHead() *clockentry {
	clk.mu.RLock()
	defer clk.mu.RUnlock()

	return &clk.bPool[clk.Head]
}

// Returns a reference to the frame at the current clock head. clock head changes
// whenever clock hand progresses
func (clk *clock) getReserved() *clockentry {
	clk.mu.RLock()
	defer clk.mu.RUnlock()

	return clk.Reserved
}

func (clk *clock) getCap() uint64 {
	clk.mu.RLock()
	defer clk.mu.RUnlock()
	return clk.capacity
}

// frees all buffer memory
func (clk *clock) close() error {
	clk.mu.Lock()
	defer clk.mu.Unlock()

	manual.FreeMem(unsafe.Pointer(&clk.bPool[0]))

	return nil
}

func (cEntry *clockentry) markOccupied() {
	cEntry.mu.Lock()
	cEntry.isOccupied = true
	cEntry.mu.Unlock()
}

func (cEntry *clockentry) markVacant() {
	cEntry.isOccupied = false
}

func (cEntry *clockentry) updateSegment(seg SegmentType) {
	cEntry.mu.Lock()
	cEntry.segtype = seg
	cEntry.mu.Unlock()
}

func (cEntry *clockentry) reference() {
	cEntry.mu.Lock()
	defer cEntry.mu.Unlock()
	cEntry.acc = true
	cEntry.ref = true

	cEntry.pinCount.Add(1)
}

func (cEntry *clockentry) unref() {
	pinCount := cEntry.pinCount.Load()
	unpinCount := cEntry.unpinCount.Load()

	if pinCount == unpinCount {
		return
	}

	cEntry.unpinCount.Add(1)

	if pinCount-unpinCount == 1 {
		cEntry.mu.Lock()
		cEntry.acc = false
		cEntry.mu.Unlock()
	}
}

func (cEntry *clockentry) isReferenced() bool {
	cEntry.mu.RLock()
	defer cEntry.mu.RUnlock()

	return cEntry.acc
}

func (cEntry *clockentry) getSegType() SegmentType {
	cEntry.mu.RLock()
	defer cEntry.mu.RUnlock()
	return cEntry.segtype
}

// Creates a clock buffer of size 'size`KB and
// returns a pointer to the new circular buffer.
// `itemCount` parameter represents number of entries/frames
// `reserve` - when set to true, an entry for metadata page is reserved
func NewClock(itemCount uint64, reserve bool) (*clock, error) {
	minItems := 3
	// Initialize entries, add to bPool and create the circular buffer
	if itemCount < uint64(minItems) {
		return nil, BufferManagerError{Message: fmt.Sprintf("Minimum capacity is %d", minItems)}
	}

	// capacity := (size * 1024) / pager.PAGE_SIZE_BYTES
	clk := &clock{
		capacity: itemCount,
	}

	// allocate buffer space
	p := manual.Alloc(uintptr(itemCount) * unsafe.Sizeof(clockentry{}))
	firstItem := (*clockentry)(p)
	clk.bPool = unsafe.Slice(firstItem, itemCount)

	// set head at first item
	clk.Head = 0

	// reserve one entry
	if reserve {
		e := clk.EvictWithoutClearing(unassigned)
		if e == nil {
			return nil, BufferManagerError{"Could not reserve metadata page frame"}
		}
		clk.Reserved = e
		e.reserved = true
		e.entry.parentEntry = e
	}

	return clk, nil
}
