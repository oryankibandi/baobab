package buffermanager

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	// max number of times we loop the clock entries to find a suitable candidate
	MAX_LOOP    = 25
	MAX_RETRIES = 5
)

type clock struct {
	// entry that the clock hand points to
	Head *Frame
	// reserved entry for metadata page
	Reserved *Frame

	// max number of items allowed in circular buffer
	capacity uint64
	mu       sync.RWMutex

	// Pool of unassigned cache slots. Freed slots are also added here
	bPool []*Frame
}

// advances the clock hand, finds a valid entry to evict and
// clears the entry. Returns evicted entry and it's  key.
// If no suitable entry is found after MAX_LOOP return nil entry
// and -1 as evictedKey
func (clk *clock) Evict(seg SegmentType) (evicted *Frame, evictedKey int) {
	start := time.Now()
	clk.mu.Lock()
	defer clk.mu.Unlock()

	for i := 0; i < int(clk.capacity)*MAX_LOOP; i++ {
		// check if reserved
		if clk.Head.isReserved() {
			clk.Head = clk.Head.GetNextLink()
			continue
		}

		if clk.Head.accessBitSet() {
			// access bit set, advance clock hand
			clk.Head = clk.Head.GetNextLink()
			continue
		}

		if clk.Head.segType != seg {
			clk.Head = clk.Head.GetNextLink()
			continue
		}

		if clk.Head.refBitSet() {
			// ref bit set, unset it
			clk.Head.unsetRef()

			clk.Head = clk.Head.GetNextLink()
		} else {
			// both access bit and reference bit unset, clear and evict
			eKey := clk.Head.getKey()
			// clk.Head.clear()
			e := clk.Head
			e.Clear()

			// advance clock hand
			clk.Head = clk.Head.GetNextLink()

			end := time.Since(start)
			slog.Info(fmt.Sprintf("Evicted in  %v", end))
			return e, int(eKey)
		}
	}

	end := time.Since(start)
	slog.Info(fmt.Sprintf("Evict failed in %v", end))
	// unable to find suitable entry. All entries referenced
	return nil, -1
}

// advances the clock hand, finds a valid entry.
// loops around the clock buffer MAX_LOOP times to find entries
// that may have not been unpinned in the first run. If no slot is found,
// we release the latch for a period of time to allow other threads to access
// the buffer then retry (exponential backoff). If we reach MAX_RETRIES,
// we simply return nil.
// Returns the entry without clearing the entry, or nil if none found.
func (clk *clock) EvictWithoutClearing(seg SegmentType) (evicted *Frame) {
	for j := range MAX_RETRIES {
		start := time.Now()
		clk.mu.Lock()

		for i := 0; i < int(clk.capacity)*MAX_LOOP; i++ {
			// check if reserved
			if clk.Head.isReserved() {
				clk.Head = clk.Head.GetNextLink()
				continue
			}

			if clk.Head.accessBitSet() {
				// access bit set, advance clock hand
				clk.Head = clk.Head.GetNextLink()
				continue
			}

			if clk.Head.segType != seg {
				clk.Head = clk.Head.GetNextLink()
				continue
			}

			if clk.Head.refBitSet() {
				// ref bit set, unset it
				clk.Head.unsetRef()

				clk.Head = clk.Head.GetNextLink()
				continue
			} else {
				// both access bit and reference bit unset
				e := clk.Head

				// advance clock hand
				clk.Head = clk.Head.GetNextLink()

				end := time.Since(start)
				slog.Info(fmt.Sprintf("Evicted page %d in %v", e.getKey(), end))
				clk.mu.Unlock()
				return e
			}
		}

		clk.mu.Unlock()
		end := time.Since(start)
		slog.Info(fmt.Sprintf("Evict failed in %v, retrying..", end))
		time.Sleep(time.Second * time.Duration(j+1))
	}
	// unable to find suitable entry. All entries referenced
	return nil
}

// Retrieve an available entry. If no entry is available return nil.
func (clk *clock) Pop() *Frame {
	clk.mu.Lock()
	defer clk.mu.Unlock()

	if len(clk.bPool) == 0 {
		fmt.Println("No item in buffer pool")
		return nil
	}

	e := clk.bPool[len(clk.bPool)-1]
	clk.bPool = append(clk.bPool[:len(clk.bPool)-1], []*Frame{}...)

	return e
}

// Clears entry and adds it back to the pool.
// Returns error if any.
func (clk *clock) addToBpool(f *Frame) error {
	if f == nil {
		return BufferManagerError{Message: "Received nil frame to add to pool"}
	}

	if f.GetNextLink() == nil || f.GetPrevLink() == nil {
		return BufferManagerError{Message: "Invalid frame"}
	}

	fmt.Println("(addtobpool) getting latch...")
	clk.mu.Lock()
	fmt.Println("(addtobpool) obtained latch...")
	defer clk.mu.Unlock()

	err := f.Clear()
	if err != nil {
		return err
	}

	clk.bPool = append(clk.bPool, f)

	return nil
}

// Returns a reference to the frame at the current clock head. clock head changes
// whenever clock hand progresses
func (clk *clock) clockHead() *Frame {
	clk.mu.RLock()
	defer clk.mu.RUnlock()

	return clk.Head
}

// Returns a reference to the frame at the current clock head. clock head changes
// whenever clock hand progresses
func (clk *clock) getReserved() *Frame {
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

	var f *Frame

	for range clk.capacity {
		f = clk.Head
		clk.Head = f.next

		err := f.Clear()
		if err != nil {
			return err
		}

		err = FreeFrame(f)
		if err != nil {
			return err
		}
	}

	return nil
}

// Creates a clock buffer of size 'size`KB and
// returns a pointer to the new circular buffer.
// `itemCount` parameter represents number of entries/frames
func NewClock(itemCount uint64) (*clock, error) {
	minItems := 3
	// Initialize entries, add to bPool and create the circular buffer
	if itemCount < uint64(minItems) {
		return nil, BufferManagerError{Message: fmt.Sprintf("Minimum capacity is %d", minItems)}
	}

	// capacity := (size * 1024) / pager.PAGE_SIZE_BYTES
	clk := &clock{
		capacity: itemCount,
	}

	var wg sync.WaitGroup
	for range itemCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := NewFrame()
			if e == nil {
				panic("Unable to create entry")
			}

			clk.mu.Lock()
			clk.bPool = append(clk.bPool, e)
			clk.mu.Unlock()
		}()
	}
	wg.Wait()

	for i, ent := range clk.bPool {
		if i == 0 {
			// first item
			ent.SetNextLink(clk.bPool[i+1])

			ent.SetPrevLink(clk.bPool[itemCount-1])
		} else if i == int(itemCount)-1 {
			// last item
			ent.SetPrevLink(clk.bPool[i-1])

			ent.SetNextLink(clk.bPool[0])
		} else {
			ent.SetNextLink(clk.bPool[i+1])

			ent.SetPrevLink(clk.bPool[i-1])
		}
	}

	// set head at first item
	clk.Head = clk.bPool[0]

	// reserve one frame
	f := clk.Pop()
	if f == nil {
		return nil, BufferManagerError{"Could not reserve metadata page frame"}
	}
	clk.Reserved = f
	f.reserveFrame()

	// ensure clock hand doesn't point to reserved frame
	if f == clk.Head {
		clk.Head = clk.Head.GetNextLink()
	}

	return clk, nil
}
