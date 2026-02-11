package buffermanager

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	// max number of times we loop the clock entries to find a suitable candidate
	MAX_LOOP = 25
)

type clock struct {
	// entry that the clock hand points to
	Head *Entry

	capacity uint64

	// windowsegment, probationSegment or protectedSegment
	segment SegmentType
	mu      sync.RWMutex

	bPool []*Entry
}

// advances the clock hand, finds a valid entry to evict and
// clears the entry. Returns evicted entry and it's  key.
// If no suitable entry is found after MAX_LOOP return nil entry
// and -1 as evictedKey
func (clk *clock) Evict() (evicted *Entry, evictedKey int) {
	start := time.Now()
	clk.mu.Lock()
	defer clk.mu.Unlock()

	for i := 0; i < int(clk.capacity)*MAX_LOOP; i++ {
		if clk.Head.accessBitSet() {
			// access bit set, advance clock hand
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

// Returns a pointer to a new circular buffer
func NewClock(capacity uint64, segType SegmentType) (*clock, error) {
	// Initialize entries, add to bPool and create the circular buffer
	if capacity < 3 {
		return nil, BufferManagerError{Message: "Minimum capacity is 3"}
	}

	clk := &clock{
		capacity: capacity,
		segment:  segType,
	}

	var wg sync.WaitGroup
	for i := uint64(0); i < capacity; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := NewEntry()

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

			ent.SetPrevLink(clk.bPool[capacity-1])
		} else if i == int(capacity)-1 {
			// last item
			ent.SetPrevLink(clk.bPool[i-1])

			ent.SetNextLink(clk.bPool[0])
		} else {
			ent.SetNextLink(clk.bPool[i+1])

			ent.SetPrevLink(clk.bPool[i-1])
		}
	}

	clk.Head = clk.bPool[capacity-1]

	return clk, nil
}
