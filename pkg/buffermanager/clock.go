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
	Head *Frame

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
// Returns the entry without clearing the entry.
// If no suitable entry is found after MAX_LOOP return nil entry
func (clk *clock) EvictWithoutClearing(seg SegmentType) (evicted *Frame) {
	start := time.Now()
	clk.mu.Lock()
	defer clk.mu.Unlock()

	for i := 0; i < int(clk.capacity)*MAX_LOOP; i++ {
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
			// both access bit and reference bit unset
			e := clk.Head

			// advance clock hand
			clk.Head = clk.Head.GetNextLink()

			end := time.Since(start)
			slog.Info(fmt.Sprintf("Evicted in  %v", end))
			return e
		}
	}

	end := time.Since(start)
	slog.Info(fmt.Sprintf("Evict failed in %v", end))
	// unable to find suitable entry. All entries referenced
	return nil
}

// Retrieve an available entry. If no entry is available return nil.
func (clk *clock) Pop() *Frame {
	clk.mu.Lock()
	defer clk.mu.Unlock()

	if len(clk.bPool) == 0 {
		return nil
	}

	e := clk.bPool[len(clk.bPool)-1]
	clk.bPool = append(clk.bPool[:len(clk.bPool)-1], []*Frame{}...)

	return e
}

// Clears entry and adds it back to the pool.
func (clk *clock) addToBpool(e *Frame) error {
	if e == nil {
		return BufferManagerError{Message: "Received nil entry to add to pool"}
	}

	clk.mu.Lock()
	defer clk.mu.Unlock()

	err := e.Clear()
	if err != nil {
		return err
	}

	clk.bPool = append(clk.bPool, e)

	return nil
}

// Returns a reference to the frame at the current clock head
func (clk *clock) clockHead() *Frame {
	clk.mu.RLock()
	defer clk.mu.RUnlock()

	return clk.Head
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

		err := FreeFrame(f)
		if err != nil {
			return err
		}
	}

	return nil
}

// Returns a pointer to a new circular buffer
func NewClock(capacity uint64) (*clock, error) {
	// Initialize entries, add to bPool and create the circular buffer
	if capacity < 3 {
		return nil, BufferManagerError{Message: "Minimum capacity is 3"}
	}

	clk := &clock{
		capacity: capacity,
	}

	var wg sync.WaitGroup
	for i := uint64(0); i < capacity; i++ {
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
