package buffermanager

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	"github.com/oryankibandi/baobab/pkg/helpers"
)

func TestNewClock(t *testing.T) {
	var expectedCapacity uint64 = 100
	var size uint64 = uint64(float64(expectedCapacity) * float64(unsafe.Sizeof(Frame{})))
	var allocatedRSS uint64

	initialRss, err := manual.CurrentRSSBytes()

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}
	helpers.PrintInfoMsg(fmt.Sprintf("Initial RSS: %dKB", initialRss/1024))

	t.Run("NewClock_low_size", func(t *testing.T) {
		_, err := NewClock(2, true)

		if err == nil {
			helpers.PrintTestErrorMsg("Expected function to throw err, got nil", t)
		}

		helpers.PrintSuccessMsg("NewClock_low_size success")
	})

	t.Run("NewClock", func(t *testing.T) {
		c, err := NewClock(expectedCapacity, true)

		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}

		if c == nil {
			helpers.PrintTestErrorMsg("Expected new clock, got nil", t)
		}

		t.Run("new_clock_capacity", func(t *testing.T) {
			cCap := c.getCap()

			if cCap != expectedCapacity {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected capacity to be %d, got %d", expectedCapacity, cCap), t)
			}
			helpers.PrintSuccessMsg("new_clock_capacity success")
		})

		t.Run("new_clock_allocated_size", func(t *testing.T) {
			allocatedRSS, err = manual.CurrentRSSBytes()

			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
			}
			helpers.PrintInfoMsg(fmt.Sprintf("Allocated RSS: %dKB", allocatedRSS/1024))

			// Ideally, Resident Set Size(RSS) should be greater than the memory
			// allocated. Due to lazy allocation, memory may not be physically allocated until accessed
			// hence RSS may be lower.
			if (allocatedRSS/1024)-(initialRss/1024) < size {
				helpers.PrintInfoMsg(fmt.Sprintf("Expected RSS above %d KB, but got %d KB", size, (allocatedRSS/1024)-(initialRss/1024)))
			}

			helpers.PrintSuccessMsg("new_clock_allocated_size success")
		})

		t.Run("new_clock_close", func(t *testing.T) {
			err = c.close()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
			}

			currRss, err := manual.CurrentRSSBytes()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
			}

			// Resident Set Size should be greater than the memory
			// allocated
			if (currRss/1024) > allocatedRSS && !ASANEnabled {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected RSS less than %d KB, but got %d KB", allocatedRSS, (currRss/1024)), t)
			}

			helpers.PrintSuccessMsg("new_clock_close success")
		})

		helpers.PrintSuccessMsg("NewClock success")
	})
}

func TestEvictConcurrent(t *testing.T) {
	var clkEntry *clockentry
	var wg sync.WaitGroup
	var size uint64 = 100
	var unreferencedFrames uint64 = 2

	expectedFrameCount := size - 1

	c, err := NewClock(size, true)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if c == nil {
		t.Fatalf("Expected new clock, got nil")
	}

	defer c.close()

	// add data
	for i := range expectedFrameCount {
		clkEntry = c.EvictWithoutClearing(unassigned)

		if clkEntry == nil {
			helpers.PrintTestErrorMsg("Expected frame, got nil", t)
		}

		clkEntry.markOccupied()
		clkEntry.updateSegment(windowSegment)
		if i >= expectedFrameCount-unreferencedFrames {
			clkEntry.unMarkForEviction()
			clkEntry.unref()
		}
	}

	// find eviction candidate
	start := make(chan struct{})
	for i := range unreferencedFrames {
		wg.Add(1)
		go func(j uint64) {
			defer wg.Done()
			<-start
			t.Run(fmt.Sprintf("%d_evict_concurrent", i), func(t *testing.T) {
				e := c.EvictWithoutClearing(windowSegment)

				if e == nil {
					helpers.PrintTestErrorMsg("Expected evicted entry, got nil", t)
				}

				if !e.acc {
					helpers.PrintTestErrorMsg("Expected acces bit to be set.", t)
				}

				if !e.markedForEviction {
					helpers.PrintTestErrorMsg("Expected frame to be marked for eviction.", t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%d_evict_concurrent success", i))
			})

		}(i)
	}
	close(start)
	wg.Wait()
}

func TestEvictWithoutClearingConcurrent(t *testing.T) {
	var clkEntry *clockentry
	var wg sync.WaitGroup
	var size uint64 = 1000
	var unreferencedFrames uint64 = 2
	expectedFrameCount := size - 1

	c, err := NewClock(size, true)

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if c == nil {
		helpers.PrintTestErrorMsg("Expected new clock, got nil", t)
	}

	defer c.close()

	// add data
	for i := range expectedFrameCount {
		clkEntry = c.EvictWithoutClearing(unassigned)

		if clkEntry == nil {
			helpers.PrintTestErrorMsg("Expected frame, got nil", t)
		}

		clkEntry.markOccupied()
		clkEntry.updateSegment(windowSegment)
		if i >= expectedFrameCount-unreferencedFrames {
			clkEntry.unMarkForEviction()
			clkEntry.unref()
		}
	}

	// find eviction candidate
	start := make(chan struct{})
	for i := range unreferencedFrames {
		wg.Add(1)
		go func(j uint64) {
			defer wg.Done()
			<-start
			t.Run(fmt.Sprintf("%d_evictwithoutclearing_concurrent", i), func(t *testing.T) {
				e := c.EvictWithoutClearing(windowSegment)
				if e == nil {
					helpers.PrintTestErrorMsg("Expected evicted entry, got nil", t) // FIX: Fix this error.
				}

				if !e.acc {
					helpers.PrintTestErrorMsg("Expected acces bit to be set.", t)
				}

				if !e.markedForEviction {
					helpers.PrintTestErrorMsg("Expected frame to be marked for eviction.", t)
				}

				if !e.isOccupied {
					helpers.PrintTestErrorMsg("Expected frame  not be cleared", t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%d_evictwithoutclearing_concurrent success", i))
			})

		}(i)
	}
	close(start)
	wg.Wait()
}
