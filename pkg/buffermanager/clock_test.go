package buffermanager

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/oryankibandi/baobab/internal/manual"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/pager"
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
		_, err := NewClock(2)

		if err == nil {
			helpers.PrintTestErrorMsg("Expected function to throw err, got nil", t)
		}

		helpers.PrintSuccessMsg("NewClock_low_size success")
	})

	t.Run("NewClock", func(t *testing.T) {
		c, err := NewClock(expectedCapacity)

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

		t.Run("new_clock_linked_circular_buffer", func(t *testing.T) {
			currFr := c.Head
			for range expectedCapacity {
				if currFr.GetPrevLink() == nil {
					helpers.PrintTestErrorMsg("Frame has no previous link in circular buffer,", t)
				}

				if currFr.GetNextLink() == nil {
					helpers.PrintTestErrorMsg("Frame has no next link in circular buffer", t)
				}
			}

			helpers.PrintSuccessMsg("new_clock_linked_circular_buffer success")
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

func TestPop(t *testing.T) {
	var fr *Frame
	var testPageData [pager.PAGE_SIZE_BYTES]byte
	var size uint64 = 100

	expectedFrameCount := size - 1

	c, err := NewClock(size)

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if c == nil {
		helpers.PrintTestErrorMsg("Expected new clock, got nil", t)
	}

	defer c.close()

	t.Run("test_pop", func(t *testing.T) {
		for i := range expectedFrameCount {
			t.Run(fmt.Sprintf("%d_test_pop", i), func(t *testing.T) {
				fr = c.Pop()

				if fr == nil {
					helpers.PrintTestErrorMsg("Expected frame, got nil.", t)
				}

				// Ensure frame is empty
				d, err := fr.ByteData()
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
				}

				if *d != testPageData {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected empty page data in new frame from buffer pool, got %v", *d), t)
				}

				if fr.isOccupied.Load() {
					helpers.PrintTestErrorMsg("Expected frame to not be occupied.", t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%d_test_pop", i))
			})
		}

		t.Run("test_pop_empty", func(t *testing.T) {
			fr = c.Pop()

			if fr != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected frame to be nil, got %v", fr), t)
			}

			helpers.PrintSuccessMsg("test_pop_empty success")
		})

		helpers.PrintSuccessMsg("test_pop success")
	})
}

func TestPopConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	var testPageData [pager.PAGE_SIZE_BYTES]byte
	var size uint64 = 100
	expectedFrameCount := size - 1

	c, err := NewClock(size)

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if c == nil {
		helpers.PrintTestErrorMsg("Expected new clock, got nil", t)
	}

	defer c.close()

	t.Run("test_pop_concurrent", func(t *testing.T) {
		start := make(chan struct{})
		for i := range expectedFrameCount {
			wg.Add(1)
			go func(i uint64) {
				defer wg.Done()
				<-start
				t.Run(fmt.Sprintf("%d_test_pop_concurrent", i), func(t *testing.T) {
					fr := c.Pop()

					if fr == nil {
						helpers.PrintTestErrorMsg(fmt.Sprintf("%d Expected frame, got nil.", i), t)
					}

					// Ensure frame is empty
					d, err := fr.ByteData()
					if err != nil {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
					}

					if *d != testPageData {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Expected empty page data in new frame from buffer pool, got %v", *d), t)
					}

					if fr.isOccupied.Load() {
						helpers.PrintTestErrorMsg("Expected frame to not be occupied.", t)
					}

					helpers.PrintSuccessMsg(fmt.Sprintf("%d_test_pop_concurrent success", i))
				})
			}(i)
		}

		close(start)
		wg.Wait()

		helpers.PrintSuccessMsg("test_pop_concurrent success")
	})
}

func TestEvictConcurrent(t *testing.T) {
	var fr *Frame
	var wg sync.WaitGroup
	var size uint64 = 100
	var unreferencedFrames uint64 = 2

	expectedFrameCount := size - 1

	c, err := NewClock(size)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if c == nil {
		t.Fatalf("Expected new clock, got nil")
	}

	defer c.close()

	// add data
	for i := range expectedFrameCount {
		fr = c.Pop()

		if fr == nil {
			helpers.PrintTestErrorMsg("Expected frame, got nil", t)
		}

		fr.MarkOccupied()
		fr.updateSegment(windowSegment)
		if i < expectedFrameCount-unreferencedFrames {
			fr.Reference()
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
				e, eKey := c.Evict(windowSegment)

				if e == nil {
					helpers.PrintTestErrorMsg("Expected evicted entry, got nil", t)
				}

				if eKey < 0 {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected evicted key, got %d", eKey), t)
				}

				if e.accessBitSet() {
					helpers.PrintTestErrorMsg("Expected acces bit to be unset.", t)
				}

				if e.isOccupied.Load() {
					helpers.PrintTestErrorMsg("Expected frame to be cleared", t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%d_evict_concurrent success", i))
			})

		}(i)
	}
	close(start)
	wg.Wait()
}

func TestEvictWithoutClearingConcurrent(t *testing.T) {
	var fr *Frame
	var wg sync.WaitGroup
	var size uint64 = 100
	var unreferencedFrames uint64 = 2

	c, err := NewClock(size)

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if c == nil {
		helpers.PrintTestErrorMsg("Expected new clock, got nil", t)
	}

	defer c.close()

	// add data
	for i := range size - 1 {
		fr = c.Pop()

		if fr == nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("%d Expected frame, got nil", i), t)
		}

		fr.MarkOccupied()
		fr.updateSegment(windowSegment)
		if i < size-unreferencedFrames {
			fr.Reference()
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
					helpers.PrintTestErrorMsg("Expected evicted entry, got nil", t)
				}

				if e.accessBitSet() {
					helpers.PrintTestErrorMsg("Expected acces bit to be unset.", t)
				}

				if !e.isOccupied.Load() {
					helpers.PrintTestErrorMsg("Expected frame to be not be cleared", t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%d_evictwithoutclearing_concurrent success", i))
			})

		}(i)
	}
	close(start)
	wg.Wait()
}

func TestAddToBPool(t *testing.T) {
	var assignedFrames []*Frame
	var size uint64 = 100
	expectedFrameCount := size - 1
	testItems := 5

	c, err := NewClock(size)

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if c == nil {
		helpers.PrintTestErrorMsg("Expected new clock, got nil", t)
	}

	defer c.close()

	for range testItems {
		fr := c.Pop()

		if fr == nil {
			helpers.PrintTestErrorMsg("Expected frame, got nil", t)
		}

		fr.MarkOccupied()
		assignedFrames = append(assignedFrames, fr)
	}

	t.Run("addtobpool_check_count_after_pop", func(t *testing.T) {
		if len(c.bPool) != int(expectedFrameCount)-testItems {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected %d remaining items in bpool, got %d", int(int(expectedFrameCount)-testItems), len(c.bPool)), t)
		}

		helpers.PrintSuccessMsg("addtobpool_check_count_after_pop success")
	})

	for i, f := range assignedFrames {
		t.Run(fmt.Sprintf("%d_addtobpool", i), func(t *testing.T) {
			// readd to bpool
			err := c.addToBpool(f)
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("(TestAddToBPool) Expected no error, got %v", err), t)
			}

			if f.isOccupied.Load() {
				helpers.PrintTestErrorMsg("Expected frame to not be occupied after readding.", t)
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_addtobpool success", i))
		})
	}

	t.Run("addtobpool_check_counter_after_readding", func(t *testing.T) {
		if len(c.bPool) != int(expectedFrameCount) {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected %d frames in bpool, got %d", int(expectedFrameCount), len(c.bPool)), t)
		}

		helpers.PrintSuccessMsg("addtobpool_check_counter_after_readding success")
	})
}

func TestAddToBPoolConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	var assignedFrames []*Frame
	var size uint64 = 100
	expectedFrameCount := size - 1
	testItems := 5

	c, err := NewClock(size)

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	if c == nil {
		helpers.PrintTestErrorMsg("Expected new clock, got nil", t)
	}

	defer c.close()

	for range testItems {
		fr := c.Pop()

		if fr == nil {
			helpers.PrintTestErrorMsg("Expected frame, got nil", t)
		}

		fr.MarkOccupied()
		assignedFrames = append(assignedFrames, fr)
	}

	t.Run("addtobpoolconcurrent_check_counte_after_pop", func(t *testing.T) {
		if len(c.bPool) != int(expectedFrameCount)-testItems {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected %d remaining items in bpool, got %d", int(int(expectedFrameCount)-testItems), len(c.bPool)), t)
		}

		helpers.PrintSuccessMsg("addtobpoolconcurrent_check_counte_after_pop success")
	})

	for i, f := range assignedFrames {
		t.Run(fmt.Sprintf("%d_addtobpoolconcurrent", i), func(t *testing.T) {
			// readd to bpool
			err := c.addToBpool(f)
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("(TestAddToBPool) Expected no error, got %v", err), t)
			}

			if f.isOccupied.Load() {
				helpers.PrintTestErrorMsg("Expected frame to not be occupied after readding.", t)
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_addtobpoolconcurrent success", i))
		})
	}

	start := make(chan struct{})
	for i := range testItems {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			t.Run(fmt.Sprintf("%d_addtobpoolconcurrent_check_counter_after_readding", j), func(t *testing.T) {
				<-start
				if len(c.bPool) != int(expectedFrameCount) {
					t.Fatalf("Expected %d frames in bpool, got %d", int(size), len(c.bPool))
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%d_addtobpoolconcurrent_check_counter_after_readding success", j))
			})
		}(i)
	}

	close(start)
	wg.Wait()

	helpers.PrintSuccessMsg("TestAddToBPoolConcurrent success")
}
