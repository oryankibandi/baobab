package buffermanager

import (
	"fmt"
	"sync"
	"testing"

	"github.com/oryankibandi/baobab/internal/manual"
	"github.com/oryankibandi/baobab/pkg/diskmanager"
)

// TODO:
// -> Test addToBpool() - item is cleared and added to bPool

func TestNewClock(t *testing.T) {
	var size uint64 = 800 // 800KB
	var allocatedRSS uint64
	expectedCapacity := (size * 1024) / diskmanager.PAGE_SIZE_BYTES
	initialRss, err := manual.CurrentRSSBytes()

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	t.Logf("Initial RSS: %dKB", initialRss/1024)

	t.Run("NewClock_low_size", func(t *testing.T) {
		_, err := NewClock(16)

		if err == nil {
			t.Fatalf("Expected function to throw err, got nil")
		}
	})

	t.Run("NewClock", func(t *testing.T) {
		c, err := NewClock(size)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if c == nil {
			t.Fatalf("Expected new clock, got nil")
		}

		t.Run("new_clock_capacity", func(t *testing.T) {
			cCap := c.getCap()

			if cCap != expectedCapacity {
				t.Fatalf("Expected capacity to be %d, got %d", expectedCapacity, cCap)
			}
		})

		t.Run("new_clock_allocated_size", func(t *testing.T) {
			allocatedRSS, err = manual.CurrentRSSBytes()

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			t.Logf("Allocated RSS: %dKB", allocatedRSS/1024)

			// Resident Set Size should be greater than the memory
			// allocated
			if (allocatedRSS/1024)-(initialRss/1024) < size {
				t.Fatalf("Expected RSS above %d KB, but got %d KB", size, (allocatedRSS/1024)-(initialRss/1024))
			}
		})

		t.Run("new_clock_linked_circular_buffer", func(t *testing.T) {
			currFr := c.Head
			for range expectedCapacity {
				if currFr.GetPrevLink() == nil {
					t.Fatalf("Frame has no previous link in circular buffer")
				}

				if currFr.GetNextLink() == nil {
					t.Fatalf("Frame has no next link in circular buffer")
				}
			}
		})

		t.Run("new_clock_close", func(t *testing.T) {
			err = c.close()
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			currRss, err := manual.CurrentRSSBytes()
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Resident Set Size should be greater than the memory
			// allocated
			if (currRss/1024) > allocatedRSS && !ASANEnabled {
				t.Fatalf("Expected RSS less than %d KB, but got %d KB", allocatedRSS, (currRss / 1024))
			}
		})
	})
}

func TestPop(t *testing.T) {
	var fr *Frame
	var testPageData [diskmanager.PAGE_SIZE_BYTES]byte
	var size uint64 = 800 // KB

	expectedCapacity := (size * 1024) / diskmanager.PAGE_SIZE_BYTES
	c, err := NewClock(size)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if c == nil {
		t.Fatalf("Expected new clock, got nil")
	}

	defer c.close()

	t.Run("test_pop", func(t *testing.T) {
		for i := range expectedCapacity {
			t.Run(fmt.Sprintf("%d_test_pop", i), func(t *testing.T) {
				fr = c.Pop()

				if fr == nil {
					t.Fatalf("Expected frame, got nil.")
				}

				// Ensure frame is empty
				d, err := fr.ByteData()
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if *d != testPageData {
					t.Fatalf("Expected empty page data in new frame from buffer pool, got %v", *d)
				}

				if fr.isOccupied.Load() {
					t.Fatalf("Expected frame to not be occupied.")
				}
			})
		}

		t.Run("test_pop_empty", func(t *testing.T) {
			fr = c.Pop()

			if fr != nil {
				t.Fatalf("Expected frame to be nil, got %v", fr)
			}
		})
	})
}

func TestPopConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	var testPageData [diskmanager.PAGE_SIZE_BYTES]byte
	var size uint64 = 800 // KB

	expectedCapacity := (size * 1024) / diskmanager.PAGE_SIZE_BYTES
	c, err := NewClock(size)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if c == nil {
		t.Fatalf("Expected new clock, got nil")
	}

	defer c.close()

	t.Run("test_pop_concurrent", func(t *testing.T) {
		start := make(chan struct{})
		for i := range expectedCapacity {
			wg.Add(1)
			go func(i uint64) {
				defer wg.Done()
				<-start
				t.Run(fmt.Sprintf("%d_test_pop_concurrent", i), func(t *testing.T) {
					fr := c.Pop()

					if fr == nil {
						t.Fatalf("Expected frame, got nil.")
					}

					// Ensure frame is empty
					d, err := fr.ByteData()
					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					if *d != testPageData {
						t.Fatalf("Expected empty page data in new frame from buffer pool, got %v", *d)
					}

					if fr.isOccupied.Load() {
						t.Fatalf("Expected frame to not be occupied.")
					}
				})
			}(i)
		}

		close(start)
		wg.Wait()

	})
}

func TestEvictConcurrent(t *testing.T) {
	var fr *Frame
	var wg sync.WaitGroup
	var size uint64 = 800 // KB
	var unreferencedFrames uint64 = 2

	expectedCapacity := (size * 1024) / diskmanager.PAGE_SIZE_BYTES
	c, err := NewClock(size)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if c == nil {
		t.Fatalf("Expected new clock, got nil")
	}

	defer c.close()

	// add data
	for i := range expectedCapacity {
		fr = c.Pop()

		if fr == nil {
			t.Fatalf("Expected frame, got nil")
		}

		fr.MarkOccupied()
		fr.updateSegment(windowSegment)
		if i < expectedCapacity-unreferencedFrames {
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
				e, eK := c.Evict(windowSegment)

				if e == nil {
					t.Fatalf("Expected evicted entry, got nil")
				}

				if eK < 0 {
					t.Fatalf("Expected evicted key, got %d", eK)
				}

				if e.accessBitSet() {
					t.Fatalf("Expected acces bit to be unset.")
				}

				if e.isOccupied.Load() {
					t.Fatalf("Expected frame to be cleared")
				}
			})

		}(i)
	}
	close(start)
	wg.Wait()
}

func TestEvictWithoutClearingConcurrent(t *testing.T) {
	var fr *Frame
	var wg sync.WaitGroup
	var size uint64 = 800 // KB
	var unreferencedFrames uint64 = 2

	expectedCapacity := (size * 1024) / diskmanager.PAGE_SIZE_BYTES
	c, err := NewClock(size)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if c == nil {
		t.Fatalf("Expected new clock, got nil")
	}

	defer c.close()

	// add data
	for i := range expectedCapacity {
		fr = c.Pop()

		if fr == nil {
			t.Fatalf("Expected frame, got nil")
		}

		fr.MarkOccupied()
		fr.updateSegment(windowSegment)
		if i < expectedCapacity-unreferencedFrames {
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
					t.Fatalf("Expected evicted entry, got nil")
				}

				if e.accessBitSet() {
					t.Fatalf("Expected acces bit to be unset.")
				}

				if !e.isOccupied.Load() {
					t.Fatalf("Expected frame to be not be cleared")
				}
			})

		}(i)
	}
	close(start)
	wg.Wait()
}
