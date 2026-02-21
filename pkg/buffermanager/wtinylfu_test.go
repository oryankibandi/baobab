package buffermanager

import (
	"fmt"
	"testing"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
)

// TODO: Items to test
// 1. NewWtinylfu
//    -> No errors
//    -> Check capacity
//    -> Edge cases - invalid window and main cache sizes
// 2. EvictWindow()
//    -> Ensure frame segment is updated
//    -> Ensure segment count is updated
// 3. AddItem()
//    -> No errors
//    -> Page added to the right segment
//    -> If any segment is full, eviction happens are required.
//    -> Incase of eviction, the frame's segment is updated
//    -> The new frame is updated with the right page data.
// 4. promoteToProtected() - Test via filling window cache, then forcing eviction,
// then force promotion
//    -> ensure probation segment and protected counts are updated.
// 5. Increment()
//    -> if item is in probation, expect segment to change to protected

func TestNewWTinyLFU(t *testing.T) {
	tests := []struct {
		windowSize uint64
		mainSize   uint64
		valid      bool
	}{
		{windowSize: 20, mainSize: 120, valid: true},
		{windowSize: 120, mainSize: 20, valid: false},
		{windowSize: 10, mainSize: 120, valid: true},
		{windowSize: 20, mainSize: 3500, valid: true},
		{windowSize: 250, mainSize: 250, valid: false},
		{windowSize: 1500, mainSize: 1500, valid: false},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d_test_newWtinylfu", i), func(t *testing.T) {
			winCap := (test.windowSize * 1024) / diskmanager.PAGE_SIZE_BYTES
			probCap := uint64(float64((test.mainSize*1024)/diskmanager.PAGE_SIZE_BYTES) * float64(MAIN_CACHE_RATIO))
			protCap := uint64(float64((test.mainSize*1024)/diskmanager.PAGE_SIZE_BYTES) * (float64(1.0) - MAIN_CACHE_RATIO))
			w, err := NewWTinylfu(test.windowSize, test.mainSize)

			if !test.valid {
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if w == nil {
					t.Fatalf("Expected wtinylfu instance, got nil")
				}

				if w.windowCapacity != winCap {
					t.Fatalf("Expected window cache to be of size %d items, got %d items", winCap, w.windowCapacity)
				}

				if w.probationCapacity != probCap {
					t.Fatalf("Expected probation cache to be of size %d items, got %d items", probCap, w.probationCapacity)
				}

				if w.protectedCapacity != protCap {
					t.Fatalf("Expected probation cache to be of size %d items, got %d items", probCap, w.probationCapacity)
				}

				w.close()
			}
		})
	}
}

func TestEvictWindow(t *testing.T) {
	// var testPage *diskmanager.Page
	var frameToBeEvicted *Frame

	windowCapacity := 48
	windowFrameCount := (windowCapacity * 1024) / diskmanager.PAGE_SIZE_BYTES
	mainCapacity := 2376

	w, err := NewWTinylfu(uint64(windowCapacity), uint64(mainCapacity))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	defer w.close()

	for i := range windowFrameCount {
		t.Run(fmt.Sprintf("%d_testevict_additem", i), func(t *testing.T) {
			testPage := diskmanager.NewTestPage(int32(i) + 1)
			f, _, err := w.AddItem(testPage, false)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if seg := f.getSegType(); seg != windowSegment {
				t.Fatalf("Expected new frame to be in window segment, got %v", seg)
			}

			// pin all frames apart from the most recent.
			if i+1 < windowFrameCount {
				f.Reference()
			} else {
				frameToBeEvicted = f
			}
		})
	}

	t.Run("testevict_counters", func(t *testing.T) {
		wc := w.getWindowCount()

		if wc != uint64(windowFrameCount) {
			t.Fatalf("Expected window count to be %d but got %d", windowFrameCount, wc)
		}
	})

	t.Run("testevict_evictwindow", func(t *testing.T) {
		if frameToBeEvicted == nil {
			t.Fatalf("No frame to be evicted set")
		}
		if frameToBeEvicted.refBitSet() {
			t.Fatalf("Expected frame to be evicted to be unreferenced")
		}

		testPage := diskmanager.NewTestPage(int32(windowFrameCount) + 1)
		fr, _, err := w.AddItem(testPage, false)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if seg := fr.getSegType(); seg != windowSegment {
			t.Fatalf("Expected new frame to be in window segment, got %v", seg)
		}

		if seg := frameToBeEvicted.getSegType(); seg != probationSegment {
			t.Fatalf("Expected evicted frame to be in probation segment, got %v", seg)
		}
	})
}
