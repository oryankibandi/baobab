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
			}
		})
	}
}
