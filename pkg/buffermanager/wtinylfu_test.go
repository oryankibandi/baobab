package buffermanager

import (
	"fmt"
	"log"
	"testing"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
)

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

func TestPromoteToProtected(t *testing.T) {
	var protectedSegmentCandidate *Frame

	windowCapacity := 48
	windowFrameCount := (windowCapacity * 1024) / diskmanager.PAGE_SIZE_BYTES
	mainCapacity := 2376

	w, err := NewWTinylfu(uint64(windowCapacity), uint64(mainCapacity))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	defer w.close()

	for i := range windowFrameCount {
		t.Run(fmt.Sprintf("%d_test_promotetoprotected_additem", i), func(t *testing.T) {
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
				protectedSegmentCandidate = f
			}
		})
	}

	t.Run("test_promotetoprotected", func(t *testing.T) {
		if protectedSegmentCandidate == nil {
			t.Fatalf("No frame to be evicted set")
		}
		if protectedSegmentCandidate.refBitSet() {
			t.Fatalf("Expected frame to be evicted to be unreferenced")
		}

		// evict item from window cache into probation
		testPage := diskmanager.NewTestPage(int32(windowFrameCount) + 1)
		fr, _, err := w.AddItem(testPage, false)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if seg := fr.getSegType(); seg != windowSegment {
			t.Fatalf("Expected new frame to be in window segment, got %v", seg)
		}

		if seg := protectedSegmentCandidate.getSegType(); seg != probationSegment {
			t.Fatalf("Expected evicted frame to be in probation segment, got %v", seg)
		}

		// increment count of item in probation to trigger promotion
		probationCount := w.getProbationCount()
		protectedCount := w.getProtectedCount()

		ok, err := w.Increment(protectedSegmentCandidate)
		if err != nil {
			log.Fatalf("Expected no error, got %v", err)
		}

		if !ok {
			log.Fatalf("Increment operation not successful")
		}

		if seg := protectedSegmentCandidate.getSegType(); seg != protectedSegment {
			t.Fatalf("Expected frame to have be in protected segment, got %v", seg)
		}

		if prob := w.getProbationCount(); prob != probationCount-1 {
			t.Fatalf("Expected probation count to be %d, got %d", probationCount-1, prob)
		}

		if prot := w.getProtectedCount(); prot != protectedCount+1 {
			t.Fatalf("Expected protected count to be %d, got %d", protectedCount+1, prot)
		}
	})
}

// Tests eviction from protected and probation segments in main cache
// To test eviction from protected, all segments have to be filled up.
func TestProtectedEviction(t *testing.T) {
	var testPage *diskmanager.Page
	var pageCounter int32 = 1
	var windowCacheSize uint64 = 48
	var mainCacheSize uint64 = 2376

	windowFrameCount := (windowCacheSize * 1024) / diskmanager.PAGE_SIZE_BYTES
	mainItemCount := (mainCacheSize * 1024) / diskmanager.PAGE_SIZE_BYTES
	probationFrameCount := uint64(float64(mainItemCount) * MAIN_CACHE_RATIO)
	protectedFrameCount := uint64(float64(mainItemCount) * float64(1.0-MAIN_CACHE_RATIO))

	// number of times protected segment is larger than probation
	probToProtMultiplier := uint64(1 / MAIN_CACHE_RATIO)

	w, err := NewWTinylfu(uint64(windowCacheSize), uint64(mainCacheSize))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	defer w.close()

	//1. fill window cache
	for range windowFrameCount {
		testPage = diskmanager.NewTestPage(pageCounter)
		f, _, err := w.AddItem(testPage, false)
		if err != nil {
			t.Fatalf("Expected not error,got %v", err)
		}

		if seg := f.getSegType(); seg != windowSegment {
			t.Fatalf("Expected new frame to be in window segment, got %v", seg)
		}

		pageCounter++
	}

	//  iteratively move items from probation to protected till protected is full
	for i := range probToProtMultiplier {
		t.Run(fmt.Sprintf("%d_protectedeviction_fillprotected", i), func(t *testing.T) {
			// 2. fill probation
			for j := range probationFrameCount {
				t.Run(fmt.Sprintf("%d_protectedeviction_addtoprobation", j), func(t *testing.T) {
					testPage = diskmanager.NewTestPage(pageCounter)
					f, _, err := w.AddItem(testPage, false)
					if err != nil {
						t.Fatalf("Expected no error,got %v", err)
					}

					if seg := f.getSegType(); seg != windowSegment {
						t.Fatalf("Expected new frame to be in window segment, got %v", seg)
					}

					pageCounter++
				})
			}

			// 3. fill protected segment.
			// Iterate over clock buffer and increment count items in probation.
			itemsPromoted := 0
			itemsIterated := 0
			currFrame := w.cBuffer.Head
			totCap := w.cBuffer.capacity
			for itemsPromoted < int(probationFrameCount) {
				if itemsIterated >= int(totCap) {
					t.Fatalf("Iterated over all %d frames but did not fill probation capacity of %d, with total capacity of %d, only managed to fill %d probation items", itemsIterated, probationFrameCount, totCap, itemsPromoted)
				}
				if currFrame.getSegType() != probationSegment {
					currFrame = currFrame.GetNextLink()
					itemsIterated++
					continue
				}

				ok, err := w.Increment(currFrame)
				if err != nil {
					t.Fatalf("Expected not error, got %v", err)
				}

				if !ok {
					t.Fatalf("Increment operation failed.")
				}

				itemsPromoted++
				itemsIterated++
				currFrame = currFrame.GetNextLink()
			}

		})
	}

	// 4. refill probation by adding items to window
	for i := range windowFrameCount {
		t.Run(fmt.Sprintf("protectedeviction_refillprobation_%d", i), func(t *testing.T) {
			testPage = diskmanager.NewTestPage(pageCounter)
			f, _, err := w.AddItem(testPage, false)
			if err != nil {
				t.Fatalf("Expected no error,got %v", err)
			}

			if seg := f.getSegType(); seg != windowSegment {
				t.Fatalf("Expected new frame to be in window segment, got %v", seg)
			}

		})
		pageCounter++
	}

	// Expect all segments to be full
	t.Run("protectedeviction_allsegmentsfull", func(t *testing.T) {
		if wCount := w.getWindowCount(); wCount != windowFrameCount {
			t.Fatalf("Expected window cache to be full, got %d items instead of %d items", wCount, windowFrameCount)
		}

		if probCount := w.getProbationCount(); probCount != probationFrameCount {
			t.Fatalf("Expected probation segment be full, got %d items instead of %d items", probCount, probationFrameCount)
		}

		if protCount := w.getProtectedCount(); protCount != protectedFrameCount {
			t.Fatalf("Expected protected segment be full, got %d items instead of %d items", protCount, protectedFrameCount)
		}
	})

	// 5. Iterate through clock buffer and pin all frames except one
	//    in probation and protected
	var unreferencedProbationFrame *Frame
	var unreferencedProtectedFrame *Frame
	probationFrames := make([]*Frame, 0)
	protectedFrames := make([]*Frame, 0)
	currFrame := w.cBuffer.clockHead()
	for i := range w.cBuffer.capacity {
		t.Run(fmt.Sprintf("protectedeviction_pinframes_%d", i), func(t *testing.T) {
			switch currFrame.getSegType() {
			case probationSegment:
				if unreferencedProbationFrame == nil {
					unreferencedProbationFrame = currFrame
					probationFrames = append(probationFrames, currFrame)
				} else {
					currFrame.Reference()
					probationFrames = append(probationFrames, currFrame)
					if !currFrame.accessBitSet() {
						t.Fatalf("probation frame not pinned")
					}
				}
				currFrame = currFrame.GetNextLink()
			case protectedSegment:
				if unreferencedProtectedFrame == nil {
					unreferencedProtectedFrame = currFrame
					protectedFrames = append(protectedFrames, currFrame)
				} else {
					currFrame.Reference()
					protectedFrames = append(protectedFrames, currFrame)
					if !currFrame.accessBitSet() {
						t.Fatalf("protected frame not pinned")
					}
				}
				currFrame = currFrame.GetNextLink()
			default:
				currFrame = currFrame.GetNextLink()
			}
		})
	}

	// 6. Increment count of unreferenced frame in probation and check segment
	//    of both unreferenced probation and protected segments
	t.Run("protectedeviction_evictprotected", func(t *testing.T) {
		if unreferencedProbationFrame == nil {
			t.Fatalf("Unreferenced probation frame is nil")
		}

		if unreferencedProtectedFrame == nil {
			t.Fatalf("Unreferenced protected frame is nil")
		}

		ok, err := w.Increment(unreferencedProbationFrame)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if !ok {
			t.Fatal("Unable to increment probation frame")
		}

		// ensure frame from probation is promoted to protected and protected frame is demoted to probation
		if seg := unreferencedProbationFrame.getSegType(); seg != protectedSegment {
			t.Fatalf("Expected frame from probation to be promoted to protected, got %v", seg)
		}

		if seg := unreferencedProtectedFrame.getSegType(); seg != probationSegment {
			t.Fatalf("Expected frame from probation to be promoted to protected, got %v", seg)
		}
	})

	// 7. Add item to window to trigger eviction from probation.
	t.Run("protectedeviction_evict_from_probation", func(t *testing.T) {
		testPage = diskmanager.NewTestPage(pageCounter)
		f, _, err := w.AddItem(testPage, false)
		if err != nil {
			t.Fatalf("Expected no error,got %v", err)
		}

		if seg := f.getSegType(); seg != windowSegment {
			t.Fatalf("Expected new frame to be in window segment, got %v", seg)
		}

		pageCounter++
	})

	// 8. Test adding to full cache with all items pinned
	t.Run("protectedeviction_add_to_full_cache", func(t *testing.T) {
		unreferencedProbationFrame.Reference()

		if !unreferencedProbationFrame.accessBitSet() {
			t.Fatal("Unable to reference frame")
		}

		unreferencedProtectedFrame.Reference()
		if !unreferencedProtectedFrame.accessBitSet() {
			t.Fatal("Unable to reference frame")
		}

		// add item
		testPage = diskmanager.NewTestPage(pageCounter)
		_, _, err := w.AddItem(testPage, false)

		if err == nil {
			t.Fatalf("Expected an error,got nil")
		}

		pageCounter++
	})

	// 9. Test adding to not full cache
	t.Run("protectedeviction_add_to_full_cache_with_available_frames", func(t *testing.T) {
		// unreferencedProtectedFrame is now in probation as a reult of previus test above
		unreferencedProtectedFrame.Unreference()
		if unreferencedProtectedFrame.accessBitSet() {
			t.Fatal("Unable to dereference frame")
		}

		// add item
		testPage = diskmanager.NewTestPage(pageCounter)
		f, _, err := w.AddItem(testPage, false)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if f == nil {
			t.Fatal("Expected new frame, got nil")
		}

		if seg := f.getSegType(); seg != windowSegment {
			t.Fatalf("Expected new frame to be in window segment, got %v", seg)
		}

		pageCounter++
	})
}
