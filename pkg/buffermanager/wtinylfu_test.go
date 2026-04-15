package buffermanager

import (
	"fmt"
	"testing"

	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/pager"
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
			winCap := (test.windowSize * 1024) / pager.PAGE_SIZE_BYTES
			probCap := uint64(float64((test.mainSize*1024)/pager.PAGE_SIZE_BYTES) * float64(MAIN_CACHE_RATIO))
			protCap := uint64(float64((test.mainSize*1024)/pager.PAGE_SIZE_BYTES)*(float64(1.0)-MAIN_CACHE_RATIO)) - 1
			w, err := NewWTinylfu(test.windowSize, test.mainSize)

			if !test.valid {
				if err == nil {
					helpers.PrintTestErrorMsg("Expected error, got nil", t)
				}
			} else {
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
				}

				if w == nil {
					helpers.PrintTestErrorMsg("Expected wtinylfu instance, got nil", t)
				}

				if w.windowCapacity != winCap {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected window cache to be of size %d items, got %d items", winCap, w.windowCapacity), t)
				}

				if w.probationCapacity != probCap {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected probation cache to be of size %d items, got %d items", probCap, w.probationCapacity), t)
				}

				if w.protectedCapacity != protCap {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected probation cache to be of size %d items, got %d items", probCap, w.probationCapacity), t)
				}

				w.close()
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_test_newWtinylfu success", i))
		})
	}
}

func TestEvictWindow(t *testing.T) {
	var frameToBeEvicted *Frame

	windowCapacity := 48
	mainCapacity := 2376
	windowFrameCount := (windowCapacity * 1024) / pager.PAGE_SIZE_BYTES

	w, err := NewWTinylfu(uint64(windowCapacity), uint64(mainCapacity))
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	defer w.close()

	for i := range windowFrameCount {
		t.Run(fmt.Sprintf("%d_testevict_additem", i), func(t *testing.T) {
			// update frame
			f, _, err := w.getFreeFrame()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
			}

			if seg := f.getSegType(); seg != windowSegment {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
			}

			// pin all frames apart from the most recent.
			if i+1 < windowFrameCount {
				f.Reference()
			} else {
				frameToBeEvicted = f
			}

			helpers.PrintSuccessMsg("%d_testevict_additem success")
		})
	}

	t.Run("testevict_counters", func(t *testing.T) {
		wc := w.getWindowCount()

		if wc != uint64(windowFrameCount) {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected window count to be %d but got %d", windowFrameCount, wc), t)
		}

		helpers.PrintSuccessMsg("testevict_counters success")
	})

	t.Run("testevict_evictwindow", func(t *testing.T) {
		if frameToBeEvicted == nil {
			helpers.PrintTestErrorMsg("No frame to be evicted set", t)
		}
		if frameToBeEvicted.refBitSet() {
			helpers.PrintTestErrorMsg("Expected frame to be evicted to be unreferenced", t)
		}

		fr, _, err := w.getFreeFrame()
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}

		if seg := fr.getSegType(); seg != windowSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
		}

		if seg := frameToBeEvicted.getSegType(); seg != probationSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected evicted frame to be in probation segment, got %v", seg), t)
		}

		helpers.PrintSuccessMsg("testevict_evictwindow success")
	})
}

func TestPromoteToProtected(t *testing.T) {
	var protectedSegmentCandidate *Frame

	windowCapacity := 48
	windowFrameCount := (windowCapacity * 1024) / pager.PAGE_SIZE_BYTES
	mainCapacity := 2376

	w, err := NewWTinylfu(uint64(windowCapacity), uint64(mainCapacity))
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	defer w.close()

	for i := range windowFrameCount {
		t.Run(fmt.Sprintf("%d_test_promotetoprotected_additem", i), func(t *testing.T) {
			f, _, err := w.getFreeFrame()
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
			}

			if seg := f.getSegType(); seg != windowSegment {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
			}

			// pin all frames apart from the most recent.
			if i+1 < windowFrameCount {
				f.Reference()
			} else {
				protectedSegmentCandidate = f
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_test_promotetoprotected_additem success", i))
		})
	}

	t.Run("test_promotetoprotected", func(t *testing.T) {
		if protectedSegmentCandidate == nil {
			helpers.PrintTestErrorMsg("No frame to be evicted set", t)
		}
		if protectedSegmentCandidate.refBitSet() {
			helpers.PrintTestErrorMsg("Expected frame to be evicted to be unreferenced", t)
		}

		// evict item from window cache into probation
		fr, _, err := w.getFreeFrame()

		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}

		if seg := fr.getSegType(); seg != windowSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
		}

		if seg := protectedSegmentCandidate.getSegType(); seg != probationSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected evicted frame to be in probation segment, got %v", seg), t)
		}

		// increment count of item in probation to trigger promotion
		probationCount := w.getProbationCount()
		protectedCount := w.getProtectedCount()

		ok, err := w.Increment(protectedSegmentCandidate)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}

		if !ok {
			helpers.PrintTestErrorMsg("Increment operation not successful", t)
		}

		if seg := protectedSegmentCandidate.getSegType(); seg != protectedSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected frame to have be in protected segment, got %v", seg), t)
		}

		if prob := w.getProbationCount(); prob != probationCount-1 {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected probation count to be %d, got %d", probationCount-1, prob), t)
		}

		if prot := w.getProtectedCount(); prot != protectedCount+1 {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected protected count to be %d, got %d", protectedCount+1, prot), t)
		}

		helpers.PrintSuccessMsg("test_promotetoprotected success")
	})
}

// Tests eviction from protected and probation segments in main cache
// To test eviction from protected, all segments have to be filled up.
func TestProtectedEviction(t *testing.T) {
	var pageCounter int32 = 1
	var windowCacheSize uint64 = 48
	var mainCacheSize uint64 = 2376

	windowFrameCount := (windowCacheSize * 1024) / pager.PAGE_SIZE_BYTES
	mainItemCount := (mainCacheSize * 1024) / pager.PAGE_SIZE_BYTES
	probationFrameCount := uint64(float64(mainItemCount) * MAIN_CACHE_RATIO)
	protectedFrameCount := uint64(float64(mainItemCount)*float64(1.0-MAIN_CACHE_RATIO)) - 1

	// number of times protected segment is larger than probation
	probToProtMultiplier := uint64(1 / MAIN_CACHE_RATIO)

	w, err := NewWTinylfu(uint64(windowCacheSize), uint64(mainCacheSize))
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	defer w.close()

	// 1. fill window cache
	for range windowFrameCount {
		f, _, err := w.getFreeFrame()
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected not error,got %v", err), t)
		}

		if seg := f.getSegType(); seg != windowSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
		}

		pageCounter++
	}

	// iteratively move items from probation to protected till protected is full
	for i := range probToProtMultiplier {
		t.Run(fmt.Sprintf("%d_protectedeviction_fillprotected", i), func(t *testing.T) {
			// 2. fill probation
			for j := range probationFrameCount {
				t.Run(fmt.Sprintf("%d_protectedeviction_addtoprobation", j), func(t *testing.T) {
					f, _, err := w.getFreeFrame()
					if err != nil {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error,got %v", err), t)
					}

					if seg := f.getSegType(); seg != windowSegment {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
					}

					pageCounter++
				})
			}

			helpers.PrintInfoMsg("Printing wtinylfu stats------------")
			w.Stat()
			helpers.PrintInfoMsg("Done ------------")

			// 3. fill protected segment.
			// Iterate over clock buffer and increment count items in probation.
			itemsPromoted := 0
			itemsIterated := 0
			currFrame := w.cBuffer.Head
			totCap := w.cBuffer.capacity
			for itemsPromoted < int(probationFrameCount) {
				if itemsIterated >= int(totCap) {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Iterated over all %d frames but did not fill probation capacity of %d, with total capacity of %d, only managed to fill %d probation items", itemsIterated, probationFrameCount, totCap, itemsPromoted), t)
				}
				if seg := currFrame.getSegType(); seg != probationSegment {
					currFrame = currFrame.GetNextLink()
					itemsIterated++
					continue
				}

				ok, err := w.Increment(currFrame)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected not error, got %v", err), t)
				}

				if !ok {
					helpers.PrintTestErrorMsg("Increment operation failed.", t)
				}

				itemsPromoted++
				itemsIterated++
				currFrame = currFrame.GetNextLink()
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_protectedeviction_fillprotected success", i))
		})
	}

	helpers.PrintSuccessMsg("Succesfully filled protected cache. printing stats..")
	w.Stat()
	helpers.PrintSuccessMsg("Done----")

	// 4. refill probation by adding items to window
	for i := range windowFrameCount {
		t.Run(fmt.Sprintf("protectedeviction_refillprobation_%d", i), func(t *testing.T) {
			f, _, err := w.getFreeFrame()
			if err != nil {
				t.Fatalf("Expected no error,got %v", err)
			}

			if seg := f.getSegType(); seg != windowSegment {
				t.Fatalf("Expected new frame to be in window segment, got %v", seg)
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("protectedeviction_refillprobation_%d success", i))
		})
		pageCounter++
	}

	// Expect all segments to be full
	t.Run("protectedeviction_allsegmentsfull", func(t *testing.T) {
		if wCount := w.getWindowCount(); wCount != windowFrameCount {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected window cache to be full, got %d items instead of %d items", wCount, windowFrameCount), t)
		}

		if probCount := w.getProbationCount(); probCount != probationFrameCount {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected probation segment be full, got %d items instead of %d items", probCount, probationFrameCount), t)
		}

		if protCount := w.getProtectedCount(); protCount != protectedFrameCount {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected protected segment be full, got %d items instead of %d items", protCount, protectedFrameCount), t)
		}

		helpers.PrintSuccessMsg("protectedeviction_allsegmentsfull success")
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

			helpers.PrintSuccessMsg(fmt.Sprintf("protectedeviction_pinframes_%d success", i))
		})
	}

	// 6. Increment count of unreferenced frame in probation and check segment
	//    of both unreferenced probation and protected segments
	t.Run("protectedeviction_evictprotected", func(t *testing.T) {
		if unreferencedProbationFrame == nil {
			helpers.PrintTestErrorMsg("Unreferenced probation frame is nil", t)
		}

		if unreferencedProtectedFrame == nil {
			helpers.PrintTestErrorMsg("Unreferenced protected frame is nil", t)
		}

		ok, err := w.Increment(unreferencedProbationFrame)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}

		if !ok {
			helpers.PrintTestErrorMsg("Unable to increment probation frame", t)
		}

		// ensure frame from probation is promoted to protected and protected frame is demoted to probation
		if seg := unreferencedProbationFrame.getSegType(); seg != protectedSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected frame from probation to be promoted to protected, got %v", seg), t)
		}

		if seg := unreferencedProtectedFrame.getSegType(); seg != probationSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected frame from probation to be promoted to protected, got %v", seg), t)
		}

		helpers.PrintSuccessMsg("protectedeviction_evictprotected success")
	})

	// 7. Add item to window to trigger eviction from probation.
	t.Run("protectedeviction_evict_from_probation", func(t *testing.T) {
		f, _, err := w.getFreeFrame()
		if err != nil {
			t.Fatalf("Expected no error,got %v", err)
		}

		if seg := f.getSegType(); seg != windowSegment {
			t.Fatalf("Expected new frame to be in window segment, got %v", seg)
		}

		pageCounter++

		helpers.PrintSuccessMsg("protectedeviction_evict_from_probation success")
	})

	// 8. Test adding to full cache with all items pinned
	t.Run("protectedeviction_add_to_full_cache", func(t *testing.T) {
		unreferencedProbationFrame.Reference()

		if !unreferencedProbationFrame.accessBitSet() {
			helpers.PrintTestErrorMsg("Unable to reference frame", t)
		}

		unreferencedProtectedFrame.Reference()
		if !unreferencedProtectedFrame.accessBitSet() {
			helpers.PrintTestErrorMsg("Unable to reference frame", t)
		}

		// add item
		_, _, err := w.getFreeFrame()

		if err == nil {
			helpers.PrintTestErrorMsg("Expected an error,got nil", t)
		}

		pageCounter++

		helpers.PrintSuccessMsg("protectedeviction_add_to_full_cache success")
	})

	// 9. Test adding to not full cache
	t.Run("protectedeviction_add_to_full_cache_with_available_frames", func(t *testing.T) {
		// unreferencedProtectedFrame is now in probation as a reult of previus test above
		unreferencedProtectedFrame.Unreference()
		if unreferencedProtectedFrame.accessBitSet() {
			t.Fatal("Unable to dereference frame")
		}

		// add item
		f, _, err := w.getFreeFrame()

		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}

		if f == nil {
			helpers.PrintTestErrorMsg("Expected new frame, got nil", t)
		}

		if seg := f.getSegType(); seg != windowSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
		}

		pageCounter++

		helpers.PrintSuccessMsg("protectedeviction_add_to_full_cache_with_available_frames success")
	})
}
