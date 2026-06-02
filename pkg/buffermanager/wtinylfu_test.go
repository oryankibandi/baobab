package buffermanager

import (
	"fmt"
	"math"
	"testing"

	"github.com/oryankibandi/baobab/pkg/helpers"
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
			probCap := uint64(math.Round(float64(test.mainSize) * MAIN_CACHE_RATIO))
			protCap := uint64(math.Round(float64(test.mainSize)*(1.0-MAIN_CACHE_RATIO))) - 1
			w, err := NewWTinylfu(test.windowSize, test.mainSize, true)

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

				if w.windowCapacity != test.windowSize {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected window cache to be of size %d items, got %d items", test.windowSize, w.windowCapacity), t)
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
	var frameToBeEvicted *clockentry

	windowCapacity := 48
	mainCapacity := 2376
	// windowFrameCount := (windowCapacity * 1024) / pager.PAGE_SIZE_BYTES

	// initialize pager
	pgr := InitPager(t)
	defer pgr.Close()

	w, err := NewWTinylfu(uint64(windowCapacity), uint64(mainCapacity), true)
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	defer w.close()

	for i := range windowCapacity {
		t.Run(fmt.Sprintf("%d_testevict_additem", i), func(t *testing.T) {
			// update frame
			en, _, err := w.getFreeFrame(pgr)
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
			}
			f := &en.entry
			f.parentEntry = en

			if seg := en.getSegType(); seg != windowSegment {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
			}

			// pin all frames apart from the most recent.
			if i+1 >= windowCapacity {
				f.Unreference()
				frameToBeEvicted = en
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_testevict_additem success", i))
		})
	}

	t.Run("testevict_counters", func(t *testing.T) {
		wc := w.getWindowCount()

		if wc != uint64(windowCapacity) {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected window count to be %d but got %d", windowCapacity, wc), t)
		}

		helpers.PrintSuccessMsg("testevict_counters success")
	})

	t.Run("testevict_evictwindow", func(t *testing.T) {
		if frameToBeEvicted == nil {
			helpers.PrintTestErrorMsg("No frame to be evicted set", t)
		}
		if frameToBeEvicted.isReferenced() {
			helpers.PrintTestErrorMsg("Expected frame to be evicted to be unreferenced", t)
		}

		en, _, err := w.getFreeFrame(pgr)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
		}
		fr := &en.entry
		fr.parentEntry = en

		if seg := en.getSegType(); seg != windowSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
		}

		if seg := frameToBeEvicted.segtype; seg != probationSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected evicted frame to be in probation segment, got %v", seg), t)
		}

		helpers.PrintSuccessMsg("testevict_evictwindow success")
	})
}

func TestPromoteToProtected(t *testing.T) {
	var protectedSegmentCandidate *clockentry

	windowCapacity := 48
	// windowFrameCount := (windowCapacity * 1024) / pager.PAGE_SIZE_BYTES
	mainCapacity := 2376

	// initialize pager
	pgr := InitPager(t)
	defer pgr.Close()

	w, err := NewWTinylfu(uint64(windowCapacity), uint64(mainCapacity), true)
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	defer w.close()

	for i := range windowCapacity {
		t.Run(fmt.Sprintf("%d_test_promotetoprotected_additem", i), func(t *testing.T) {
			en, _, err := w.getFreeFrame(pgr)
			if err != nil {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
			}

			if seg := en.getSegType(); seg != windowSegment {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
			}

			// pin all frames apart from the most recent.
			if i+1 >= windowCapacity {
				en.unref()
				protectedSegmentCandidate = en
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_test_promotetoprotected_additem success", i))
		})
	}

	t.Run("test_promotetoprotected", func(t *testing.T) {
		if protectedSegmentCandidate == nil {
			helpers.PrintTestErrorMsg("No frame to be evicted set", t)
		}
		if protectedSegmentCandidate.isReferenced() {
			helpers.PrintTestErrorMsg("Expected frame to be evicted to be unreferenced", t)
		}

		// evict item from window cache into probation
		fr, _, err := w.getFreeFrame(pgr)

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
	// var windowCacheSize uint64 = 48
	// var mainCacheSize uint64 = 2376
	var totCacheSize uint64 = 10000

	windowFrameCount := uint64(math.Round(float64(totCacheSize) * WINDOW_CACHE_RATIO))
	mainItemCount := uint64(math.Round(float64(totCacheSize) * (1.0 - WINDOW_CACHE_RATIO)))
	probationFrameCount := uint64(math.Round(float64(mainItemCount) * MAIN_CACHE_RATIO))
	protectedFrameCount := uint64(math.Round(float64(mainItemCount)*float64(1.0-MAIN_CACHE_RATIO))) - 1

	// number of times protected segment is larger than probation
	probToProtMultiplier := uint64(1 / MAIN_CACHE_RATIO)

	// initialize pager
	pgr := InitPager(t)
	defer pgr.Close()

	w, err := NewWTinylfu(windowFrameCount, mainItemCount, true)
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %v", err), t)
	}

	t.Cleanup(func() {
		w.close()
	})

	// 1. fill window cache
	for range windowFrameCount {
		f, _, err := w.getFreeFrame(pgr)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected not error,got %v", err), t)
		}
		// unref to make available for eviction
		f.unref()

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
					f, _, err := w.getFreeFrame(pgr)
					if err != nil {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error,got %v", err), t)
					}
					f.unref()

					if seg := f.getSegType(); seg != windowSegment {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
					}

					pageCounter++
				})
			}

			// 3. fill protected segment.
			// Iterate over clock buffer and increment count items in probation.
			itemsPromoted := 0
			itemsIterated := 0
			frIdx := 0

			totCap := w.cBuffer.capacity
			for itemsPromoted < int(probationFrameCount) {
				currFrame := &w.cBuffer.bPool[frIdx%int(w.cBuffer.capacity)]
				if itemsIterated >= int(totCap) {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Iterated over all %d frames but did not fill probation capacity of %d, with total capacity of %d, only managed to fill %d probation items", itemsIterated, probationFrameCount, totCap, itemsPromoted), t)
				}
				if seg := currFrame.getSegType(); seg != probationSegment {
					itemsIterated++
					frIdx++
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
				frIdx++
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("%d_protectedeviction_fillprotected success", i))
		})
	}

	helpers.PrintSuccessMsg("Succesfully filled protected cache. printing stats..")

	// 4. refill probation by adding items to window
	for i := range windowFrameCount {
		t.Run(fmt.Sprintf("protectedeviction_refillprobation_%d", i), func(t *testing.T) {
			en, _, err := w.getFreeFrame(pgr)
			if err != nil {
				t.Fatalf("Expected no error,got %v", err)
			}
			en.unref()

			if seg := en.getSegType(); seg != windowSegment {
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
	var unreferencedProbationFrame *clockentry
	var unreferencedProtectedFrame *clockentry
	probationFrames := make([]*clockentry, 0)
	protectedFrames := make([]*clockentry, 0)
	currEntry := &w.cBuffer.bPool[0]
	reservedWindowFrames := 0
	for i := range w.cBuffer.capacity {
		j := i
		currEntry = &w.cBuffer.bPool[j]
		t.Run(fmt.Sprintf("protectedeviction_pinframes_%d", j), func(t *testing.T) {
			switch currEntry.getSegType() {
			case probationSegment:
				if unreferencedProbationFrame == nil {
					unreferencedProbationFrame = currEntry
					probationFrames = append(probationFrames, currEntry)
				} else {
					currEntry.reference()
					probationFrames = append(probationFrames, currEntry)
					if !currEntry.acc {
						t.Fatalf("probation frame not pinned")
					}
				}
			case protectedSegment:
				if unreferencedProtectedFrame == nil {
					unreferencedProtectedFrame = currEntry
					protectedFrames = append(protectedFrames, currEntry)
					fmt.Printf("5. unrefprotectedframe ref count -> %d\n", unreferencedProtectedFrame.pinCount.Load()-unreferencedProtectedFrame.unpinCount.Load())
				} else {
					currEntry.reference()
					protectedFrames = append(protectedFrames, currEntry)
					if !currEntry.acc {
						t.Fatalf("protected frame not pinned")
					}
				}
			case windowSegment:
				// reference window cache item.
				if reservedWindowFrames == 0 {
					reservedWindowFrames++
					fmt.Println("Reserving window  frame item(not referencing)")
				} else {
					currEntry.reference()
					fmt.Println("protectedeviction window frame ref count after pin: ", currEntry.pinCount.Load()-currEntry.unpinCount.Load())
				}
			default:
				fmt.Println("unassigned, continue")
			}

			helpers.PrintSuccessMsg(fmt.Sprintf("protectedeviction_pinframes_%d success", j))
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
		f, _, err := w.getFreeFrame(pgr)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error,got %v", err), t)
		}

		if seg := f.getSegType(); seg != windowSegment {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected new frame to be in window segment, got %v", seg), t)
		}

		pageCounter++

		helpers.PrintSuccessMsg("protectedeviction_evict_from_probation success")
	})

	// 8. Test adding to full cache with all items pinned
	t.Run("protectedeviction_add_to_full_cache", func(t *testing.T) {
		w.Stat()
		unreferencedProbationFrame.reference()

		if !unreferencedProbationFrame.acc {
			helpers.PrintTestErrorMsg("Unable to reference frame", t)
		}

		unreferencedProtectedFrame.reference()
		if !unreferencedProtectedFrame.acc {
			helpers.PrintTestErrorMsg("Unable to reference frame", t)
		}

		// add item
		_, _, err := w.getFreeFrame(pgr)
		if err == nil {
			helpers.PrintTestErrorMsg("Expected an error,got nil", t)
		}

		pageCounter++

		unreferencedProtectedFrame.unref()
		helpers.PrintSuccessMsg("protectedeviction_add_to_full_cache success")
	})

	// 9. Test adding to not full cache
	t.Run("protectedeviction_add_to_full_cache_with_available_frames", func(t *testing.T) {
		// unreferencedProtectedFrame is now in probation as a result of previous test above
		unreferencedProtectedFrame.unref()
		if unreferencedProtectedFrame.acc {
			t.Fatal("Unable to dereference frame")
		}

		// add item
		w.Stat()
		f, _, err := w.getFreeFrame(pgr)
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
