package buffermanager

import (
	"fmt"
	"math"
	"sync"

	tiny "github.com/oryankibandi/baobab/internal/tinylfu"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/pager"
)

const (
	MAIN_CACHE_RATIO = 0.2
)

// Frames start with no assigned segment, so 0 represents unassigned stage
const (
	unassigned SegmentType = iota
	windowSegment
	probationSegment
	protectedSegment
)

type SegmentType uint32

type WTinyLfu struct {
	// circular buffer
	cBuffer *clock

	// TinyLFU filter
	tinyFilter *tiny.TinyLFU
	// count
	windowCount    uint64
	probationCount uint64
	protectedCount uint64

	// capacity
	windowCapacity    uint64
	probationCapacity uint64
	protectedCapacity uint64

	mu sync.RWMutex
	wg sync.WaitGroup
}

type paddedFramePtr struct {
	en *clockentry
	_  [56]byte
}

// Increments count of an item in TinyLFU.
// If item is in probation, it is promoted to protected. If any cache is full one of it's items is evicted.
// Returns true if operation is successful, else false and the error
func (w *WTinyLfu) Increment(en *clockentry) (bool, error) {
	if en == nil {
		panic("(Increment) clockentry is required")
	}

	cType := en.getSegType()
	if cType == probationSegment {
		// promote to protected.
		err := w.promoteToProtected(en)
		if err != nil {
			return false, err
		}
	} else if cType == unassigned {
		// new item, set as window item
		en.updateSegment(windowSegment)
	}

	k := en.entry.getKey()

	go func(key []byte) {
		err := w.tinyFilter.IncrementItem(key)

		if err != nil {
			panic(err)
		}
	}(toBytes(k))

	return true, nil
}

// Promotes an item from probation to protected
func (w *WTinyLfu) promoteToProtected(entry *clockentry) error {
	// fmt.Println("(promoteToProtected)")
	if entry == nil {
		return BufferManagerError{Message: "entry to promote no provided"}
	}

	cType := entry.segtype

	if cType != probationSegment {
		return WTinyLFUError{Message: "(promoteToProtected) Only frames in probation LRU can be promoted to protected LRU."}
	}

	w.mu.Lock()
	if w.protectedCount < w.protectedCapacity {
		// protected segment not full
		entry.updateSegment(protectedSegment)
		w.probationCount--
		w.protectedCount++
		w.mu.Unlock()
	} else {
		w.mu.Unlock()
		protectedVictim := w.cBuffer.EvictWithoutClearing(protectedSegment)

		if protectedVictim == nil {
			return BufferManagerError{Message: "Could not find eligible victim."}
		}
		defer protectedVictim.unref()

		protectedVictim.updateSegment(probationSegment)
		entry.updateSegment(protectedSegment)
	}

	return nil
}

// Evicts an item from the window cache. This is called when the w-cache is full.
// If main cache is also full, it compares victims from main cache and window cache
// and evicts the one with a lesser count.
func (w *WTinyLfu) evictWindow(pgr *pager.Pager) ([]uint32, error) {
	if w.probationCount > w.probationCapacity {
		panic("probation items surpassed allowed max")
	}

	if w.windowCount < w.windowCapacity {
		return nil, WTinyLFUError{Message: "Window cache is not full"}
	}

	// use padded struct to prevent false sharing
	windVictim := paddedFramePtr{}
	probationVictim := paddedFramePtr{}

	probationFull := w.probationCount >= w.probationCapacity

	if probationFull {
		w.wg.Add(2)
		go func(e **clockentry) {
			*e = w.cBuffer.EvictWithoutClearing(windowSegment)
			w.wg.Done()
		}(&(windVictim.en))

		go func(e **clockentry) {
			*e = w.cBuffer.EvictWithoutClearing(probationSegment)
			w.wg.Done()
		}(&(probationVictim.en))

		w.wg.Wait()
	} else {
		// item can be moved to probation segment without evicting probation
		windVictim.en = w.cBuffer.EvictWithoutClearing(windowSegment)
		if windVictim.en == nil {
			return nil, BufferManagerError{Message: "Unable to find window victim(all frames in use)."}
		}

		windVictim.en.updateSegment(probationSegment)
		w.probationCount++
		w.windowCount--

		windVictim.en.unref()

		// unref
		windVictim.en = nil
		return nil, nil
	}

	if windVictim.en == nil {
		if probationVictim.en != nil {
			probationVictim.en.unref()

			probationVictim.en = nil
		}
		return nil, BufferManagerError{Message: "(probation full) Could not find window victim(all frames in use)."}
	}

	if probationVictim.en == nil {
		if windVictim.en != nil {
			windVictim.en.unref()

			windVictim.en = nil
		}
		return nil, BufferManagerError{Message: "Unable to find probation victim(all frames in use)."}
	}

	// compare counts of window victim and main cache victim
	windKey := windVictim.en.entry.getKey()
	windCount, err := w.tinyFilter.CheckItemCount(toBytes(windKey))
	if err != nil {
		windVictim.en.unref()
		probationVictim.en.unref()

		//unref
		windVictim.en = nil
		probationVictim.en = nil
		return nil, err
	}

	mainKey := probationVictim.en.entry.getKey()
	mainCacheCount, err := w.tinyFilter.CheckItemCount(toBytes(mainKey))
	if err != nil {
		windVictim.en.unref()
		probationVictim.en.unref()

		//unref
		windVictim.en = nil
		probationVictim.en = nil
		return nil, err
	}

	// del keys
	var delKeys []uint32

	if windCount >= mainCacheCount {
		delKeys = append(delKeys, mainKey)

		// flush main cache victim
		buff, err := probationVictim.en.entry.ByteData()
		if err != nil {
			panic("No byte data associated with frame")
		}

		// create a slice around the buff
		frSlice := buff[:]
		err = pgr.WritePage(mainKey, &frSlice, false)
		if err != nil {
			//unref
			windVictim.en = nil
			probationVictim.en = nil
			return nil, err
		}
		// helpers.PrintInfoMsg(fmt.Sprintf("Flushed page %d to be evicted from probation.", mainKey))

		// Add window victim to probation and readd main cache victim to pool
		err = w.cBuffer.clearEntry(probationVictim.en)
		if err != nil {
			panic(err)
		}

		windVictim.en.updateSegment(probationSegment)
		windVictim.en.unref()
		w.windowCount--
	} else {
		delKeys = append(delKeys, windKey)
		// flush window cache victim
		buff, err := windVictim.en.entry.ByteData()
		if err != nil {
			panic("No byte data associated with frame")
		}

		// create a slice around the buff
		frSlice := buff[:]
		err = pgr.WritePage(windKey, &frSlice, false)
		if err != nil {
			//unref
			windVictim.en = nil
			probationVictim.en = nil
			return nil, err
		}
		// helpers.PrintInfoMsg(fmt.Sprintf("Flushed page %d which is to be evicted.", windKey))
		err = w.cBuffer.clearEntry(windVictim.en)
		if err != nil {
			panic(err)
		}

		w.windowCount--
		probationVictim.en.unref()
	}

	//unref
	windVictim.en = nil
	probationVictim.en = nil

	return delKeys, nil
}

// getEmptyFrame returns a free frame from the circular buffer. If no frame
// is available, evict from window cache
//
// Parameters:
//
//	pgr - pager instance required to flush any frames that may be evicted
func (w *WTinyLfu) getFreeFrame(pgr *pager.Pager) (fr *clockentry, evicted []uint32, e error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// check if window is full
	// incase of eviction, IDs of evicted frames are appended to the array
	var keysToEvict []uint32
	var err error

	if w.windowCount > w.windowCapacity {
		panic("window cache overflow.")
	}

	if w.windowCount == w.windowCapacity {
		// fmt.Println("WINDOW CACHE FULL...")
		keysToEvict, err = w.evictWindow(pgr)
		if err != nil {
			return nil, nil, err
		}
	}

	// Get empty frame from cbuffer
	en := w.cBuffer.EvictWithoutClearing(unassigned)
	if en == nil {
		return nil, nil, BufferManagerError{Message: "Unable to add item to WtinyLFU"}
	}

	en.updateSegment(windowSegment)

	w.windowCount++
	en.reference()

	return en, keysToEvict, nil
}

// readdFrameToPool  re-adds a frame to bpool after clearing its content first.
// Returns error if any
func (w *WTinyLfu) readdFrameToPool(entry *clockentry) error {
	err := w.cBuffer.clearEntry(entry)
	if err != nil {
		fmt.Println(helpers.BOLDRED + err.Error() + helpers.RESET)
	}

	return err
}

// Returns the number of frames in the window cache
func (w *WTinyLfu) getWindowCount() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.windowCount
}

// Returns the number of frames in the probation segment
func (w *WTinyLfu) getProbationCount() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.probationCount
}

// Returns the number of frames in the protected segment
func (w *WTinyLfu) getProtectedCount() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.protectedCount
}

func (w *WTinyLfu) getMetadataPage() *Frame {
	return &w.cBuffer.getReserved().entry
}

// List metadata i.e no. of items in all segments
func (w *WTinyLfu) Stat() {
	w.mu.RLock()
	defer w.mu.RUnlock()
	msg := "------------------------------------------------------------------\n"
	msg += fmt.Sprintf("WINDOW COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", w.windowCount, w.windowCount >= w.windowCapacity, w.windowCapacity, (float64(w.windowCount)/float64(w.windowCapacity))*100)
	msg += fmt.Sprintf("PROBATION COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", w.probationCount, w.probationCount >= w.probationCapacity, w.probationCapacity, (float64(w.probationCount)/float64(w.probationCapacity))*100)
	msg += fmt.Sprintf("PROTECTED COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", w.protectedCount, w.protectedCount >= w.protectedCapacity, w.protectedCapacity, (float64(w.protectedCount)/float64(w.protectedCapacity))*100)
	msg += "------------------------------------------------------------------\n"

	fmt.Println(helpers.BOLDWHITE + msg + helpers.RESET)
}

func (w *WTinyLfu) close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.cBuffer.close()
	if err != nil {
		panic(err)
	}
}

// Creates new instance  of W-TinyLFU.
// windowSize and mainCacheSize represents the number of frames for each segment
func NewWTinylfu(windowSize uint64, mainCacheSize uint64, reserveMetadata bool) (*WTinyLfu, error) {
	if windowSize <= 0 {
		return nil, WTinyLFUError{Message: "Window size must be greater than 0"}
	}

	if mainCacheSize <= 0 {
		return nil, WTinyLFUError{Message: "Main cache size must be greater than 0"}
	}

	if windowSize >= mainCacheSize {
		return nil, WTinyLFUError{Message: "Window size is greater than main cache size."}
	}

	probationCap := uint64(math.Round(float64(mainCacheSize) * MAIN_CACHE_RATIO))
	// do not include reserved frame for metadata page
	protectedCap := uint64(math.Round(float64(mainCacheSize)*float64(1.0-MAIN_CACHE_RATIO))) - 1

	cBuff, err := NewClock(windowSize+mainCacheSize, reserveMetadata)
	if err != nil {
		panic(err)
	}

	t, err := tiny.NewTinyLFU()
	if err != nil {
		return nil, err
	}

	w := WTinyLfu{
		cBuffer:           cBuff,
		windowCapacity:    windowSize,
		probationCapacity: probationCap,
		protectedCapacity: protectedCap,
		tinyFilter:        t,
	}

	return &w, nil
}
