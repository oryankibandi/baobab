package buffermanager

import (
	"fmt"
	"log"
	"sync"

	tiny "github.com/oryankibandi/baobab/internal/tinylfu"
	"github.com/oryankibandi/baobab/pkg/diskmanager"
)

const (
	windowSegment SegmentType = iota
	probationSegment
	protectedSegment
)

type SegmentType uint64

type WTinyLfu struct {
	cBuffer    *clock
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

// Increments count of an item in TinyLFU.
// If item is in probation, it is promoted to protected. If any cache is full one of it's items is evicted.
// Returns true if operation is successful, else false and the error
func (w *WTinyLfu) Increment(f *Frame) (bool, error) {
	if f == nil {
		panic("(Increment) frame is required")
	}

	cType := f.getSegType()

	if cType == probationSegment {
		// promote to protected.
		err := w.promoteToProtected(f)
		if err != nil {
			return false, err
		}
	}

	k := f.getKey()

	go func(key []byte) {
		err := w.tinyFilter.IncrementItem(key)

		if err != nil {
			panic(err)
		}
	}(toBytes(k))

	return true, nil
}

// Promotes an item from probation to protected
func (w *WTinyLfu) promoteToProtected(f *Frame) error {
	if f == nil {
		return BufferManagerError{Message: "frame to promote no provided"}
	}

	fmt.Println("wtinylfu.PromoteToProtected()")
	cType := f.getSegType()

	if cType != probationSegment {
		return WTinyLFUError{Message: "(promoteToProtected) Only frames in probation LRU can be promoted to protected LRU."}
	}

	if w.protectedCount < w.protectedCapacity {
		// protected segment not full
		f.updateSegment(protectedSegment)
		w.probationCount--
		w.protectedCount++
	} else {
		protectedVictim := w.cBuffer.EvictWithoutClearing(protectedSegment)

		if protectedVictim == nil {
			return BufferManagerError{Message: "Could not find eligible victim."}
		}

		protectedVictim.updateSegment(probationSegment)
		f.updateSegment(protectedSegment)
	}

	return nil
}

// Evicts an item from the window cache. This is called when the w-cache is full.
// If main cache is full, compares victims from main cache and window cache
// and evicts the one  with a lesser count.
func (w *WTinyLfu) evictWindow() ([]uint32, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var windVictim *Frame
	var probationVictim *Frame

	probationFull := w.probationCount < w.probationCapacity
	fmt.Println("w.evictWindow()")
	fmt.Println("EVICTING WINDOW CACHE...")
	if w.windowCount < w.windowCapacity {
		log.Printf("(evictWindow) Window Cache Is Not Full: COUNT -> %d\t CAPACITY -> %d\n", w.windowCount, w.windowCapacity)
		return nil, WTinyLFUError{Message: "Window cache is not full"}
	}

	if probationFull {
		w.wg.Add(2)
		go func(e *Frame) {
			e = w.cBuffer.EvictWithoutClearing(windowSegment)
			w.wg.Done()
		}(windVictim)

		go func(e *Frame) {
			e = w.cBuffer.EvictWithoutClearing(probationSegment)
			w.wg.Done()
		}(probationVictim)

		w.wg.Wait()
	} else {
		// item can be moved to probation segment
		windVictim = w.cBuffer.EvictWithoutClearing(windowSegment)

		windVictim.updateSegment(probationSegment)
		w.probationCount++
		w.windowCount++

		return nil, nil
	}

	if windVictim == nil {
		return nil, BufferManagerError{Message: "Unable to find window victim(all frames in use)."}
	}

	if probationVictim == nil && probationFull {
		return nil, BufferManagerError{Message: "Unable to find probation victim(all frames in use)."}
	}

	// compare counts of window victim and main cache victim
	windKey := windVictim.getKey()
	windCount, err := w.tinyFilter.CheckItemCount(toBytes(windKey))

	if err != nil {
		return nil, err
	}

	mainKey := probationVictim.getKey()
	mainCacheCount, err := w.tinyFilter.CheckItemCount(toBytes(mainKey))

	if err != nil {
		return nil, err
	}

	// del keys
	var delKeys []uint32

	if windCount > mainCacheCount {
		delKeys = append(delKeys, mainKey)

		// Add window victim to probation
		err = w.cBuffer.addToBpool(probationVictim)
		if err != nil {
			panic(err)
		}

		windVictim.updateSegment(probationSegment)
		w.windowCount--
	} else {
		err = w.cBuffer.addToBpool(windVictim)
		if err != nil {
			panic(err)
		}

		delKeys = append(delKeys, windKey)
		w.windowCount--
	}

	return delKeys, nil
}

// Adds a new item to Cache.
// By default, all new items are added to the window cache.
// If window segment is full, an item is evicted or added to main cache
// Returns a slice of uint32 keys that have been evicted and error if any
func (w *WTinyLfu) AddItem(p *diskmanager.Page, isDirty bool) (entry *Frame, evictedKIds []uint32, errr error) {
	if p == nil {
		panic("(AddItem)frame is required")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// keys that might be evicted if window is full
	var evictKeys []uint32
	var err error
	if w.windowCount == w.windowCapacity {
		fmt.Println("(cache.AddItem()) WindowCache Is Full, evicting...")
		// evict window cache first
		evictKeys, err = w.evictWindow()

		if err != nil {
			panic(err)
		}
	}

	// retrieve empty entry slot and add new page data
	e := w.cBuffer.Pop()
	if e == nil {
		return nil, nil, BufferManagerError{Message: "Unable to add item to WtinyLFU"}
	}

	e.updateSegment(windowSegment)

	err = e.SetData(p)
	if err != nil {
		panic(err)
	}

	if isDirty {
		e.MarkDirty()
	}

	return e, evictKeys, nil
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

	fmt.Println(msg)
}

func (w *WTinyLfu) close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.cBuffer.close()
	if err != nil {
		panic(err)
	}
}

func NewWTinylfu(windowSize uint64, mainCacheSize uint64) (*WTinyLfu, error) {
	if windowSize <= 0 {
		return nil, WTinyLFUError{Message: "Window size must be greater than 0"}
	}
	if mainCacheSize <= 0 {
		return nil, WTinyLFUError{Message: "Main cache size must be greater than 0"}
	}

	cBuff, err := NewClock(windowSize + mainCacheSize)
	if err != nil {
		panic(err)
	}

	w := WTinyLfu{
		cBuffer:        cBuff,
		windowCount:    windowSize,
		probationCount: uint64(float64(mainCacheSize) * float64(0.2)),
		protectedCount: uint64(float64(mainCacheSize) * float64(0.8)),
		tinyFilter:     tiny.New(),
	}

	return &w, nil
}
