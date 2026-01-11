package buffermanager

import (
	"fmt"
	"log"

	tiny "github.com/oryankibandi/baobab/internal/tinylfu"
)

type WTinyLfu struct {
	windowCache    *Lru
	probationCache *Lru
	protectedCache *Lru
	tinyFilter     *tiny.TinyLFU
}

// Increments count of an item in TinyLFU. If item is in probation, it is promoted to protected. If any cache is full one of it's items is evicted.
func (w *WTinyLfu) Increment(f *Frame) (bool, error) {
	if f == nil {
		panic("(Increment) frame is required")
	}

	cType := f.GetCacheType()

	switch cType {
	case Probation:
		// promote to protected.
		err := w.promoteToProtected(f)

		if err != nil {
			panic(err)
		}
	case Protected:
		w.protectedCache.SetMostRecent(f)
	default:
		w.windowCache.SetMostRecent(f)
	}

	k := f.GetKey()

	go func(key []byte) {
		err := w.tinyFilter.IncrementItem(key)

		if err != nil {
			panic(err)
		}
	}(toBytes(k))

	return false, nil
}

// Increments count from a Get request.
// A Get request is different in that a frame will be pinned(removed from LRU)
// hence there is no need to move it to head of LRU as it will be readded during
// unpinning.
func (w *WTinyLfu) GetIncrement(f *Frame) error {
	cType := f.GetCacheType()

	if cType == Probation {
		// promote to protected.
		err := w.promoteToProtected(f)

		if err != nil {
			panic(err)
		}
	}

	k := f.GetKey()

	// Increment count in TinyLFU
	go func(key []byte) {
		err := w.tinyFilter.IncrementItem(key)

		if err != nil {
			panic(err)
		}
	}(toBytes(k))

	w.handlePinFrame(f)

	fmt.Println("REMOVED FROM LRU...")

	return nil
}

// Removes frame from the respective LRU list. Called during pinning.
func (w *WTinyLfu) handlePinFrame(f *Frame) error {
	cType := f.GetCacheType()

	switch cType {
	case Window:
		w.windowCache.RemoveFrame(f)
	case Protected:
		w.protectedCache.RemoveFrame(f)
	case Probation:
		w.probationCache.RemoveFrame(f)
	default:
		log.Println("No LRU associated with frame.")
	}
	fmt.Println("(handlePinFrame) DONE.")

	return nil
}

// Promotes an item from probation to protected
func (w *WTinyLfu) promoteToProtected(f *Frame) error {
	cType := f.GetCacheType()

	if cType != Probation {
		return WTinyLFUError{Message: "(promoteToProtected) Only frames in probation LRU can be promoted to protected LRU."}
	}

	// remove from probationCache and reset pointers
	w.probationCache.Delete(f)
	f.setPtrs(nil, nil)

	if w.protectedCache.IsFull() {
		// remove LRU item from protected
		protVictim := w.protectedCache.Pop()

		// add protected segment victim to probation
		protVictim.UpdateCacheType(Probation)
		w.probationCache.Add(protVictim)

		// add candidate to protected
		f.UpdateCacheType(Protected)
		w.protectedCache.Add(f)
	} else {
		// add candidate to protected
		f.UpdateCacheType(Protected)
		w.protectedCache.Add(f)
	}

	return nil
}

// Evicts an item from the window cache. This is called when the w-cache is full.
func (w *WTinyLfu) evictWindow() ([]uint32, error) {
	fmt.Println("w.evictWindow()")
	fmt.Println("EVICTING WINDOW CACHE...")
	if !w.windowCache.IsFull() {
		log.Printf("(evictWindow) Window Cache Is Not Full: COUNT -> %d\t CAPACITY -> %d\n", w.windowCache.GetCount(), w.windowCache.GetCapacity())
		return nil, WTinyLFUError{Message: "Window cache is not full"}
	}

	fmt.Println("Pop() tail  item in window cache....")
	windVictim := w.windowCache.Pop()
	fmt.Println("(evictWindow) WindowVictim -> ", windVictim)

	if windVictim == nil {
		// all items pinned, borrow frame
		err := w.windowCache.borrow()

		if err != nil {
			panic(err)
		}

		return nil, nil
	}
	fmt.Println("(w.evictWindow()) windVictim not nil, evicting...")

	// If main cache not full, send window victim to main cache
	if !w.probationCache.IsFull() {
		// add window victim to main cache
		w.probationCache.Add(windVictim)

		return nil, nil
	}

	mainCacheVictim := w.probationCache.Pop()

	if mainCacheVictim == nil {
		panic("No main victim in probation segment")
	}

	// compare counts of window victim and main cache victim
	windKey := windVictim.GetKey()
	windCount, err := w.tinyFilter.CheckItemCount(toBytes(windKey))

	if err != nil {
		return nil, err
	}

	mainKey := mainCacheVictim.GetKey()
	mainCacheCount, err := w.tinyFilter.CheckItemCount(toBytes(mainKey))

	if err != nil {
		return nil, err
	}

	// del keys
	var delKeys []uint32

	if windCount > mainCacheCount {
		// evict & forget main cache item
		// err = c.Delete(mainCacheVictim.GetKey(), false)

		// if err != nil {
		// 	panic(err)
		// }
		delKeys = append(delKeys, mainKey)

		// Add window victim to probation
		windVictim.UpdateCacheType(Probation)
		w.probationCache.Add(windVictim)
	} else {
		// evict and forget window cache item
		//	err = c.Delete(windVictim.GetKey(), false)

		//	if err != nil {
		//		panic(err)
		//	}

		delKeys = append(delKeys, windKey)
	}

	return delKeys, nil
}

// Evict an item from main cache(probation). Called when protected is full.
func (w *WTinyLfu) evictMainCache() error {
	if !w.protectedCache.IsFull() {
		return WTinyLFUError{Message: "protected cache is not full"}
	}

	if w.probationCache.IsFull() {
		// evict item from probation if full.
		w.probationCache.Pop()
	}

	protVictim := w.protectedCache.Pop()

	if protVictim == nil {
		panic("No protected victim.")
	}

	// add victim of protected to probation
	w.probationCache.Add(protVictim)

	return nil
}

//
// func (w *WTinyLfu) RemoveItemFromLru(f *Frame) error {
// 	if f == nil {
// 		panic("(RemoveItemFromLru)frame is required")
// 	}
//
// 	cType := f.GetCacheType()
//
// 	switch cType {
// 	case Window:
// 		w.windowCache.RemoveFrame(f)
// 	case Protected:
// 		w.protectedCache.RemoveFrame(f)
// 	default:
// 		w.probationCache.RemoveFrame(f)
// 	}
//
// 	return nil
// }

// Adds a new item to Cache.
// By default, all new items are added to the window cache.
// If window segment is full, an item is evicted or added to main cache
// Returns a slice of uint32 keys that have been evicted and error if any
func (w *WTinyLfu) AddItem(f *Frame) ([]uint32, error) {
	if f == nil {
		panic("(AddItem)frame is required")
	}

	f.UpdateCacheType(Window)

	// keys that might be evicted if window is full
	var evictKeys []uint32
	var err error
	if w.windowCache.IsFull() {
		fmt.Println("(cache.AddItem()) WindowCache Is Full, evicting...")
		// evict window cache first
		evictKeys, err = w.evictWindow()

		if err != nil {
			panic(err)
		}
	}

	w.windowCache.Add(f)

	return evictKeys, nil
}

func (w *WTinyLfu) deleteFromLru(f *Frame) error {
	cType := f.GetCacheType()

	switch cType {
	case Protected:
		fmt.Println("(Protected) Evicting... ")
		w.protectedCache.Delete(f)
	case Probation:
		fmt.Println("(Probation) Evicting: ")
		w.probationCache.Delete(f)
	default:
		fmt.Println("(Window) Evicting: ")
		w.windowCache.Delete(f)
	}

	return nil
}

// Readds frame `f` to LRU. Called during unpinning.
// If the lru list the frame is a part of has borrowed frames,
// decrement borrowed frames return del=true.
// `flushed` indicates whether the frame has already been flushed, in
// which case there is no need to return borrowed frames and evict the
// window segment. This is true when the requests comes from the background
// writer.
func (w *WTinyLfu) reAddToLru(f *Frame, flushed bool) (del bool, deletedKeys []uint32, err error) {
	cType := f.GetCacheType()

	switch cType {
	case Probation:
		fmt.Println("READDING TO PROBATION.....")
		// err = c.probationCache.ReAddFrame(f)
		w.probationCache.ReAddFrame(f)
	case Protected:
		fmt.Println("READDING TO PROTECTED -> ", f)
		// err = c.protectedCache.ReAddFrame(f)
		w.protectedCache.ReAddFrame(f)
		fmt.Println("READDED TO PROTECTED.....")
	default:
		fmt.Println("READDING TO WINDOW.....")
		// Check if has borrowed frames
		if w.windowCache.hasBorrowedFrames() && !flushed {
			fmt.Println("HAS BORROWED FRAMES")
			// do not readd
			err := w.windowCache.returnBorrowedFrame()

			if err != nil {
				panic(err)
			}

			// ReAdd item to head of lru
			w.windowCache.ReAddFrame(f)
			// 1f full, evict an item
			if w.windowCache.IsFull() {
				delKeys, err := w.evictWindow()

				if err != nil {
					return false, nil, err
				}

				return true, delKeys, nil
			}

			return false, nil, nil
		}

		w.windowCache.ReAddFrame(f)
	}

	return false, nil, nil
}

// List metadata i.e no. of items in all segments
func (w *WTinyLfu) Stat() {
	winCount := w.windowCache.GetCount()
	probCount := w.probationCache.GetCount()
	protCount := w.protectedCache.GetCount()

	winCap := w.windowCache.GetCapacity()
	probCap := w.probationCache.GetCapacity()
	protCap := w.protectedCache.GetCapacity()

	msg := "------------------------------------------------------------------\n"
	msg += fmt.Sprintf("WINDOW COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", winCount, w.windowCache.IsFull(), winCap, (float64(winCount)/float64(winCap))*100)
	msg += fmt.Sprintf("PROBATION COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", probCount, w.probationCache.IsFull(), probCap, (float64(probCount)/float64(probCap))*100)
	msg += fmt.Sprintf("PROTECTED COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", protCount, w.protectedCache.IsFull(), protCap, (float64(protCount)/float64(protCap))*100)
	msg += fmt.Sprintf("------------------------------------------------------------------\n")

	fmt.Println(msg)
}

func NewWTinylfu(windowSize uint64, mainCacheSize uint64) (*WTinyLfu, error) {
	if windowSize <= 0 {
		return nil, WTinyLFUError{Message: "Window size must be greater than 0"}
	}

	if mainCacheSize <= 0 {
		return nil, WTinyLFUError{Message: "Main cache size must be greater than 0"}
	}

	w := WTinyLfu{
		windowCache:    NewLRU(windowSize, "window"),
		probationCache: NewLRU(uint64(MAIN_CACHE_RATIO*float64(mainCacheSize)), "probation"),
		protectedCache: NewLRU(uint64((1-MAIN_CACHE_RATIO)*float64(mainCacheSize)), "protected"),
		tinyFilter:     tiny.New(),
	}

	return &w, nil
}
