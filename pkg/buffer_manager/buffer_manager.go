package buffermanager

import (
	"encoding/binary"
	"errors"
	"fmt"

	//"fmt"
	"log"
	"sync"

	lru "github.com/oryankibandi/on_disk_btree/internal/lrulist"
	tiny "github.com/oryankibandi/on_disk_btree/internal/tinylfu"
	diskio "github.com/oryankibandi/on_disk_btree/pkg/disk_io"
)

// const (
// 	Window CacheType = iota
// 	Probation
// 	Protected
// )

// Ideally, window cache size ≈ 1%, main cache ≈ 99%
const (
	WINDOW_CACHE_SIZE = 200
	MAIN_CACHE_SIZE   = 800
	CACHE_KEY_SIZE    = 16
	MAIN_CACHE_RATIO  = 0.25
)

var BCache *Cache

// type CacheType int

type Cache struct {
	windowCache    *lru.LruList[*lru.Frame]
	probationCache *lru.LruList[*lru.Frame]
	protectedCache *lru.LruList[*lru.Frame]
	CacheMap       map[uint32]*lru.Frame
	tinyFilter     *tiny.TinyLFU
	rmu            sync.RWMutex
	frameCount     uint32
	freeFrames     uint32
}

func init() {
	var err error
	BCache, err = NewCache(WINDOW_CACHE_SIZE, MAIN_CACHE_SIZE)

	if err != nil {
		panic(err)
	}

	log.Println("Initialized Buffer Pool: ", BCache)

	bgWriter := NewBgWriter()

	go bgWriter.Start()

	log.Println("Initialized BgWriter ")
}

// 1. Add item to cache
func (c *Cache) Put(k uint32, v *diskio.Page) (*lru.Frame, error) {
	c.rmu.Lock()
	// formattedKey := toKey(k)
	item := lru.Frame{
		Page: v,
		Key:  k,
	}

	// log.Println("NEW FRAME PREV ====================> ", item.Prev)
	// log.Println("NEW FRAME NEXT ====================>", item.Next)

	// Check if item already exists
	val, ok := c.CacheMap[k]
	if !ok {
		// TODO: Check free frames first, if none evict first
		// First time entry
		log.Println("First Entry, adding to window cache----------------")
		// item.CacheType = lru.Window
		item.UpdateCacheType(lru.Window)

		c.rmu.Unlock()
		n := c.addToWindowCache(k, &item)

		// log.Println("NEW NEW FRAME PREV ====================> ", n.Prev)
		// log.Println("NEW NEW FRAME NEXT ====================>", n.Next)

		// log.Println("Added to window cache.....................")
		c.rmu.Lock()
		// log.Println("Regained lock..........................")
		c.CacheMap[k] = n
		// c.rmu.Unlock()

	} else {
		// data already exists, update, retrieve, and update count
		// item.Type = val.(*CacheItem[T]).Type
		// c.CacheMap.Store(formattedKey, item)
		// update value

		// log.Println("Item Exists, incrementing count........")

		// log.Println("EXISTING FRAME PREV ====================> ", val.Prev)
		// log.Println("EXISTING FRAME NEXT ====================>", val.Next)

		val.UpdatePage(v)
		cType := val.GetCacheType()

		if cType == lru.Probation {
			// Move to protected.
			c.promoteItemFromProbation(val)
		} else if cType == lru.Protected {
			c.protectedCache.SetMostRecent(val)
		} else {
			c.windowCache.SetMostRecent(val)
		}

		// c.rmu.RUnlock()
	}

	// increment count in filter
	log.Println("Incrementing item....")
	go c.tinyFilter.IncrementItem(toBytes(k))
	log.Println("Incremented item....")

	// if err != nil {
	// 	panic(err.Error())
	// }

	log.Println("Printing stats ....")
	c.rmu.Unlock()
	c.Stat()
	return &item, nil
}

// List metadata i.e no. of items in all segments
func (c *Cache) Stat() {
	c.rmu.RLock()
	defer c.rmu.RUnlock()
	log.Println("------------------------------------------------------------------")
	log.Printf("WINDOW COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", c.windowCache.Count, c.windowCache.Full, c.windowCache.Capacity, (float64(c.windowCache.Count)/float64(c.windowCache.Capacity))*100)
	log.Printf("PROBATION COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", c.probationCache.Count, c.probationCache.Full, c.probationCache.Capacity, (float64(c.probationCache.Count)/float64(c.probationCache.Capacity))*100)
	log.Printf("PROTECTED COUNT: %d - FULL %v - CAP %d - OCCUPANCY %.2f%%\n", c.protectedCache.Count, c.protectedCache.Full, c.protectedCache.Capacity, (float64(c.protectedCache.Count)/float64(c.protectedCache.Capacity))*100)
	log.Println("------------------------------------------------------------------")

}

// Add new item to window cache
func (c *Cache) addToWindowCache(key uint32, item *lru.Frame) *lru.Frame {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	// update LRU type
	item.UpdateCacheType(lru.Window)
	if !c.windowCache.Full {
		// Cache not full, add
		node := c.windowCache.Add(item)
		c.frameCount++

		if c.freeFrames > 0 {
			c.freeFrames--
		}

		return node
	} else {
		// cache is full, need to evict
		windowVictim := c.windowCache.Pop()

		if windowVictim == nil {
			msg := fmt.Sprintf("NO ITEM IN WINDOW CACHE.Frame Count => %d", int(c.frameCount))
			fmt.Println()
			panic(msg)
		}

		// if probation not full, add victim to probation, otherwise evict
		if !c.probationCache.Full {
			// Add window victim to main cache
			// windowVictim.CacheType = lru.Probation
			windowVictim.UpdateCacheType(lru.Probation)
			c.probationCache.Add(windowVictim)

			// Add new item to window cache
			n := c.windowCache.Add(item)

			return n
		} else {
			// evict from probation
			mainCacheVictim := c.probationCache.Pop()

			// compare the two, and evict one with lower count
			windCount, err := c.tinyFilter.CheckItemCount(toBytes(key))

			if err != nil {
				panic(err.Error())
			}

			mainCacheCount, err := c.tinyFilter.CheckItemCount(toBytes(mainCacheVictim.Key))

			if err != nil {
				panic(err.Error())
			}

			if windCount > mainCacheCount {
				// admit window victim to main cache and evict & delete main cache victim
				// windowVictim.CacheType = lru.Probation
				windowVictim.UpdateCacheType(lru.Probation)
				c.probationCache.Add(windowVictim)

				log.Println("(Probation) Evicting: ", string(mainCacheVictim.Key))

				// flush page first before eviction
				mainCacheVictim.PrepareForEviction()

				delete(c.CacheMap, mainCacheVictim.Key)
				// c.CacheMap.Delete(mainCacheVictim.Item.Key)
			} else {
				// evict window victim and add new item
				c.probationCache.Add(mainCacheVictim)

				log.Println("(Window) Evicting: ", string(windowVictim.Key))
				windowVictim.PrepareForEviction()
				delete(c.CacheMap, windowVictim.Key)
				// c.CacheMap.Delete(windowVictim.Item.Key)
			}

			n := c.windowCache.Add(item)

			return n
		}

	}
}

// promote item from probation
func (c *Cache) promoteItemFromProbation(candidate *lru.Frame) *lru.Frame {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	log.Println("Promoting to protected: ", string(candidate.Key))
	// 1. Check if protected is full, if not full add to protected
	if !c.protectedCache.Full {
		candidate.UpdateCacheType(lru.Protected)
		// candidate.CacheType = lru.Protected
		n := c.protectedCache.Add(candidate)

		log.Println("(Probation) Evicting: ", string(candidate.Key))
		c.probationCache.Delete(candidate)

		return n
	}
	// 2. If protected is full, compare LRU from protected with candidate
	protectedVictim := c.protectedCache.Tail

	candidateCount, err := c.tinyFilter.CheckItemCount(toBytes(candidate.Key))

	if err != nil {
		panic(err.Error())
	}

	protectedCount, err := c.tinyFilter.CheckItemCount(toBytes(protectedVictim.Key))

	if err != nil {
		panic(err.Error())
	}

	if candidateCount > protectedCount {
		// Demote protected cache victim to probation and add candidate to protected
		p := c.protectedCache.Pop()

		log.Println("(Probation) Evicting: ", string(candidate.Key))
		pr := c.probationCache.Delete(candidate)

		n := c.protectedCache.Add(pr)
		c.probationCache.Add(p)

		// update metadata
		// pr.CacheType = lru.Protected
		pr.UpdateCacheType(lru.Protected)
		// p.CacheType = lru.Probation
		p.UpdateCacheType(lru.Probation)

		return n
	} else {
		// Just update recency of the node in the DLL
		c.probationCache.SetMostRecent(candidate)
		return candidate
	}
}

func (c *Cache) RemoveItemFromLru(f *lru.Frame) error {
	c.rmu.RLock()
	defer c.rmu.RUnlock()
	switch f.GetCacheType() {
	case lru.Window:
		c.windowCache.RemoveFrame(f)
	case lru.Protected:
		c.protectedCache.RemoveFrame(f)
	default:
		c.probationCache.RemoveFrame(f)
	}

	return nil
}

// 2. Retrieve item from cache
func (c *Cache) Get(pageId uint32) (*lru.Frame, error) {
	fmt.Println("(Get) Obtaining lock for cache...")
	c.rmu.RLock()
	fmt.Println("(Get) Obtained lock for cache...")
	// fKey := toKey(key)
	val, ok := c.CacheMap[pageId]

	if ok {
		// increment count
		// log.Println("Value found in cache.... ")
		// log.Println("EXISTING FRAME PREV ====================> ", val.Prev)
		// log.Println("EXISTING FRAME NEXT ====================>", val.Next)

		err := c.tinyFilter.IncrementItem(toBytes(pageId))

		if err != nil {
			return nil, err
		}
		// log.Println("Incremented item in LRU.... ")

		cType := val.GetCacheType()
		// log.Println("CacheType => ", cType)

		if cType == lru.Probation {
			c.rmu.RUnlock()
			c.promoteItemFromProbation(val)
			c.rmu.RLock()
		} else if cType == lru.Protected {
			c.protectedCache.SetMostRecent(val)
		} else {
			c.windowCache.SetMostRecent(val)
		}

		// log.Println("EXISTING FRAME PREV ====================> ", val.Prev)
		// log.Println("EXISTING FRAME NEXT ====================>", val.Next)

		// log.Println("performed the necessary promotion....")

		// pin frame
		val.PinFrame()
		fmt.Println("Pinned Frame.....")

		// remove frame from LRU
		switch val.GetCacheType() {
		case lru.Window:
			c.windowCache.RemoveFrame(val)
		case lru.Protected:
			c.protectedCache.RemoveFrame(val)
		default:
			c.probationCache.RemoveFrame(val)
		}

		fmt.Println("REMOVED FROM LRU...")

		c.rmu.RUnlock()

		return val, nil
	} else {
		// Retrieve item from disk
		ch := make(chan *diskio.Page)
		err := diskio.DiskBTree.ReadReq(uint32(pageId), &ch)

		if err != nil {
			return nil, err
		}

		pge := <-ch

		//  add item  to cache
		c.rmu.RUnlock()
		f, err := c.Put(pageId, pge)

		if err != nil {
			return nil, errors.New("No item found")
		}

		return f, nil
	}
}

// Releases a frame in use by unpinning and reinserting to LRU.
// Called when a thread is done with a frame
func (c *Cache) ReleaseFrame(f *lru.Frame) error {
	fmt.Println("(ReleaseFrame) Obtaining lock....")
	c.rmu.Lock()
	defer c.rmu.Unlock()

	fmt.Println("Unpinning Fram...e")
	addToLru, err := f.UnpinFrame()
	cType := f.GetCacheType()

	if err != nil {
		return err
	}

	fmt.Println("ADD BACK TO LRU ==> ", addToLru)

	if addToLru {
		switch cType {
		case lru.Probation:
			fmt.Println("ADDING TO PROBATION.....")
			c.probationCache.Add(f)
		case lru.Protected:
			fmt.Println("ADDING TO PROTETED.....")
			c.protectedCache.Add(f)
		default:
			fmt.Println("ADDING TO WINDOW.....")
			c.windowCache.Add(f)
		}
	}
	fmt.Println("Unpinned frame.....")

	return nil
}

// Traverses the DLL and prints all keys
//func (c *Cache[K]) Traverse() {
//	if c.windowCache.Count <= 0 {
//		return
//	}
//
//	start := c.windowCache.Head
//
//	log.Println("-------------------------------------------------------------------------------------------")
//	for start != nil {
//		if start.Next != nil {
//			fmt.Printf("%s -> ", string(start.Item.Key[:]))
//		} else {
//			fmt.Printf("%s \n", string(start.Item.Key[:]))
//		}
//
//		start = start.Next
//	}
//	log.Println("-------------------------------------------------------------------------------------------")
//}

// Delete an item from cache. flush parameter is set to true if it's a direct request from client. If bgwriter, it is false since the page is already flushed.
func (c *Cache) Delete(key uint32, flush bool) error {
	// c.rmu.Lock()
	// defer c.rmu.Unlock()
	// fKey := toKey(key)
	val, ok := c.CacheMap[key]

	if !ok {
		return errors.New("No key in cache")
	}

	// fVal := val.(*lru.LRUNode[CacheItem[K]])

	cType := val.GetCacheType()

	switch cType {
	case lru.Protected:
		log.Println("(Protected) Evicting: ", string(val.Key))
		c.protectedCache.Delete(val)
	case lru.Probation:
		log.Println("(Probation) Evicting: ", string(val.Key))
		c.probationCache.Delete(val)
	default:
		log.Println("(Window) Evicting: ", string(val.Key))
		c.windowCache.Delete(val)
	}

	if flush {
		val.PrepareForEviction()
	}
	delete(c.CacheMap, key)

	c.freeFrames++

	if c.frameCount > 0 {
		c.frameCount++
	}
	// c.CacheMap.Delete(toBytes(key))

	return nil
}

// Create new cache instance\n windowSize, probationSize and protectedSize are sized of the individual segments
func NewCache(windowSize uint64, mainCacheSize uint64) (*Cache, error) {
	if windowSize <= 0 {
		return nil, errors.New("Window size must be greater than 0")
	}

	if mainCacheSize <= 0 {
		return nil, errors.New("Main cache size must be greater than 0")
	}

	n := Cache{
		windowCache:    lru.NewLRUList[*lru.Frame](windowSize),
		probationCache: lru.NewLRUList[*lru.Frame](uint64(MAIN_CACHE_RATIO * float64(mainCacheSize))),
		protectedCache: lru.NewLRUList[*lru.Frame](uint64((1 - MAIN_CACHE_RATIO) * float64(mainCacheSize))),
		tinyFilter:     tiny.New(),
		CacheMap:       make(map[uint32]*lru.Frame),
	}

	return &n, nil
}

func toBytes(key uint32) []byte {
	b := make([]byte, 4)

	binary.LittleEndian.PutUint32(b, key)

	return b
}

// Converts p to fixed size []byte
//func toKey(p []byte) KeyType {
//	n := make([]byte, 16)
//
//	n = append(append(n[:0], p...), n[len(p):]...)
//	//	fmt.Println("APPENDED => ", n)
//
//	return KeyType(n)
//}
