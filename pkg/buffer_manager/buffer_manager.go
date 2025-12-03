package buffermanager

import (
	"encoding/binary"
	"errors"
	"fmt"

	//"fmt"
	"log"
	"sync"

	diskmanager "github.com/oryankibandi/baobab/pkg/disk_io"
	"github.com/oryankibandi/baobab/pkg/wal"
)

// Ideally, window cache size ≈ 1%, main cache ≈ 99%
const (
	WINDOW_CACHE_SIZE = 20
	MAIN_CACHE_SIZE   = 800
	CACHE_KEY_SIZE    = 16
	MAIN_CACHE_RATIO  = 0.25
)

// var BCache *Cache

// type CacheType int

type Cache struct {
	CacheMap    map[uint32]*Frame
	wTinyLfu    *WTinyLfu
	rmu         sync.RWMutex
	frameCount  uint32
	freeFrames  uint32
	diryList    *dPages
	diskManager *diskmanager.DiskManager
}

// Adds a page to cache, setting k as key. k is the unique page ID.
func (c *Cache) put(k uint32, v *diskmanager.Page, dirty bool) (*Frame, error) {
	c.rmu.Lock()
	// formattedKey := toKey(k)
	// internal page
	isInternal, err := v.IsInternal()

	if err != nil {
		return nil, err
	}

	item := Frame{
		page:       v,
		Key:        k,
		IsInternal: isInternal,
	}

	if dirty {
		err = c.MarkFrameDirty(&item)

		if err != nil {
			log.Panic(err)
		}

		// item.MarkDirty()
	}

	// log.Println("NEW FRAME PREV ====================> ", item.Prev)
	// log.Println("NEW FRAME NEXT ====================>", item.Next)

	// Check if item already exists
	val, ok := c.CacheMap[k]
	if !ok {
		// TODO: Check free frames first, if none evict first
		// First time entry
		log.Println("First Entry, adding to window cache----------------")
		item.UpdateCacheType(Window)

		c.CacheMap[k] = &item

		// add to lru
		err := c.wTinyLfu.AddItem(c, &item)

		if err != nil {
			panic(err)
		}
	} else {
		// update value
		log.Println("Item Exists, incrementing count........")
		log.Println("EXISTING FRAME PREV ====================> ", val.prev)
		log.Println("EXISTING FRAME NEXT ====================>", val.next)

		val.UpdatePage(v)

		// increment count
		go c.wTinyLfu.Increment(&item)
	}

	// if err != nil {
	// 	panic(err.Error())
	// }

	log.Println("Printing stats ....")
	c.rmu.Unlock()
	go c.wTinyLfu.Stat()
	return &item, nil
}

// Add new item to window cache
//
//	func (c *Cache) addToWindowCache(key uint32, item *Frame) *Frame {
//		c.rmu.Lock()
//		defer c.rmu.Unlock()
//		// update LRU type
//		item.UpdateCacheType(lru.Window)
//		if !c.windowCache.IsFull() {
//			// Cache not full, add
//			node := c.windowCache.Add(item)
//			c.frameCount++
//
//			if c.freeFrames > 0 {
//				c.freeFrames--
//			}
//
//			return node
//		} else {
//			// cache is full, need to evict
//			windowVictim := c.windowCache.Pop()
//
//			if windowVictim == nil {
//				msg := fmt.Sprintf("NO ITEM IN WINDOW CACHE.Frame Count => %d", int(c.frameCount))
//				fmt.Println()
//				panic(msg)
//			}
//
//			// if probation not full, add victim to probation, otherwise evict
//			if !c.probationCache.IsFull() {
//				// Add window victim to main cache
//				// windowVictim.CacheType = lru.Probation
//				windowVictim.UpdateCacheType(lru.Probation)
//				c.probationCache.Add(windowVictim)
//
//				// Add new item to window cache
//				n := c.windowCache.Add(item)
//
//				return n
//			} else {
//				// evict from probation
//				mainCacheVictim := c.probationCache.Pop()
//
//				// compare the two, and evict one with lower count
//				windCount, err := c.tinyFilter.CheckItemCount(toBytes(key))
//
//				if err != nil {
//					panic(err.Error())
//				}
//
//				mainCacheCount, err := c.tinyFilter.CheckItemCount(toBytes(mainCacheVictim.Key))
//
//				if err != nil {
//					panic(err.Error())
//				}
//
//				if windCount > mainCacheCount {
//					// admit window victim to main cache and evict & delete main cache victim
//					// windowVictim.CacheType = lru.Probation
//					windowVictim.UpdateCacheType(lru.Probation)
//					c.probationCache.Add(windowVictim)
//
//					log.Println("(Probation) Evicting: ", string(mainCacheVictim.Key))
//
//					// flush page first before eviction
//					mainCacheVictim.PrepareForEviction()
//
//					delete(c.CacheMap, mainCacheVictim.Key)
//					// c.CacheMap.Delete(mainCacheVictim.Item.Key)
//				} else {
//					// evict window victim and add new item
//					c.probationCache.Add(mainCacheVictim)
//
//					log.Println("(Window) Evicting: ", string(windowVictim.Key))
//					windowVictim.PrepareForEviction()
//					delete(c.CacheMap, windowVictim.Key)
//					// c.CacheMap.Delete(windowVictim.Item.Key)
//				}
//
//				n := c.windowCache.Add(item)
//
//				return n
//			}
//		}
//	}
//
// promote item from probation
//
//	func (c *Cache) promoteItemFromProbation(candidate *Frame) *Frame {
//		// c.rmu.Lock()
//		// defer c.rmu.Unlock()
//		log.Println("Promoting to protected: ", string(candidate.Key))
//		// 1. Check if protected is full, if not full add to protected
//		if !c.protectedCache.IsFull() {
//			log.Println("Protected cache is not full...")
//			candidate.UpdateCacheType(lru.Protected)
//			log.Println("Updated cache type...")
//			// candidate.CacheType = lru.Protected
//			log.Println("Adding to protected cache...")
//			n := c.protectedCache.Add(candidate)
//
//			log.Println("(Probation) Evicting: ", string(candidate.Key))
//			c.probationCache.Delete(candidate)
//
//			return n
//		}
//		log.Println("Protected is full, evicting...")
//		// 2. If protected is full, compare LRU from protected with candidate
//		// protectedVictim := c.protectedCache.Tail // TODO: Ensure this is accessed in a concurrency safe manner
//		protectedTailKey := c.protectedCache.GetTailKey()
//		candidateKey := candidate.GetKey()
//
//		candidateCount, err := c.tinyFilter.CheckItemCount(toBytes(candidateKey))
//
//		if err != nil {
//			panic(err.Error())
//		}
//
//		protectedCount, err := c.tinyFilter.CheckItemCount(toBytes(protectedTailKey))
//
//		if err != nil {
//			panic(err.Error())
//		}
//
//		if candidateCount > protectedCount {
//			// Demote protected cache victim to probation and add candidate to protected
//			p := c.protectedCache.Pop()
//
//			log.Println("(Probation) Evicting: ", string(candidateKey))
//			pr := c.probationCache.Delete(candidate)
//
//			n := c.protectedCache.Add(pr)
//			c.probationCache.Add(p)
//
//			// update metadata
//			// pr.CacheType = lru.Protected
//			pr.UpdateCacheType(lru.Protected)
//			// p.CacheType = lru.Probation
//			p.UpdateCacheType(lru.Probation)
//
//			return n
//		} else {
//			// Just update recency of the node in the DLL
//			c.probationCache.SetMostRecent(candidate)
//			return candidate
//		}
//	}
func (c *Cache) RemoveItemFromLru(f *Frame) error {
	err := c.wTinyLfu.handlePinFrame(f)

	if err != nil {
		return err
	}

	return nil
}

// 2. Retrieve item from cache
func (c *Cache) Get(pageId uint32) (*Frame, error) {
	fmt.Println("(Get) Obtaining lock for cache...")
	c.rmu.RLock()
	fmt.Println("(Get) Obtained lock for cache...")
	// fKey := toKey(key)
	val, ok := c.CacheMap[pageId]

	if ok {
		// increment count & pin frame
		c.wTinyLfu.GetIncrement(val)
		c.rmu.RUnlock()

		return val, nil
	} else {
		// Retrieve item from disk
		ch := make(chan *diskmanager.Page)
		err := c.diskManager.ReadReq(uint32(pageId), &ch)

		if err != nil {
			return nil, err
		}

		pge := <-ch

		if pge == nil {
			return nil, BufferManagerError{Message: "Page not found"}
		}

		//  add item  to cache
		c.rmu.RUnlock()
		f, err := c.put(pageId, pge, false)

		if err != nil {
			return nil, errors.New("No item found")
		}

		fmt.Println("(Get) Frame not found, read from disk...")

		// remove frame from LRU & Pin
		err = c.wTinyLfu.handlePinFrame(f)

		if err != nil {
			panic(err)
		}

		return f, nil
	}
}

// Releases a frame in use by unpinning and reinserting to LRU.
// Called when a thread is done with a frame
func (c *Cache) ReleaseFrame(f *Frame) error {
	addToLru, err := f.UnpinFrame()

	if err != nil {
		return err
	}

	if addToLru {
		err = c.wTinyLfu.reAddToLru(f)
		if err != nil {
			return err
		}
	}

	return nil
}

// Releases a frame in use by unpinning and reinserting to LRU.
// Called when a thread is done with a frame
// func (c *Cache) ReleaseFrame(f *Frame) error {
// 	fmt.Println("(ReleaseFrame) Obtaining lock....")
// 	c.rmu.Lock()
// 	defer c.rmu.Unlock()
//
// 	fmt.Println("Unpinning Fram...e")
// 	addToLru, err := f.UnpinFrame()
// 	cType := f.GetCacheType()
//
// 	if err != nil {
// 		return err
// 	}
//
// 	fmt.Println("ADD BACK TO LRU ==> ", addToLru)
//
// 	if addToLru {
// 		switch cType {
// 		case lru.Probation:
// 			fmt.Println("ADDING TO PROBATION.....")
// 			// err = c.probationCache.ReAddFrame(f)
// 			c.probationCache.Add(f)
// 		case lru.Protected:
// 			fmt.Println("ADDING TO PROTETED.....")
// 			// err = c.protectedCache.ReAddFrame(f)
// 			c.protectedCache.Add(f)
// 			fmt.Println("ADDED TO PROTECTED.....")
// 		default:
// 			fmt.Println("ADDING TO WINDOW.....")
// 			// err = c.windowCache.ReAddFrame(f)
// 			c.windowCache.Add(f)
// 		}
// 	}
//
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Println("Unpinned frame.....")
//
// 	return nil
// }

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
		return BufferManagerError{Message: "No key in cache"}
	}

	// fVal := val.(*lru.LRUNode[CacheItem[K]])

	err := c.wTinyLfu.deleteFromLru(val)

	if err != nil {
		return err
	}

	if flush {
		err = c.prepareForEviction(val)

		if err != nil {
			panic(err)
		}
	}

	delete(c.CacheMap, key)

	c.freeFrames++

	if c.frameCount > 0 {
		c.frameCount++
	}

	return nil
}

func (c *Cache) GetRootPageId() uint32 {
	return c.diskManager.CheckRootPageId()
}

// marks a frame as dirty and adds it to dirty list LRU
func (c *Cache) MarkFrameDirty(f *Frame) error {
	f.markDirty()
	c.diryList.addDirtyFrame(f)

	return nil
}

// Creates a new frame and assigns page ID to frame with provided keys and values/child
// SetAsRoot parameter ensures to set
func (c *Cache) CreateNewEntry(lsn []byte, keys [][]byte, values *([][]byte), childPageIds *[]int32, setAsRoot bool) (*Frame, error) {
	// create page
	pgeId, pge, err := c.diskManager.NewPage(lsn, keys, values, childPageIds, setAsRoot)

	if err != nil {
		return nil, err
	}

	if setAsRoot {
		if pgeId == 0 {
			// invalid page id
			panic(fmt.Errorf("Invalid page id to set as root: %v", pgeId))
		}

		// set  new page as root
		err = c.diskManager.SetAsRoot(pgeId)

		if err != nil {
			return nil, err
		}
	}

	// Add to buffer
	log.Println("Adding new page to cache...")
	f, err := c.put(uint32(pge.Header.PageId), pge, true)
	log.Println("Added new page to cache...")

	return f, nil
}

// Calls ForceFlush on disk manager
func (c *Cache) flushWritten() {
	c.diskManager.ForceFlush()
}

// Prepares page for eviction by flushing page to disk
func (c *Cache) prepareForEviction(f *Frame) error {
	log.Println("lru.PrepareForEviction()")
	f.mu.Lock()
	f.mu.Unlock()
	ch := make(chan int32)
	lsnChan := make(chan []byte)
	err := c.diskManager.WriteReq(f.page, &ch, &lsnChan)

	if err != nil {
		panic(err.Error())
	}

	n := <-ch

	log.Printf("(PrepareForEviction) Flushed %d page\n", n)

	return nil
}

// sets the frame's page ID as the new root on the disk manager
func (c *Cache) SetNewRoot(f *Frame) error {
	pId := f.GetKey()

	err := c.diskManager.SetAsRoot(int32(pId))

	if err != nil {
		return err
	}

	return nil
}

// Syncs contents to the frame's associated page.
// Called when the materialized node has been updated
// through DELETEs, PUTs, Merges or Splits
func (c *Cache) SyncFrame(f *Frame, lsn []byte, keys [][]byte, vals [][]byte, pageIds []int32, rightSibling uint32, leftSibling uint32) error {
	if len(lsn) != diskmanager.LSN_SIZE_BYTE {
		panic(fmt.Errorf("Invalid LSN length. Got length %d, expected length %d", len(lsn), diskmanager.LSN_SIZE_BYTE))
	}
	f.mu.Lock()
	if f.page == nil {
		f.mu.Unlock()
		return BufferManagerError{Message: "No page associated with frame"}
	}

	err := f.page.Sync(lsn, keys, vals, pageIds, rightSibling, leftSibling)
	fmt.Println("DONE SYNCING...")

	if err != nil {
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()

	// mark frame as dirty
	// f.MarkDirty()
	fmt.Println("MARKING FRAMS AS DIRTY...")
	err = c.MarkFrameDirty(f)
	fmt.Println("MARKED FRAME AS DIRTY...")

	if err != nil {
		return err
	}

	return nil
}

// Safely close down cache
func (c *Cache) Close() error {
	c.diskManager.Close()

	return nil
}

// Create new cache instance\n windowSize, probationSize and protectedSize are sized of the individual segments
func NewCache(windowSize uint64, mainCacheSize uint64, wal *wal.WAL) (*Cache, error) {
	if windowSize <= 0 {
		return nil, errors.New("Window size must be greater than 0")
	}

	if mainCacheSize <= 0 {
		return nil, errors.New("Main cache size must be greater than 0")
	}

	w, err := NewWTinylfu(windowSize, mainCacheSize)

	if err != nil {
		panic(err)
	}

	dList := NewDirtyPageList()

	n := Cache{
		CacheMap:    make(map[uint32]*Frame),
		wTinyLfu:    w,
		diryList:    dList,
		diskManager: diskmanager.NewDiskManager(),
	}

	// create new background writer
	bg := NewBgWriter(&n, wal)

	go bg.Start()

	return &n, nil
}

func toBytes(key uint32) []byte {
	b := make([]byte, 4)

	binary.LittleEndian.PutUint32(b, key)

	return b
}
