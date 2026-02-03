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
func (c *Cache) put(k uint32, p *diskmanager.Page, dirty bool) (*Frame, error) {
	fmt.Println("cache.Put")
	c.rmu.Lock()

	// FIX: Remove debug code below
	fmt.Println("(cache.put()) PUT NEW PAGE ============================================================================================> ", k)

	fmt.Println("")

	// formattedKey := toKey(k)
	// internal page
	isInternal, err := p.IsInternal()

	if err != nil {
		return nil, err
	}

	var delKeys []uint32

	item := Frame{
		page:       p,
		Key:        k,
		IsInternal: isInternal,
	}

	// A newly created page will be marked as dirty so the background
	// writer can flush it to disk
	if dirty {
		err = c.MarkFrameDirty(&item)

		if err != nil {
			panic(err)
		}
	}

	lsn := p.GetLSN()
	// set lsn
	item.lsn = lsn

	// Check if item already exists
	val, ok := c.CacheMap[k]
	if !ok {
		// First time entry
		log.Println("First Entry, adding to window cache----------------")
		// item.UpdateCacheType(Window)

		c.CacheMap[k] = &item

		// add to lru
		delKeys, err = c.wTinyLfu.AddItem(&item)

		if err != nil {
			panic(err)
		}

	} else {
		// update value
		log.Println("Item Exists, incrementing count........")
		log.Println("EXISTING FRAME PREV ====================> ", val.prev)
		log.Println("EXISTING FRAME NEXT ====================>", val.next)

		val.UpdatePage(p)

		// increment count
		c.wTinyLfu.Increment(&item)
	}

	// if err != nil {
	// 	panic(err.Error())
	// }

	// If there are items that have been evicted, delete them from map
	if len(delKeys) > 0 {
		for _, k := range delKeys {
			// delete(c.CacheMap, k)
			c.Delete(k, true)
		}

		delKeys = nil
	}

	log.Println("Printing stats ....")
	c.rmu.Unlock()
	go c.wTinyLfu.Stat()
	return &item, nil
}

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
		fmt.Println("Item found, doing a GetIncrement()")
		c.wTinyLfu.GetIncrement(val)
		c.rmu.RUnlock()

		return val, nil
	} else {
		// Retrieve item from disk
		fmt.Println("Item not found, reading from disk")
		ch := make(chan *diskmanager.Page)
		err := c.diskManager.ReadReq(uint32(pageId), &ch)

		if err != nil {
			return nil, err
		}

		fmt.Println("(bufferManager.Get()) Waiting for read req to complete..")
		pge := <-ch
		fmt.Println("(bufferManager.Get() Read request done.")

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
func (c *Cache) ReleaseFrame(f *Frame, flushed bool) error {
	log.Println("READDING TO LRU --> ", f)
	del, delKeys, err := c.wTinyLfu.reAddToLru(f, flushed)

	if err != nil {
		return err
	}

	if del && delKeys != nil && len(delKeys) > 0 {
		fmt.Println("DELETING FRAME FROM LRU ----> ")
		// flush and delete keys
		for _, k := range delKeys {
			c.rmu.Lock()
			err := c.Delete(k, true)
			c.rmu.Unlock()

			if err != nil {
				panic(err)
			}
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

	// TODO: Check if frame is dirty
	if flush {
		err = c.prepareForEviction(val)
		fmt.Println("(Delete) prepared for eviction...")

		if err != nil {
			panic(err)
		}

		// delete from dirty page list
		c.diryList.removePage(key)
	}

	delete(c.CacheMap, key)

	c.freeFrames++

	if c.frameCount > 0 {
		c.frameCount++
	}

	fmt.Println("c.Delete() DONE>..")
	return nil
}

func (c *Cache) GetRootPageId() uint32 {
	return c.diskManager.CheckRootPageId()
}

// marks a frame as dirty and adds it to dirty list LRU
func (c *Cache) MarkFrameDirty(f *Frame) error {
	f.markDirty()
	fmt.Println("(MarkDirtyFrame) Adding frame to dirty list")
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
	log.Printf("Added new page to cache. Page ID: %d\nframe -> %v", pge.Header.PageId, f)

	// Set LSN
	err = f.UpdatePageLSN(lsn)

	if err != nil {
		panic(err)
	}

	return f, nil
}

// Calls ForceFlush on disk manager
func (c *Cache) flushWritten() {
	c.diskManager.ForceFlush()
}

// Prepares page for eviction by flushing page to disk
func (c *Cache) prepareForEviction(f *Frame) error {
	log.Println("lru.PrepareForEviction()")
	if f == nil {
		return BufferManagerError{"Provided frame is nil."}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.isDirty {
		// frame not dirty, skip flushing to disk
		fmt.Println("(prepareForEviction) frame not dirty, skipping flushing")

		return nil
	}

	ch := make(chan int32)
	// lsnChan := make(chan []byte)
	err := c.diskManager.WriteReq(f.page, f.Key, ch, nil)

	if err != nil {
		panic(err.Error())
	}

	n := <-ch

	log.Printf("(PrepareForEviction) Flushed %d bytes of page %d\n", n, f.Key)

	// l := <-lsnChan
	// fmt.Println("Received LSN -> ", l)

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

	// update lsn on frame
	f.lsn = lsn
	f.mu.Unlock()

	// mark frame as dirty
	// f.MarkDirty()
	fmt.Println("(SyncFrame) MARKING FRAMS AS DIRTY...")
	err = c.MarkFrameDirty(f)
	fmt.Println("(SyncFrame) MARKED FRAME AS DIRTY...")

	if err != nil {
		return err
	}

	return nil
}

// calls disk manager to flush metadata to metadata page
func (c *Cache) flushMetadata() error {
	c.rmu.RLock()
	defer c.rmu.RUnlock()

	c.diskManager.FlushMetadata()

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
