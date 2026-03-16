package buffermanager

import (
	"encoding/binary"
	"errors"
	"fmt"

	//"fmt"
	"log"
	"sync"

	diskmanager "github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/wal"
)

// Ideally, window cache size ≈ 1%, main cache ≈ 99%
const (
	WINDOW_CACHE_SIZE = 20
	MAIN_CACHE_SIZE   = 800
	CACHE_KEY_SIZE    = 16
	CACHE_RATIO       = 0.25
	MAIN_CACHE_RATIO  = 0.2
	LSN_SIZE          = 12

	// minimum cache size in KB
	MIN_CACHE_SIZE_KB = 8192
)

type CacheConfig struct {
	// size of the cache in KB. Minimum and default size is 8192KB(8MB)
	CacheSize uint64
}

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

	var delKeys []uint32
	var val *Frame

	val, ok := c.CacheMap[k]
	if !ok {
		// First time entry
		log.Println("First Frame, adding to window cache----------------")
		// add to wtinylfu
		newEntr, dKeys, err := c.wTinyLfu.AddItem(p, dirty)
		if err != nil {
			fmt.Println(helpers.BOLDRED + err.Error() + helpers.RESET)
			return nil, err
		}

		delKeys = dKeys
		c.CacheMap[k] = newEntr
		val = newEntr
	} else {
		// update value
		err := val.SetData(p)
		if err != nil {
			return nil, err
		}

		// increment count
		c.wTinyLfu.Increment(val)
	}

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
	return val, nil
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
		c.wTinyLfu.Increment(val)
		c.rmu.RUnlock()

		val.Reference()
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

		f.Reference()
		return f, nil
	}
}

// Delete an item from cache. flush parameter is set to true if it's a direct request from client. If bgwriter, it is false since the page is already flushed.
func (c *Cache) Delete(key uint32, flush bool) error {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	val, ok := c.CacheMap[key]
	if !ok {
		return BufferManagerError{Message: "No key in cache"}
	}

	if flush && val.isDirty() {
		err := c.prepareForEviction(val)
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
	f.MarkDirty()
	fmt.Println("(MarkDirtyFrame) Adding frame to dirty list")
	c.diryList.addDirtyFrame(f)

	return nil
}

// Creates a new frame and assigns page ID to frame with provided keys and values/child
// SetAsRoot parameter ensures to set
func (c *Cache) CreateNewFrame(lsn []byte, keys [][]byte, values [][]byte, childPageIds []int32, setAsRoot bool) (*Frame, error) {
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
	f, err := c.put(uint32(pgeId), pge, true)
	log.Printf("Added new page to cache. Page ID: %d\n", pge.PageId)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Calls ForceFlush on disk manager
func (c *Cache) flushWritten() {
	c.diskManager.ForceFlush()
}

func (c *Cache) flushFreeList() error {
	err := c.diskManager.FlushFreeList()

	if err != nil {
		return err
	}

	return nil
}

// Prepares page for eviction by flushing page to disk
func (c *Cache) prepareForEviction(f *Frame) error {
	log.Println("lru.PrepareForEviction()")
	if f == nil {
		return BufferManagerError{"Provided frame is nil."}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.isDirty() {
		// frame not dirty, skip flushing to disk
		fmt.Println("(prepareForEviction) frame not dirty, skipping flushing")

		return nil
	}

	ch := make(chan int32)
	// lsnChan := make(chan []byte)
	err := c.diskManager.WriteReq(&f.page, f.getKey(), ch, nil)

	if err != nil {
		panic(err.Error())
	}

	n := <-ch
	fmt.Printf("Written %d bytes\n", n)
	return nil
}

// sets the frame's page ID as the new root on the disk manager
func (c *Cache) SetNewRoot(f *Frame) error {
	pId := f.getKey()

	err := c.diskManager.SetAsRoot(int32(pId))

	if err != nil {
		return err
	}

	return nil
}

// Syncs contents to the frame's associated page.
// Called when the materialized node has been updated
// through DELETEs, PUTs, Merges or Splits
// func (c *Cache) SyncFrame(f *Frame, lsn []byte, keys [][]byte, vals [][]byte, pageIds []int32, rightSibling uint32, leftSibling uint32) error {
// 	if len(lsn) != diskmanager.LSN_SIZE_BYTE {
// 		panic(fmt.Errorf("Invalid LSN length. Got length %d, expected length %d", len(lsn), diskmanager.LSN_SIZE_BYTE))
// 	}
//
// 	f.mu.Lock()
//
// 	err := f.page.Sync(lsn, keys, vals, pageIds, rightSibling, leftSibling)
// 	fmt.Println("DONE SYNCING...")
//
// 	if err != nil {
// 		f.mu.Unlock()
// 		return err
// 	}
//
// 	// update lsn on frame
// 	f.lsn = lsn
// 	f.mu.Unlock()
//
// 	// mark frame as dirty
// 	// f.MarkDirty()
// 	fmt.Println("(SyncFrame) MARKING FRAMS AS DIRTY...")
// 	err = c.MarkFrameDirty(f)
// 	fmt.Println("(SyncFrame) MARKED FRAME AS DIRTY...")
//
// 	if err != nil {
// 		return err
// 	}
//
// 	return nil
// }

// calls disk manager to flush metadata to metadata page
func (c *Cache) flushMetadata() error {
	c.rmu.RLock()
	defer c.rmu.RUnlock()

	c.diskManager.FlushMetadata()

	return nil
}

// Safely close down cache
func (c *Cache) Close() error {
	if c.wTinyLfu != nil {
		c.wTinyLfu.close()
	}

	if c.diskManager != nil {
		c.diskManager.Close()
	}

	return nil
}

// NewCache Create new cache instance\n windowSize, probationSize and protectedSize are sized of the individual segments
//
//	Parameters:
//	cacheSize - size of the cache in KB. Minimum and default size is 8192KB(8MB)
//	wal - initialized wal instance
//	config - disk manager config
//
// Returns:
func NewCache(cacheConfig CacheConfig, wal *wal.WAL, config diskmanager.DiskManagerConfig) (*Cache, error) {
	if cacheConfig.CacheSize < MIN_CACHE_SIZE_KB {
		return nil, BufferManagerError{Message: fmt.Sprintf("Minimum cache size is %dKB", MIN_CACHE_SIZE_KB)}
	}

	if wal == nil {
		return nil, BufferManagerError{Message: "wal instance  not provided."}
	}

	windSize := uint64(float64(0.01) * float64(cacheConfig.CacheSize))
	mainSize := uint64(float64(0.99) * float64(cacheConfig.CacheSize))
	w, err := NewWTinylfu(windSize, mainSize)
	if err != nil {
		panic(err)
	}

	dList := NewDirtyPageList()

	diskMan, err := diskmanager.NewDiskManager(config)
	if err != nil {
		return nil, err
	}

	n := Cache{
		CacheMap:    make(map[uint32]*Frame),
		wTinyLfu:    w,
		diryList:    dList,
		diskManager: diskMan,
	}

	// create new background writer
	bg := NewBgWriter(&n, wal, w.cBuffer, diskMan)

	go bg.Start()

	return &n, nil
}

func toBytes(key uint32) []byte {
	b := make([]byte, 4)

	binary.LittleEndian.PutUint32(b, key)

	return b
}
