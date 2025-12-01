package buffermanager

import (
	"fmt"
	"sync"
	"time"

	//	buffermanager "github.com/oryankibandi/baobab/pkg/buffer_manager"
	diskio "github.com/oryankibandi/baobab/pkg/disk_io"
)

const (
	MAX_PAGES             = 100   // Max # of pages to flush in one operation
	BGWRITER_DELAY        = 5000  // // delay between BgWriter activity writes (Milliseconds)
	BG_FLUSH_AFTER        = 65536 // size after which to force flush to disk(bypass OS cache) size after which to force flush to disk(bypass OS cache (Bytes)
	FREELIST_WRITER_DELAY = 5000
)

type BgWriter struct {
	writtenPages uint32
	writtenBytes uint32
	mu           sync.Mutex
	wg           sync.WaitGroup
	cache        *Cache
}

func (bw *BgWriter) Start() {
	// Give time for other processes to initialize
	time.Sleep(time.Second * 15)
	go bw.watchFreeList()

	for {

		if bw.cache.diryList.dPageLru.isEmpty() {
			// no dirty page
			fmt.Println("No dirty frame... ")
			time.Sleep(time.Millisecond * BGWRITER_DELAY)
			continue
		}

		// check dirty list
		fDirty := bw.cache.diryList.popDirtyPage()
		for fDirty != nil {
			// remove frame from LRU
			err := bw.cache.RemoveItemFromLru(fDirty.frame)

			// err := f.PinFrame()

			if err != nil {
				panic(fmt.Sprintf("Unable to pin frame: ", err.Error()))
			}

			if d := fDirty.frame.IsDirty(); !d {
				err = bw.cache.ReleaseFrame(fDirty.frame)
				if err != nil {
					panic(err)
				}
				continue
			}

			bw.mu.Lock()
			bw.writtenPages++
			bw.mu.Unlock()

			c := make(chan int32)
			err = bw.cache.diskManager.WriteReq(fDirty.frame.page, &c)

			if err != nil {
				panic(err.Error())
			}

			n := <-c

			if n >= 0 {
				fmt.Printf("(bgwriter) Written %d bytes.\n", n)

				// Check if page is marked for deletion
				if d, err := fDirty.frame.PageIsDead(); err == nil && d {
					// release shared reader lock temporarily  to gain exclusive lock in bw.cache.Delete()
					// bw.cache.rmu.RUnlock()
					// remove from buffer pool
					bw.cache.Delete(uint32(fDirty.frame.page.Header.PageId), false)
					// bw.cache.rmu.RLock()
				} else if err != nil {
					panic(err)
				}

				bw.mu.Lock()
				bw.writtenBytes += uint32(n)
				bw.mu.Unlock()
			} else {
				fmt.Println("(bgwriter) Unable to write to disk")
			}

			// If flushed MAX_PAGES, break
			bw.mu.Lock()
			if bw.writtenPages >= MAX_PAGES {

				bw.writtenPages = 0
				bw.mu.Unlock()
				break
			}
			bw.mu.Unlock()

			// mark frame  as clean
			fDirty.frame.markClean()

			// unpin frame
			err = bw.cache.ReleaseFrame(fDirty.frame)

			if err != nil {
				panic(fmt.Sprintf("Unable to release frame: ", err.Error()))
			}

			fDirty = bw.cache.diryList.popDirtyPage()
		}

		// call Sync() to flush buffer contents to disk
		bw.cache.flushWritten()

		// bw.cache.rmu.RUnlock()
		// bw.wg.Wait()

		// DiskBTree.forceFlush()
		if bw.writtenPages > 0 {
			fmt.Println("+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")
			fmt.Printf("(bgwriter) Flushed %d page(s)\n", bw.writtenPages)
			fmt.Printf("(bgwriter) Written %d bytes\n", bw.writtenBytes)
			fmt.Println("+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")
		}

		//  DiskBTree.forceFlush()

		if bw.writtenPages > 0 {
			bw.mu.Lock()
			bw.writtenPages = 0
			bw.mu.Unlock()
		}

		if bw.writtenBytes > 0 {
			bw.mu.Lock()
			bw.writtenBytes = 0
			bw.mu.Unlock()
		}

		time.Sleep(time.Millisecond * BGWRITER_DELAY)
	}
}

func (bw *BgWriter) watchFreeList() {
	c := make(chan int)

	for {
		go diskio.PgFreeList.FlushFreeList(&c)

		// fmt.Println("Flushing free list....")
		n := <-c

		if n > 0 {
			fmt.Printf("Flushed %d bytes in free list\n", n)
		}

		time.Sleep(time.Millisecond * FREELIST_WRITER_DELAY)
	}
}

func NewBgWriter(cache *Cache) *BgWriter {
	return &BgWriter{
		cache: cache,
	}
}
