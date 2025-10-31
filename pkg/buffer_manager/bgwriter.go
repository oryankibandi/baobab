package buffermanager

import (
	"fmt"
	"sync"
	"time"

	//	buffermanager "github.com/oryankibandi/on_disk_btree/pkg/buffer_manager"
	"github.com/oryankibandi/on_disk_btree/internal/lrulist"
	diskio "github.com/oryankibandi/on_disk_btree/pkg/disk_io"
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
}

func (bw *BgWriter) Start() {
	// Give time for other processes to initialize
	time.Sleep(time.Second * 5)
	go bw.watchFreeList()
	dirtyList := make([]*lrulist.Frame, 0)
	for {
		BCache.rmu.Lock()
		for _, f := range BCache.CacheMap {

			dirty, err := f.Page.IsDirty()

			if err != nil {
				panic(err.Error())
			}

			if dirty {
				dirtyList = append(dirtyList, f)
			}

		}

		BCache.rmu.Unlock()
		fmt.Println("DIRTY LIST COUNT => ", len(dirtyList))
		// check dirty list and flush dirty frames
		for _, f := range dirtyList {
			err := f.PinFrame()

			if err != nil {
				panic(fmt.Sprintf("Unable to pin frame: ", err.Error()))
			}

			// remove frame from LRU
			// BCache.rmu.RUnlock()
			BCache.RemoveItemFromLru(f)
			// BCache.rmu.RLock()

			if d, err := f.Page.IsDirty(); err != nil {
				panic(err)
			} else if !d {
				BCache.ReleaseFrame(f)
				continue
			}

			bw.mu.Lock()
			bw.writtenPages++
			bw.mu.Unlock()

			c := make(chan int32)
			err = diskio.DiskBTree.WriteReq(f.Page, &c)

			if err != nil {
				panic(err.Error())
			}

			n := <-c

			if n >= 0 {
				fmt.Printf("(bgwriter) Written %d bytes.\n", n)

				if f.Page.Header.IsSet(4) {
					// release shared reader lock temporarily  to gain exclusive lock in BCache.Delete()
					// BCache.rmu.RUnlock()
					// remove from buffer pool
					BCache.Delete(uint32(f.Page.Header.PageId), false)
					// BCache.rmu.RLock()
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

			err = BCache.ReleaseFrame(f)

			if err != nil {
				panic(fmt.Sprintf("Unable to release frame: ", err.Error()))
			}
		}

		diskio.DiskBTree.ForceFlush()

		// BCache.rmu.RUnlock()
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

		dirtyList = make([]*lrulist.Frame, 0)

		time.Sleep(time.Millisecond * BGWRITER_DELAY)
	}
}

func (bw *BgWriter) watchFreeList() {
	for {
		c := make(chan int)
		go diskio.PgFreeList.FlushFreeList(&c)

		fmt.Println("Flushing free list....")
		n := <-c

		fmt.Printf("Flushed %d bytes in free list\n", n)

		time.Sleep(time.Millisecond * FREELIST_WRITER_DELAY)
	}
}

func NewBgWriter() *BgWriter {
	return &BgWriter{}
}
