package buffermanager

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	//	buffermanager "github.com/oryankibandi/baobab/pkg/buffer_manager"
	diskio "github.com/oryankibandi/baobab/pkg/disk_io"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/wal"
)

const (
	MAX_PAGES             = 100   // Max # of pages to flush in one operation
	BGWRITER_DELAY        = 5000  // // delay between BgWriter activity writes (Milliseconds)
	BG_FLUSH_AFTER        = 65536 // size after which to force flush to disk(bypass OS cache) size after which to force flush to disk(bypass OS cache (Bytes)
	FREELIST_WRITER_DELAY = 5000
)

type BgWriter struct {
	writtenPages atomic.Uint32
	writtenBytes atomic.Uint32
	mu           sync.Mutex
	wg           sync.WaitGroup
	cache        *Cache
	wal          *wal.WAL
}

func (bw *BgWriter) Start() {
	// Give time for other processes to initialize
	time.Sleep(time.Second * 15)
	go bw.watchFreeList()

	var dFrame []*Frame
	for {
		if bw.cache.diryList.dPageLru.isEmpty() {
			// no dirty page
			log.Println("No dirty frame... ")
			time.Sleep(time.Millisecond * BGWRITER_DELAY)
			continue
		}

		fmt.Println("Dirty frames available....")
		// check dirty list
		fDirty := bw.cache.diryList.popDirtyPage()
		LSN := struct {
			maxLSN []byte
			mu     sync.Mutex
		}{maxLSN: make([]byte, 8)}

		var k int

		for fDirty != nil {
			// pin frames sequentially to avoid deadlocks
			fmt.Printf("(BGWRITER) %d RemoveItemFromLru\n", k)
			k++
			err := bw.cache.RemoveItemFromLru(fDirty.frame)

			fmt.Println("(bgwriter) Frame Pinned")
			// err := f.PinFrame()

			if err != nil {
				panic(fmt.Sprintf("Unable to pin frame: %v", err))
			}

			if d := fDirty.frame.IsDirty(); !d {
				fmt.Println("(bgwriter) Frame not dirty, releasing...")
				err = bw.cache.ReleaseFrame(fDirty.frame, true)
				if err != nil {
					panic(err)
				}
				continue
				// return
			}

			dFrame = append(dFrame, fDirty.frame)

			fDirty = bw.cache.diryList.popDirtyPage()
		}

		// spin up goroutines to issue writes
		for _, f := range dFrame {
			bw.wg.Add(1)
			go func(fr *Frame) {
				defer bw.wg.Done()
				c := make(chan int32)
				lsnChan := make(chan []byte)
				err := bw.cache.diskManager.WriteReq(fr.GetPage(), fr.GetKey(), c, lsnChan)

				if err != nil {
					panic(err.Error())
				}

				fmt.Println("(bgwriter) waiting for write completion from disk manager...")
				n := <-c
				fmt.Println("(bgwriter) Received write completion  signal...")

				if n >= 0 {
					fmt.Printf("(bgwriter) Written %d byte(s).\n", n)

					// Check if page is marked for deletion
					if d, err := fr.PageIsDead(); err == nil && d {
						fmt.Println("(bgwriter) Page marked for deletion, removing from cache")
						// release shared reader lock temporarily  to gain exclusive lock in bw.cache.Delete()
						// bw.cache.rmu.RUnlock()
						// remove from buffer pool
						bw.cache.Delete(uint32(fr.GetKey()), false)
						// bw.cache.rmu.RLock()
					} else if err != nil {
						fmt.Println("(bgwriter) Page not marked for deletion")
						panic(err)
					}

					// bw.writtenBytes += uint32(n)
					bw.writtenBytes.Add(uint32(n))

					bw.writtenPages.Add(1)
				} else {
					fmt.Println("(bgwriter) Unable to write to disk")
				}
				// Get written page LSN
				fmt.Println("WAITING FOR LSN CHANNEL....")
				lsn := <-lsnChan

				fmt.Println("RECEIVED LSN FROM CHAN => ", lsn)
				// compare and update LSN
				LSN.mu.Lock()

				newLSN, err := helpers.MaxLSN(LSN.maxLSN, lsn)

				if err != nil {
					panic(fmt.Errorf("Unable to get max LSN in bgwriter: %v", err))
				}
				fmt.Printf("(MaxLSN) Comparing LSNs:a) %v  (b) %v\t MAx: %v\n", LSN.maxLSN, lsn, newLSN)

				LSN.maxLSN = newLSN
				LSN.mu.Unlock()

				fmt.Println("RECEIVED MAX LSN ==> ", lsn)
				// If flushed MAX_PAGES, break
				// if bw.writtenPages.Load() >= MAX_PAGES {

				// 	bw.writtenPages.Store(0)
				// 	// break
				// 	return
				// }

				// mark frame  as clean
				fr.markClean()
			}(f)
		}

		// release frames
		for _, f := range dFrame {
			err := bw.cache.ReleaseFrame(f, true)

			if err != nil {
				panic(fmt.Sprintf("Unable to release frame: %v", err))
			}
		}

		// for fDirty != nil {
		// 	bw.wg.Add(1)
		// 	go func(f *pDirty) {
		// 		defer bw.wg.Done()
		// 		// remove frame from LRU(Pin)
		// 		err := bw.cache.RemoveItemFromLru(f.frame)

		// 		fmt.Println("(bgwriter) Frame Pinned")
		// 		// err := f.PinFrame()

		// 		if err != nil {
		// 			panic(fmt.Sprintf("Unable to pin frame: %v", err))
		// 		}

		// 		if d := f.frame.IsDirty(); !d {
		// 			fmt.Println("(bgwriter) Frame not dirty, releasing...")
		// 			err = bw.cache.ReleaseFrame(f.frame, true)
		// 			if err != nil {
		// 				panic(err)
		// 			}
		// 			// continue
		// 			return
		// 		}

		// 		bw.writtenPages.Add(1)
		// 		// bw.writtenPages++

		// 		c := make(chan int32)
		// 		lsnChan := make(chan []byte)
		// 		err = bw.cache.diskManager.WriteReq(f.frame.page, &c, &lsnChan)

		// 		if err != nil {
		// 			panic(err.Error())
		// 		}

		// 		fmt.Println("(bgwriter) waiting for write completion from disk manager...")
		// 		n := <-c
		// 		fmt.Println("(bgwriter) Received write completion  signal...")

		// 		if n >= 0 {
		// 			fmt.Printf("(bgwriter) Written %d byte(s).\n", n)

		// 			// Check if page is marked for deletion
		// 			if d, err := f.frame.PageIsDead(); err == nil && d {
		// 				fmt.Println("(bgwriter) Page marked for deletion, removing from cache")
		// 				// release shared reader lock temporarily  to gain exclusive lock in bw.cache.Delete()
		// 				// bw.cache.rmu.RUnlock()
		// 				// remove from buffer pool
		// 				bw.cache.Delete(uint32(f.frame.page.Header.PageId), false)
		// 				// bw.cache.rmu.RLock()
		// 			} else if err != nil {
		// 				fmt.Println("(bgwriter) Page not marked for deletion")
		// 				panic(err)
		// 			}

		// 			// bw.writtenBytes += uint32(n)
		// 			bw.writtenBytes.Add(uint32(n))
		// 		} else {
		// 			fmt.Println("(bgwriter) Unable to write to disk")
		// 		}
		// 		// Get written page LSN
		// 		fmt.Println("WAITING FOR LSN CHANNEL....")
		// 		lsn := <-lsnChan

		// 		fmt.Println("RECEIVED LSN FROM CHAN => ", lsn)
		// 		// compare and update LSN
		// 		LSN.mu.Lock()

		// 		newLSN, err := helpers.MaxLSN(LSN.maxLSN, lsn)

		// 		if err != nil {
		// 			panic(fmt.Errorf("Unable to get max LSN in bgwriter: %v", err))
		// 		}
		// 		fmt.Printf("(MaxLSN) Comparing LSNs:a) %v  (b) %v\t MAx: %v\n", LSN.maxLSN, lsn, newLSN)

		// 		LSN.maxLSN = newLSN
		// 		LSN.mu.Unlock()

		// 		fmt.Println("RECEIVED MAX LSN ==> ", lsn)
		// 		// If flushed MAX_PAGES, break
		// 		if bw.writtenPages.Load() >= MAX_PAGES {

		// 			bw.writtenPages.Store(0)
		// 			// break
		// 			return
		// 		}

		// 		// flush metadata
		// 		// err = bw.cache.flushMetadata()

		// 		// if err != nil {
		// 		// 	panic(err)
		// 		// }

		// 		bw.mu.Unlock()

		// 		// mark frame  as clean
		// 		f.frame.markClean()

		// 		// unpin frame
		// 		err = bw.cache.ReleaseFrame(f.frame, true)

		// 		if err != nil {
		// 			panic(fmt.Sprintf("Unable to release frame: %v", err))
		// 		}
		// 	}(fDirty)

		// 	fDirty = bw.cache.diryList.popDirtyPage()
		// }

		log.Println("(BGWRITER) WAITING FOR GOROUTINES....")
		bw.wg.Wait()
		log.Println("(BGWRITER) DONE...")
		// flush metadata
		err := bw.cache.flushMetadata()

		if err != nil {
			panic(err)
		}

		// call Sync() to flush buffer contents to disk
		bw.cache.flushWritten()

		// Add checkpoint
		log.Println("(BGWRITER) Adding checkpoint.....")
		err = bw.wal.AddCheckpoint(LSN.maxLSN)
		log.Println("(BGWRITER) Added checkpoint.....")

		if err != nil {
			panic(fmt.Errorf("Unable to add checkoint: %v", err))
		}

		// bw.cache.rmu.RUnlock()
		// bw.wg.Wait()

		// DiskBTree.forceFlush()
		log.Println("(BGWRITER) Getting written pages and bytes...")
		writtenP := bw.writtenPages.Load()
		writtenB := bw.writtenBytes.Load()
		fmt.Println("--+-+-+-+-+-+-+->> WrittenPages: ", writtenP)
		if writtenP > 0 {
			msg := "+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+\n"
			msg += fmt.Sprintf("(bgwriter) Flushed %d page(s)\n", writtenP)
			msg += fmt.Sprintf("(bgwriter) Written %d bytes\n", writtenB)
			msg += "+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+\n"

			log.Println(msg)
		}

		log.Println("MAXLSN ===> ", LSN.maxLSN)

		//  DiskBTree.forceFlush()

		if writtenP > 0 {
			bw.writtenPages.Store(0)
		}

		if writtenB > 0 {
			bw.writtenBytes.Store(0)
		}

		log.Println("(BGWRITER) Reset written page counters...")
		dFrame = make([]*Frame, 0)
		log.Println("(BGWRITER) Sleeping...")

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

func NewBgWriter(cache *Cache, wal *wal.WAL) *BgWriter {
	return &BgWriter{
		cache: cache,
		wal:   wal,
	}
}
