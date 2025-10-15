package diskio

import (
	"fmt"
	"sync"
	"time"
)

const (
	MAX_PAGES      = 100   // Max # of pages to flush in one operation
	BGWRITER_DELAY = 10000 // // delay between BgWriter activity writes (Milliseconds)
	BG_FLUSH_AFTER = 65536 // size after which to force flush to disk(bypass OS cache) size after which to force flush to disk(bypass OS cache (Bytes)
)

type BgWriter struct {
	writtenPages uint32
	writtenBytes uint32
	mu           sync.Mutex
	wg           sync.WaitGroup
}

func (bw *BgWriter) start() {
	for {
		for _, p := range BPool.pool {
			dirty, err := p.isDirty()

			if err != nil {
				panic(err.Error())
			}

			if dirty {
				bw.mu.Lock()
				bw.writtenPages++
				bw.mu.Unlock()
				// write to disk
				bw.wg.Add(1)
				go func() {
					defer bw.wg.Done()
					c := make(chan int32)
					err = DiskBTree.WriteReq(p, &c)

					if err != nil {
						panic(err.Error())
					}

					n := <-c

					if n >= 0 {
						fmt.Printf("(bgwriter) Written %d bytes.\n", n)
						bw.mu.Lock()
						bw.writtenBytes += uint32(n)
						bw.mu.Unlock()
					} else {
						fmt.Println("(bgwriter) Unable to write to disk")
					}
				}()

				// If flushed MAX_PAGES, break
				if bw.writtenPages >= MAX_PAGES {
					bw.mu.Lock()
					bw.writtenPages = 0
					bw.mu.Unlock()
					break
				}
			}
		}

		bw.wg.Wait()

		DiskBTree.forceFlush()
		fmt.Println("+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")
		fmt.Printf("(bgwriter) Flushed %d page(s)\n", bw.writtenPages)
		fmt.Printf("(bgwriter) Written %d bytes\n", bw.writtenBytes)
		fmt.Println("+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")

		DiskBTree.forceFlush()

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

func newBgWriter() *BgWriter {
	return &BgWriter{}
}
