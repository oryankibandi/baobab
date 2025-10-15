package diskio

import (
	"log"
	"os"
)

type BufferPool struct {
	pool map[uint32]*Page
	fd   *os.File
}

var BPool BufferPool

func init() {
	BPool = BufferPool{
		pool: make(map[uint32]*Page),
	}

	log.Println("Initialized Buffer Pool: ", BPool)

	bgWriter := newBgWriter()
	go bgWriter.start()

	log.Println("Initialized BgWriter ")
}

// Gets page if in cache, otherwise read from disk(diskio)
func (bp *BufferPool) FetchPage(pageID uint32) (*Page, error) {
	// Check cache else read from disk
	v, ok := bp.pool[pageID]

	if ok {
		log.Println("CACHE HIT: ", pageID)
		log.Println("(FetchPage) PAGE KEYS: ")
		for i, c := range v.Cells {
			log.Printf("%d: %v\n", i, c.Key)
		}
		return v, nil
	}

	// cache miss
	log.Println("CACHE MISS: ", pageID)
	c := make(chan *Page)
	err := DiskBTree.ReadReq(pageID, &c)
	// page, err := DiskBTree.LoadPage(int32(pageID))

	if err != nil {
		return nil, err
	}

	pge := <-c
	// Add to cache
	bp.pool[pageID] = pge
	log.Println("ADDED TO POOL ------> ", bp.pool)

	return pge, nil
}

// Delete a page from pool
func (bp *BufferPool) Delete(pageID uint32) {
	_, ok := bp.pool[pageID]

	if !ok {
		return
	}

	delete(bp.pool, pageID)
}

// Add page to cache
func (bp *BufferPool) AddPageToCache(pageId uint32, page *Page) (bool, error) {
	bp.pool[pageId] = page

	return true, nil
}

// Clear buffer pool
func (bp *BufferPool) Clear() {
	bp.pool = make(map[uint32]*Page)
}

// TODO: Add an eviction policy (algorithm)
