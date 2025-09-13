package diskio

import (
	"os"
)

type BufferPool struct {
	pool map[uint32]*Page
	fd   *os.File
}

// Gets page if in cache, otherwise read from disk(diskio)
func (bp *BufferPool) FetchPage(pageID uint32) (*Page, error) {
	// Check cache else read from disk
	v, ok := bp.pool[pageID]

	if ok {
		return v, nil
	}

	// cache miss
	page, err := DiskBTree.LoadPage(int32(pageID))

	if err != nil {
		return nil, err
	}

	// Add to cache
	bp.pool[pageID] = page

	return page, nil
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
