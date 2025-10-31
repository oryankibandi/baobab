package diskio

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"sync"
)

const (
	FREE_LIST_PAGE_SIZE  = 4096
	FREE_LIST_ENTRY_SIZE = 4
	ITEMS_PER_PAGE       = 1024
)

type FreeList struct {
	count     int32
	freePages []uint32
	dirty     bool
	mu        sync.Mutex
	fd        *os.File
}

func (fl *FreeList) pop() int32 {
	fmt.Println("(pop) GETTING ITEM FROM FREE LIST...")
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fmt.Println("(pop) obtained exclusive lock")

	if fl.count <= 0 {
		return -1
	}

	p := fl.freePages[fl.count-1]

	fl.freePages = append(fl.freePages[:fl.count-1], fl.freePages[fl.count:]...)

	fl.count--
	fl.dirty = true
	fmt.Println("FREE LIST AFTER POP() => ", fl.freePages)

	return int32(p)
}

func (fl *FreeList) add(p uint32) bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()

	fl.freePages = append([]uint32{p}, fl.freePages...)
	fl.count++
	fl.dirty = true

	return true
}

func (fl *FreeList) FlushFreeList(c *chan int) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if !fl.dirty {
		*c <- 0
		return
	}

	var written int
	numOfPages := math.Ceil(float64(fl.count) / float64(FREE_LIST_PAGE_SIZE))
	byteArr := make([]byte, FREE_LIST_PAGE_SIZE)
	var startIdx int
	var itemList []uint32

	if fl.count <= 0 {
		// clear file contents
		err := fl.fd.Truncate(0)

		if err != nil {
			panic(fmt.Sprintf("Unable to truncate free list: ", err.Error()))
		}

		log.Println("Cleared file")

		// Sync
		fl.fd.Sync()

		fl.dirty = false
		*c <- written
		return
	}

	for i := range int(numOfPages) {
		if fl.count >= int32((i+1)*ITEMS_PER_PAGE) {
			itemList = fl.freePages[i*ITEMS_PER_PAGE : (i*ITEMS_PER_PAGE)+ITEMS_PER_PAGE]
		} else {
			itemList = fl.freePages[i*ITEMS_PER_PAGE:]
		}

		for idx, p := range itemList {
			startIdx = idx * FREE_LIST_ENTRY_SIZE
			binary.LittleEndian.PutUint32(byteArr[startIdx:startIdx+4], p)
		}

		n, err := fl.fd.WriteAt(byteArr, int64(i*FREE_LIST_PAGE_SIZE))

		if err != nil {
			panic(err.Error())
		}

		written += n

		// reset byte arr
		byteArr = make([]byte, FREE_LIST_PAGE_SIZE)
	}

	// truncate file to get rid of excess data
	err := fl.fd.Truncate(int64(numOfPages) * FREE_LIST_PAGE_SIZE)

	if err != nil {
		panic(fmt.Sprintf("Unable to truncate free list: ", err.Error()))
	}

	fmt.Println("TRUNCATED SUCCESSFULLY...")

	// Sync
	fl.fd.Sync()

	fl.dirty = false
	*c <- written

	return
}

// Called during startup to read the free list from disk
func (fl *FreeList) loadFreeList() {
	if fl.fd == nil {
		panic("Free list file descriptor not initialized")
	}

	// Get size
	info, err := fl.fd.Stat()

	if err != nil {
		panic(err.Error())
	}

	size := info.Size()
	numPages := math.Ceil(float64(size) / float64(FREE_LIST_PAGE_SIZE))
	byteArr := make([]byte, FREE_LIST_PAGE_SIZE)

FreeLoop:
	for i := range int(numPages) {
		n, err := fl.fd.ReadAt(byteArr, int64(i*FREE_LIST_PAGE_SIZE))

		if err != nil {
			panic(fmt.Sprintf("Unable to read free list: ", err.Error()))
		}

		fmt.Printf("(freelist) Read %d bytes\n", n)

		for j := range ITEMS_PER_PAGE {
			// add each item to free list
			item := binary.LittleEndian.Uint32(byteArr[j*FREE_LIST_ENTRY_SIZE : (j*FREE_LIST_ENTRY_SIZE)+FREE_LIST_ENTRY_SIZE])

			if item == 0 {
				// end of items, break
				break FreeLoop
			}

			fl.freePages = append(fl.freePages, item)
		}
	}

	fl.count = int32(len(fl.freePages))

	fmt.Printf("(freelist) Loaded %d items from free list\n", fl.count)
}

func (fl *FreeList) close() {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	err := fl.fd.Close()

	if err != nil {
		panic(fmt.Sprintf("Unable to close free list file descriptor: ", err))
	}

	fmt.Println("closed free list file descriptors.")
}

func NewFreeList() *FreeList {
	fd, err := os.OpenFile("data_fl", os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		panic(fmt.Sprintf("Unable to open free list file: ", err.Error()))
	}

	fl := &FreeList{
		fd:        fd,
		freePages: make([]uint32, 0),
		dirty:     false,
		count:     0,
	}

	return fl
}
