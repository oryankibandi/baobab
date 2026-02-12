package diskmanager

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewDiskManager(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{dataFile: dbFile}

	// test no config
	_, err := NewDiskManager(DiskManagerConfig{})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if !errors.As(err, &DiskioError{}) {
		t.Fatalf("Expected DiskioError, got %v", err)
	}

	// test good config
	dm, err := NewDiskManager(config)

	if err != nil || dm == nil {
		t.Fatal("Could not initiate disk manager")
	}

	defer dm.Close()

	// check free list
	if dm.freeList == nil {
		t.Fatalf("Free list not initialized.")
	}
}

func TestWritePage(t *testing.T) {
	var pageId int32 = 1
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{dataFile: dbFile}

	dm, err := NewDiskManager(config)

	if err != nil || dm == nil {
		t.Fatal("Could not initiate disk manager")
	}

	defer dm.Close()

	newPage := NewTestPage(pageId)
	if newPage == nil {
		t.Fatal("Expected new Page, got nil")
	}

	// fill with 1s
	tmpPage := make([]byte, PAGE_SIZE_BYTES-5)
	for i := range tmpPage {
		tmpPage[i] = 1
	}

	copy(newPage.pgeData[6:], tmpPage)
	tmpPage = nil

	wrChan := make(chan int32)
	err = dm.WriteReq(newPage, newPage.PageId, wrChan, nil)
	if err != nil {
		t.Fatalf("Expected no error on WriteReq, got %s", err.Error())
	}

	testTimeout := 200
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

loop:
	for {
		select {
		case n := <-wrChan:
			cancel()
			t.Logf("Written %d bytes", n)
			break loop
		case <-ctx.Done():
			t.Fatalf("Writing to disk timed out after %v", testTimeout)
		}
	}

	// check file
	s, err := os.Stat(dbFile)
	if err != nil {
		t.Fatalf("File not created: %s", err.Error())
	}

	expectedFileSize := PAGE_SIZE_BYTES * (pageId + 1)
	actualSize := s.Size()
	if actualSize != int64(expectedFileSize) {
		t.Fatalf("Expected file size of %d bytes, got %d bytes", expectedFileSize, actualSize)
	}
}

func TestWritePageConcurrent(t *testing.T) {
	pageCount := 100
	var pageId int32 = 1
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{dataFile: dbFile}

	dm, err := NewDiskManager(config)

	if err != nil || dm == nil {
		t.Fatal("Could not initiate disk manager")
	}

	defer dm.Close()

	pages := make([]*Page, 0)

	for range pageCount {
		p := NewTestPage(pageId)
		if p == nil {
			t.Fatal("Expected new Page, got nil")
		}

		p.pgeData[PAGE_SIZE_BYTES-1] = 1

		pages = append(pages, p)
		pageId++
	}

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i, v := range pages {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("test_write_%d", i), func(t *testing.T) {
				<-start
				wrChan := make(chan int32)
				err = dm.WriteReq(v, v.PageId, wrChan, nil)
				if err != nil {
					t.Fatalf("Expected no error on WriteReq, got %s", err.Error())
				}

				testTimeout := 200
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

			loop:
				for {
					select {
					case n := <-wrChan:
						cancel()
						t.Logf("Written %d bytes", n)
						break loop
					case <-ctx.Done():
						t.Errorf("Writing to disk timed out after %v", testTimeout)
					}
				}
			})
		}()
	}

	// start goroutines concurrently
	close(start)
	wg.Wait()

	s, err := os.Stat(dbFile)
	if err != nil {
		t.Fatalf("File not created: %s", err.Error())
	}

	expectedFileSize := PAGE_SIZE_BYTES * (pageId)
	actualSize := s.Size()
	if actualSize != int64(expectedFileSize) {
		t.Fatalf("Expected file size of %d bytes, got %d bytes", expectedFileSize, actualSize)
	}
}

func TestReadPage(t *testing.T) {
	// write to page
	var pageId int32 = 1
	cellCount := 3
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{dataFile: dbFile}

	dm, err := NewDiskManager(config)
	if err != nil || dm == nil {
		t.Fatal("Could not initiate disk manager")
	}
	defer dm.Close()

	newPage := NewTestPage(pageId)
	if newPage == nil {
		t.Fatal("Expected new Page, got nil")
	}

	// add tuples
	cellData := make([]byte, 13)
	cellOffset := make([]byte, CELL_POINTER_SIZE_BYTE)
	currOff := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES
	itemCount := binary.LittleEndian.Uint32(newPage.pgeData[17:21])
	for i := range cellCount {
		key := fmt.Appendf(make([]byte, 0), "key_%d", i)
		val := fmt.Appendf(make([]byte, 0), "val_%d", i)
		klen := len(key)
		vlen := len(val)

		cellData[0] = 0x32
		binary.LittleEndian.PutUint32(cellData[1:5], uint32(klen))
		binary.LittleEndian.PutUint32(cellData[5:9], uint32(vlen))
		cellData = append(cellData, key...)
		cellData = append(cellData, val...)

		currOff -= len(cellData)
		copy(newPage.pgeData[currOff:(currOff+len(cellData))], cellData)

		// add cell offset
		binary.LittleEndian.PutUint32(cellOffset[1:5], uint32(currOff))
		startOff := HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
		copy(newPage.pgeData[startOff:startOff+CELL_POINTER_SIZE_BYTE], cellOffset)

		// increase count
		itemCount++
		binary.LittleEndian.PutUint32(newPage.pgeData[17:21], itemCount)

		cellData = make([]byte, 13)
	}

	// write page to disk
	wrChan := make(chan int32)
	err = dm.WriteReq(newPage, newPage.PageId, wrChan, nil)
	if err != nil {
		t.Fatalf("Expected no error on WriteReq, got %s", err.Error())
	}
	<-wrChan

	// read page
	pageChan := make(chan *Page)
	err = dm.ReadReq(uint32(pageId), &pageChan)

	if err != nil {
		t.Fatalf("Expected no error while reading page, got %s", err.Error())
	}

	testTimeout := 200
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)
	var readPage *Page

	// wait for read to complete, or time limit to run out.
loop:
	for {
		select {
		case readPage = <-pageChan:
			cancel()
			break loop
		case <-ctx.Done():
			t.Fatalf("Writing to disk timed out after %v", testTimeout)
		}
	}

	if readPage == nil {
		t.Fatalf("Expected read  page, got nil")
	}

	if len(readPage.pgeData) < PAGE_SIZE_BYTES {
		t.Fatalf("no data in read page")
	}

	t.Run("read_itemcount", func(t *testing.T) {
		currCount := binary.LittleEndian.Uint32(readPage.pgeData[17:21])

		if currCount != itemCount {
			t.Fatalf("Expected item count of %d, got %d", currCount, itemCount)
		}
	})

	t.Run("read_pageid", func(t *testing.T) {
		id := binary.LittleEndian.Uint32(readPage.pgeData[1:5])
		if newPage.PageId != id {
			t.Fatalf("Expected page ID %d but got %d", newPage.PageId, id)
		}
	})

	// check page contents
	t.Run("test_read", func(t *testing.T) {
		c := binary.LittleEndian.Uint32(readPage.pgeData[17:21])

		for i := range c {
			// read offset, ignore flags(initial byte)
			cellOff := binary.LittleEndian.Uint32(
				readPage.pgeData[HEADER_SIZE_BYTES+(i*CELL_POINTER_SIZE_BYTE)+1 : HEADER_SIZE_BYTES+(i*CELL_POINTER_SIZE_BYTE)+CELL_POINTER_SIZE_BYTE])

			if cellOff == 0 {
				t.Fatalf("Invalid cell offset: %d", cellOff)
			}

			// read tuple at offset
			key, val, err := readTuple(readPage, cellOff)
			if err != nil {
				t.Fatal(err.Error())
			}

			kExpected := fmt.Sprintf("key_%d", i)
			vExpected := fmt.Sprintf("val_%d", i)
			if string(key) != kExpected {
				t.Fatalf("Expected key %s, got %s, %v", kExpected, key, key)
			}

			if string(val) != vExpected {
				t.Fatalf("Expected val %s, got %s", vExpected, val)
			}
		}
	})

}

func TestReadWriteConcurrent(t *testing.T) {
	pageCount := 200
	cellPerPage := 50

	var wg sync.WaitGroup
	var currPageId int32 = 1

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{dataFile: dbFile}

	dm, err := NewDiskManager(config)
	if err != nil || dm == nil {
		t.Fatal("Could not initiate disk manager")
	}
	defer dm.Close()

	// write pages
	pages := make([]*Page, 0)
	for range pageCount {
		p := NewTestPage(currPageId)

		// Add tuples
		cellData := make([]byte, 13)
		cellOffset := make([]byte, CELL_POINTER_SIZE_BYTE)
		currOff := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES
		itemCount := binary.LittleEndian.Uint32(p.pgeData[17:21])
		for i := range cellPerPage {
			key := fmt.Appendf(make([]byte, 0), "key_%d", i)
			val := fmt.Appendf(make([]byte, 0), "val_%d", i)
			klen := len(key)
			vlen := len(val)

			cellData[0] = 0x32
			binary.LittleEndian.PutUint32(cellData[1:5], uint32(klen))
			binary.LittleEndian.PutUint32(cellData[5:9], uint32(vlen))
			cellData = append(cellData, key...)
			cellData = append(cellData, val...)

			currOff -= len(cellData)
			copy(p.pgeData[currOff:(currOff+len(cellData))], cellData)

			// add cell offset
			binary.LittleEndian.PutUint32(cellOffset[1:5], uint32(currOff))
			startOff := HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
			copy(p.pgeData[startOff:startOff+CELL_POINTER_SIZE_BYTE], cellOffset)

			// increase count
			itemCount++
			binary.LittleEndian.PutUint32(p.pgeData[17:21], itemCount)

			cellData = make([]byte, 13)
		}

		pages = append(pages, p)
		currPageId++
	}

	writeStart := make(chan struct{})
	for i, pge := range pages {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("testreadwriteconcurr_write_%d", i), func(t *testing.T) {
				<-writeStart
				wrChan := make(chan int32)
				err = dm.WriteReq(pge, pge.PageId, wrChan, nil)
				if err != nil {
					t.Fatalf("Expected no error on WriteReq, got %s", err.Error())
				}

				testTimeout := 200
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

			loop:
				for {
					select {
					case n := <-wrChan:
						cancel()
						t.Logf("Written %d bytes", n)
						break loop
					case <-ctx.Done():
						t.Errorf("Writing to disk timed out after %v", testTimeout)
					}
				}
			})
		}()
	}

	close(writeStart)
	wg.Wait()

	// read page content
	readStart := make(chan struct{})
	for i, pge := range pages {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("testreadwriteconcurr_read_%d", i), func(t *testing.T) {
				<-readStart
				pageChan := make(chan *Page)
				err = dm.ReadReq(pge.PageId, &pageChan)
				if err != nil {
					t.Fatalf("Expected no error while reading page, got %s", err.Error())
				}

				testTimeout := 200
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)
				var readPage *Page

				// wait for read to complete, or time limit to run out.
			loop:
				for {
					select {
					case readPage = <-pageChan:
						cancel()
						break loop
					case <-ctx.Done():
						t.Fatalf("Writing to disk timed out after %v", testTimeout)
					}
				}

				if readPage == nil {
					t.Fatalf("Expected read page, got nil")
				}

				if len(readPage.pgeData) < PAGE_SIZE_BYTES {
					t.Fatalf("no data in read page")
				}

				// check page contents
				t.Run("test_read", func(t *testing.T) {
					// item count
					c := binary.LittleEndian.Uint32(readPage.pgeData[17:21])

					for i := range c {
						// read offset, ignore flags(initial byte)
						cellOff := binary.LittleEndian.Uint32(
							readPage.pgeData[HEADER_SIZE_BYTES+(i*CELL_POINTER_SIZE_BYTE)+1 : HEADER_SIZE_BYTES+(i*CELL_POINTER_SIZE_BYTE)+CELL_POINTER_SIZE_BYTE])

						if cellOff == 0 {
							t.Fatalf("Invalid cell offset: %d", cellOff)
						}

						// read tuple at offset
						key, val, err := readTuple(readPage, cellOff)
						if err != nil {
							t.Fatal(err.Error())
						}

						kExpected := fmt.Sprintf("key_%d", i)
						vExpected := fmt.Sprintf("val_%d", i)
						if string(key) != kExpected {
							t.Fatalf("Expected key %s, got %s, %v", kExpected, key, key)
						}

						if string(val) != vExpected {
							t.Fatalf("Expected val %s, got %s", vExpected, val)
						}
					}
				})

			})
		}()
	}

	close(readStart)
	wg.Wait()
}

func readTuple(p *Page, offset uint32) (k []byte, v []byte, err error) {
	kSize := binary.LittleEndian.Uint32(p.pgeData[offset+1 : offset+5])
	vSize := binary.LittleEndian.Uint32(p.pgeData[offset+5 : offset+9])

	if kSize <= 0 {
		return nil, nil, DiskioError{Message: fmt.Sprintf("Invalid key size:: %d", kSize)}
	}

	if vSize <= 0 {
		return nil, nil, DiskioError{Message: fmt.Sprintf("Invalid val size:: %d", vSize)}
	}

	key := p.pgeData[offset+13 : offset+13+kSize]
	val := p.pgeData[offset+13+kSize : offset+13+kSize+vSize]
	return key, val, nil
}
