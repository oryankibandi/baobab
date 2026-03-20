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

	"github.com/oryankibandi/baobab/pkg/helpers"
)

func TestNewDiskManager(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{DataFile: dbFile}

	// test no config
	t.Run("test_newdiskmanager_badconfig", func(t *testing.T) {
		_, err := NewDiskManager(DiskManagerConfig{})

		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		if !errors.As(err, &DiskManagerError{}) {
			t.Fatalf("Expected DiskManagerError, got %v", err)
		}

		helpers.PrintSuccessMsg("test_newdiskmanager_badconfig success")
	})

	// test good config
	t.Run("test_newdiskmanager_goodconfig", func(t *testing.T) {
		dm, err := NewDiskManager(config)

		if err != nil || dm == nil {
			helpers.PrintTestErrorMsg("Could not initiate disk manager", t)
		}

		dm.Close()
		helpers.PrintSuccessMsg("test_newdiskmanager_goodconfig success")
	})

}

func TestWritePage(t *testing.T) {
	var wg sync.WaitGroup
	var pageId int32 = 1
	pageSize := 8192
	cacheLine := 64 // most systems have 64 byte cache line
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{DataFile: dbFile}

	t.Run("test_writepage", func(t *testing.T) {
		dm, err := NewDiskManager(config)

		if err != nil || dm == nil {
			helpers.PrintTestErrorMsg("Could not initiate disk manager", t)
		}

		defer dm.Close()

		newPageBuff := make([]byte, pageSize)

		// fill with 1s
		for i := range pageSize / cacheLine {
			start := cacheLine * i
			end := start + cacheLine
			wg.Add(1)
			go func(arr []byte) {
				defer wg.Done()
				for j := range arr {
					arr[j] |= 1
				}
			}(newPageBuff[start:end])
		}
		wg.Wait()

		errChan := make(chan error)
		err = dm.WriteReq(uint32(pageSize)*uint32(pageId), &newPageBuff, int64(pageSize), false, &errChan)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error on WriteReq, got %s", err.Error()), t)
		}

		testTimeout := 200
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

	loop:
		for {
			select {
			case e := <-errChan:
				cancel()
				if e != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write: %v"+helpers.RESET, e.Error()), t)
				}
				break loop
			case <-ctx.Done():
				helpers.PrintTestErrorMsg(fmt.Sprintf("Writing to disk timed out after %vms\n", testTimeout), t)
			}
		}

		// check file
		s, err := os.Stat(dbFile)
		if err != nil {
			t.Fatalf("File not created: %s", err.Error())
		}

		expectedFileSize := int32(pageSize) * (pageId + 1)
		actualSize := s.Size()
		if actualSize != int64(expectedFileSize) {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected file size of %d bytes, got %d bytes", expectedFileSize, actualSize), t)
		}

		helpers.PrintSuccessMsg("test_writepage success")
	})
}

func TestWritePageConcurrent(t *testing.T) {
	pageCount := 100
	pageSize := 8192
	var pageId int32 = 1
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{DataFile: dbFile}

	dm, err := NewDiskManager(config)

	if err != nil || dm == nil {
		t.Fatal("Could not initiate disk manager")
	}

	defer dm.Close()

	pages := make([][]byte, pageCount)

	for i := range pageCount {
		pages[i] = make([]byte, pageSize)
		pageId++
	}

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range pages {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			t.Run(fmt.Sprintf("test_write_concurrent_%d", idx), func(t *testing.T) {
				<-start
				// idx starts at 0, our writable pages start at 1 hence idx + 1. offset 0 reserved for metadata page
				pageOff := uint32((idx + 1) * pageSize)
				errChan := make(chan error)
				err := dm.WriteReq(pageOff, &pages[idx], int64(pageSize), false, &errChan)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error on WriteReq, got %s", err.Error()), t)
				}

				testTimeout := 200
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

			loop:
				for {
					select {
					case e := <-errChan:
						cancel()
						if e != nil {
							helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write: %v"+helpers.RESET, e.Error()), t)
						}
						break loop
					case <-ctx.Done():
						helpers.PrintTestErrorMsg(fmt.Sprintf("Writing to disk timed out after %vms\n", testTimeout), t)
					}
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("test_write_concurrent_%d success", idx))
			})
		}(i)
	}

	// start goroutines concurrently
	close(start)
	wg.Wait()

	t.Run("test_write_concurrent_filecheck", func(t *testing.T) {
		s, err := os.Stat(dbFile)
		if err != nil {
			t.Fatalf("File not created: %s", err.Error())
		}

		expectedFileSize := int32(pageSize) * (pageId)
		actualSize := s.Size()
		if actualSize != int64(expectedFileSize) {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected file size of %d bytes, got %d bytes", expectedFileSize, actualSize), t)
		}

		helpers.PrintSuccessMsg("test_write_concurrent_filecheck success")
	})

}

func TestReadPage(t *testing.T) {
	var pageId int32 = 1
	pageSize := 8192
	cellCount := 3
	cellPtrSize := 5
	lowerPadd := 16
	hdrSize := 51

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	config := DiskManagerConfig{DataFile: dbFile}

	dm, err := NewDiskManager(config)
	if err != nil || dm == nil {
		t.Fatal("Could not initiate disk manager")
	}
	defer dm.Close()

	newPageBuff := make([]byte, pageSize)
	if newPageBuff == nil {
		t.Fatal("Expected new Page, got nil")
	}

	// add tuples
	cellData := make([]byte, 13)
	cellOffset := make([]byte, cellPtrSize)
	currOff := pageSize - lowerPadd
	itemCount := binary.LittleEndian.Uint32(newPageBuff[17:21])
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
		copy(newPageBuff[currOff:(currOff+len(cellData))], cellData)

		// add cell offset
		binary.LittleEndian.PutUint32(cellOffset[1:5], uint32(currOff))
		startOff := hdrSize + (i * cellPtrSize)
		copy(newPageBuff[startOff:startOff+cellPtrSize], cellOffset)

		// increase count
		itemCount++
		binary.LittleEndian.PutUint32(newPageBuff[17:21], itemCount)

		cellData = make([]byte, 13)
	}

	// write page to disk
	t.Run("test_readpage_write", func(t *testing.T) {
		errChan := make(chan error)
		err = dm.WriteReq(uint32(pageId*int32(pageSize)), &newPageBuff, int64(pageSize), false, &errChan)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error scheduling write req, got %s", err.Error()), t)
		}
		err = <-errChan
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error writing to disk, got %s", err.Error()), t)
		}

		helpers.PrintSuccessMsg("test_readpage_write success")
	})

	// read page
	t.Run("test_readpage_read", func(t *testing.T) {
		readErr := make(chan error)
		readPage := make([]byte, pageSize)

		err = dm.ReadReq(&readPage, uint32(pageId*int32(pageSize)), &readErr)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error while reading page, got %s", err.Error()), t)
		}

		testTimeout := 200
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

		// wait for read to complete, or time limit to run out.
	loop:
		for {
			select {
			case err := <-readErr:
				cancel()
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to read page: %v", err), t)
				}
				break loop
			case <-ctx.Done():
				helpers.PrintTestErrorMsg(fmt.Sprintf("Writing to disk timed out after %vms\n", testTimeout), t)
			}
		}

		t.Run("read_itemcount", func(t *testing.T) {
			currCount := binary.LittleEndian.Uint32(readPage[17:21])

			if currCount != itemCount {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected item count of %d, got %d", currCount, itemCount), t)
			}
		})

		t.Run("read_pageid", func(t *testing.T) {
			id := binary.LittleEndian.Uint32(readPage[1:5])
			expectedId := binary.LittleEndian.Uint32(newPageBuff[1:5])
			if expectedId != id {
				helpers.PrintTestErrorMsg(fmt.Sprintf("Expected page ID %d but got %d", expectedId, id), t)
			}
		})

		// check page contents
		t.Run("test_read", func(t *testing.T) {
			numItems := binary.LittleEndian.Uint32(readPage[17:21])

			for i := range numItems {
				// read offset, ignore flags(initial byte)
				cellOff := binary.LittleEndian.Uint32(
					readPage[uint32(hdrSize)+(i*uint32(cellPtrSize))+1 : uint32(hdrSize)+(i*uint32(cellPtrSize))+uint32(cellPtrSize)])

				if cellOff == 0 {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Invalid cell offset: %d", cellOff), t)
				}

				// read tuple at offset
				key, val, err := readTuple(&readPage, cellOff)
				if err != nil {
					t.Fatal(err.Error())
				}

				kExpected := fmt.Sprintf("key_%d", i)
				vExpected := fmt.Sprintf("val_%d", i)
				if string(key) != kExpected {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected key %s, got %s, %v", kExpected, key, key), t)
				}

				if string(val) != vExpected {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected val %s, got %s", vExpected, val), t)
				}
			}
		})

		helpers.PrintSuccessMsg("test_readpage_read success")
	})

}

func TestReadWriteConcurrent(t *testing.T) {
	pageSize := 8192
	cellPtrSize := 5
	lowerPadd := 16
	hdrSize := 51

	tests := []struct {
		name         string
		pageCount    uint32
		cellsPerPage uint32
	}{
		{name: "50_page_20_cells_per_page", pageCount: 50, cellsPerPage: 20},
		{name: "150_page_50_cells_per_page", pageCount: 150, cellsPerPage: 50},
		{name: "250_page_80_cells_per_page", pageCount: 250, cellsPerPage: 80},
		{name: "300_page_150_cells_per_page", pageCount: 300, cellsPerPage: 150},
		{name: "500_page_200_cells_per_page", pageCount: 500, cellsPerPage: 200},
		{name: "50_page_800_cells_per_page", pageCount: 50, cellsPerPage: 800},
	}

	for j, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			timeStart := time.Now()
			var wg sync.WaitGroup
			var currPageId int32 = 1

			pageCount := 200
			cellPerPage := 50

			dir := t.TempDir()
			dbFile := filepath.Join(dir, fmt.Sprintf("baobab_%d.db", j))
			config := DiskManagerConfig{DataFile: dbFile}

			dm, err := NewDiskManager(config)
			if err != nil || dm == nil {
				helpers.PrintTestErrorMsg("Could not initiate disk manager", t)
			}
			defer dm.Close()

			// write pages
			pages := make([][]byte, pageCount)
			for range pageCount {
				p := make([]byte, pageSize)

				// Add tuples
				cellData := make([]byte, 13)
				cellOffset := make([]byte, cellPtrSize)
				currOff := pageSize - lowerPadd
				itemCount := binary.LittleEndian.Uint32(p[17:21])
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
					copy(p[currOff:(currOff+len(cellData))], cellData)

					// add cell offset
					binary.LittleEndian.PutUint32(cellOffset[1:5], uint32(currOff))
					startOff := hdrSize + (i * cellPtrSize)
					copy(p[startOff:startOff+cellPtrSize], cellOffset)

					// increase count
					itemCount++
					binary.LittleEndian.PutUint32(p[17:21], itemCount)

					cellData = make([]byte, 13)
				}

				pages = append(pages, p)
				currPageId++
			}

			writeStart := make(chan struct{})
			for i := range pages {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					t.Run(fmt.Sprintf("testreadwriteconcurr_write_%d", idx), func(t *testing.T) {
						<-writeStart
						errChan := make(chan error)
						pgeOff := uint32((idx + 1) * pageSize)
						err := dm.WriteReq(pgeOff, &(pages[i]), int64(pageSize), false, &errChan)
						if err != nil {
							helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error on WriteReq, got %s", err.Error()), t)
						}

						testTimeout := 200
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

					loop:
						for {
							select {
							case e := <-errChan:
								cancel()
								if e != nil {
									helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write: %v"+helpers.RESET, e.Error()), t)
								}
								break loop
							case <-ctx.Done():
								helpers.PrintTestErrorMsg(fmt.Sprintf("Writing to disk timed out after %vms\n", testTimeout), t)
							}
						}

						helpers.PrintSuccessMsg(fmt.Sprintf("testreadwriteconcurr_write_%d success", idx))
					})
				}(i)
			}

			close(writeStart)
			wg.Wait()

			// read page content
			readStart := make(chan struct{})
			for i := range pages {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					t.Run(fmt.Sprintf("testreadwriteconcurr_read_%d", idx), func(t *testing.T) {
						<-readStart
						readPage := make([]byte, pageSize)
						readErr := make(chan error)
						pgeOff := uint32((idx + 1) * pageSize)
						err := dm.ReadReq(&readPage, pgeOff, &readErr)
						if err != nil {
							helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error while reading page, got %s", err.Error()), t)
						}

						testTimeout := 200
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(testTimeout)*time.Millisecond)

						// wait for read to complete, or time limit to run out.
					loop:
						for {
							select {
							case err := <-readErr:
								cancel()
								if err != nil {
									t.Fatalf(helpers.BOLDRED+"Unable to read page: %v"+helpers.RESET, err)
								}
								break loop
							case <-ctx.Done():
								helpers.PrintTestErrorMsg(fmt.Sprintf("Writing to disk timed out after %vms\n", testTimeout), t)
							}
						}

						if readPage == nil {
							helpers.PrintTestErrorMsg("Expected read page, got nil", t)
						}

						if len(readPage) < pageSize {
							helpers.PrintTestErrorMsg("no data in read page", t)
						}

						// check page contents
						t.Run(fmt.Sprintf("testreadwriteconcurr_read_%d_read", idx), func(t *testing.T) {
							// item count
							numItems := binary.LittleEndian.Uint32(readPage[17:21])

							for i := range numItems {
								// read offset, ignore flags(initial byte)
								cellOff := binary.LittleEndian.Uint32(
									readPage[uint32(hdrSize)+(i*uint32(cellPtrSize))+1 : uint32(hdrSize)+uint32(i*uint32(cellPtrSize))+uint32(cellPtrSize)])

								if cellOff == 0 {
									helpers.PrintTestErrorMsg(fmt.Sprintf("Invalid cell offset: %d", cellOff), t)
								}

								// read tuple at offset
								key, val, err := readTuple(&readPage, cellOff)
								if err != nil {
									helpers.PrintTestErrorMsg(err.Error(), t)
								}

								kExpected := fmt.Sprintf("key_%d", i)
								vExpected := fmt.Sprintf("val_%d", i)
								if string(key) != kExpected {
									helpers.PrintTestErrorMsg(fmt.Sprintf("Expected key %s, got %s, %v", kExpected, key, key), t)
								}

								if string(val) != vExpected {
									helpers.PrintTestErrorMsg(fmt.Sprintf("Expected val %s, got %s", vExpected, val), t)
								}
							}
						})
						helpers.PrintSuccessMsg(fmt.Sprintf("testreadwriteconcurr_read_%d success", idx))
					})
				}(i)
			}

			close(readStart)
			wg.Wait()

			dur := time.Since(timeStart)
			helpers.PrintSuccessMsg(fmt.Sprintf("%s %d tests successful in %v", test.name, test.pageCount, dur))

		})
	}
}

func TestMain(m *testing.M) {
	success := m.Run()
	if success == 0 {
		helpers.PrintSuccessMsg("Disk manager tests run successfully")
	} else {
		helpers.PrintErrorMsg("Some tests failed")
	}
}

func readTuple(pBuff *[]byte, offset uint32) (k []byte, v []byte, err error) {
	kSize := binary.LittleEndian.Uint32((*pBuff)[offset+1 : offset+5])
	vSize := binary.LittleEndian.Uint32((*pBuff)[offset+5 : offset+9])

	if kSize <= 0 {
		return nil, nil, DiskManagerError{Message: fmt.Sprintf("Invalid key size:: %d", kSize)}
	}

	if vSize <= 0 {
		return nil, nil, DiskManagerError{Message: fmt.Sprintf("Invalid val size:: %d", vSize)}
	}

	key := (*pBuff)[offset+13 : offset+13+kSize]
	val := (*pBuff)[offset+13+kSize : offset+13+kSize+vSize]
	return key, val, nil
}
