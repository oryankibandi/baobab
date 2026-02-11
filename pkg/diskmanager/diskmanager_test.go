package diskmanager

import (
	"context"
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
				wg.Done()
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
