package pager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/oryankibandi/baobab/pkg/diskmanager"
	"github.com/oryankibandi/baobab/pkg/helpers"
)

func TestNewPager(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	freelistFile := filepath.Join(dir, "baobab")

	dManConfig := diskmanager.DiskManagerConfig{
		DataFile: dbFile,
	}

	dMan, err := diskmanager.NewDiskManager(dManConfig)
	if err != nil {
		helpers.PrintTestErrorMsg("Unable to initialize disk manager", t)
	}

	type tTest struct {
		name         string
		freelistFile string
		diskMan      *diskmanager.DiskManager
		dbFile       string
		isValid      bool
	}

	tests := make([]tTest, 0)

	// no freelist file
	tests = append(tests, tTest{
		name:    "test_newpager_no_freelist",
		diskMan: dMan,
		dbFile:  dbFile,
		isValid: false,
	})

	// no diskmanager
	tests = append(tests, tTest{
		name:         "test_newpager_no_diskmanager",
		freelistFile: freelistFile,
		isValid:      false,
	})

	// no diskmanager or freelist
	tests = append(tests, tTest{
		name:    "test_newpager_no_freelist_and_diskmanager",
		isValid: false,
	})

	// valid
	dbFileValid := fmt.Sprintf("%s_valid", dbFile)
	dManConfigValid := diskmanager.DiskManagerConfig{
		DataFile: dbFileValid,
	}
	dManValid, err := diskmanager.NewDiskManager(dManConfigValid)
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to create new diskmanager: %s", err.Error()), t)
	}

	tests = append(tests, tTest{
		name:         "test_newpager_valid",
		diskMan:      dManValid,
		freelistFile: fmt.Sprintf("%s_valid", freelistFile),
		dbFile:       dbFileValid,
		isValid:      true,
	})

	for _, v := range tests {
		t.Run(v.name, func(t *testing.T) {
			pgr, err := NewPager(PagerConfig{
				FreeListFile: v.freelistFile,
				DManager:     v.diskMan,
			})

			if v.isValid {
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected no error, got %s", err.Error()), t)
				}

				if pgr == nil {
					helpers.PrintTestErrorMsg("Expected pager, got nil.", t)
				}

				// ensure that the files have been created
				var expectedFlFile string

				if len(v.freelistFile) == 0 {
					expectedFlFile = fmt.Sprintf("%s_fl", DEFAULT_FREELIST_FILE)
				} else {
					expectedFlFile = fmt.Sprintf("%s_fl", v.freelistFile)
				}

				_, err := os.Stat(expectedFlFile)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("freelist file not created: %s.", err.Error()), t)
				}

				_, err = os.Stat(v.dbFile)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("dbFile not created: %s", err.Error()), t)
				}

				pgr.Close()
				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", v.name))
			} else {
				if err == nil {
					helpers.PrintTestErrorMsg("Expected error, got nil", t)
				}

				if !errors.As(err, &PagerError{}) && !errors.As(err, &FreelistError{}) {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Expected PagerError, got %v", err), t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%s success", v.name))
			}
		})
	}
}

func TestStartWorkers(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	freelistFile := filepath.Join(dir, "baobab")

	dManConfig := diskmanager.DiskManagerConfig{
		DataFile: dbFile,
	}

	dMan, err := diskmanager.NewDiskManager(dManConfig)
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize disk manager: %s", err.Error()), t)
	}

	workerSize := 20
	pgr, err := NewPager(PagerConfig{
		FreeListFile: freelistFile,
		DManager:     dMan,
		WorkerSize:   uint64(workerSize),
	})

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize pager: %s", err.Error()), t)
	}

	t.Run("test_jobqueue_initialized", func(t *testing.T) {
		if pgr.workers.jobQ == nil {
			helpers.PrintTestErrorMsg("workers not initialized", t)
		}

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})

	pgr.Close()

	t.Run("test_worker_goroutineleak", func(t *testing.T) {
		activeWrk := runtime.NumGoroutine()
		if activeWrk >= workerSize {
			helpers.PrintTestErrorMsg(fmt.Sprintf("goroutine leak on worker pool. Expected less than %d active workers but got %d", workerSize, activeWrk), t)
		}

		helpers.PrintSuccessMsg(fmt.Sprintf("%s success", t.Name()))
	})
	helpers.PrintSuccessMsg("test_startworkers success")
}

func TestReadAndWritePage(t *testing.T) {
	var metaPage Page
	var testPage Page
	var testRootPage Page

	testPageCellCount := 50

	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	freelistFile := filepath.Join(dir, "baobab")

	dManConfig := diskmanager.DiskManagerConfig{
		DataFile: dbFile,
	}

	dMan, err := diskmanager.NewDiskManager(dManConfig)
	if err != nil {
		helpers.PrintTestErrorMsg("Unable to initialize disk manager", t)
	}

	// init pager and read metadata
	pgr, err := NewPager(PagerConfig{
		DManager:     dMan,
		FreeListFile: freelistFile,
	})

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize pager: %s", err.Error()), t)
	}

	if pgr == nil {
		helpers.PrintTestErrorMsg("Received nil pager", t)
	}
	defer pgr.Close()
	helpers.PrintSuccessMsg(fmt.Sprintf("Initialize pager. DB file -> %s", dbFile))

	// read metadata page from disk
	err = pgr.InitFromMetadata(&metaPage)
	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to read metadata: %s", err.Error()), t)
	}

	// initialize pages
	t.Run("test_initialize_page", func(t *testing.T) {
		pgeIdInternal, err := pgr.NewPage(true, true, &testRootPage)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize a new page: %s", err.Error()), t)
		}

		if pgeIdInternal == 0 {
			helpers.PrintTestErrorMsg("PageId 0 reserved for etadata page.", t)
		}

		if testRootPage.PageId != pgeIdInternal {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Curr pageId: %d not updated to new pageId: %d", testRootPage.PageId, pgeIdInternal), t)
		}

		if rootPgeId := pgr.RootPage(); rootPgeId != pgeIdInternal {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected set root page to be %d but got %d", pgeIdInternal, rootPgeId), t)
		}
		helpers.PrintInfoMsg(fmt.Sprintf("Root page assigned pageId --> %d", pgeIdInternal))

		pgeIdLeaf, err := pgr.NewPage(false, false, &testPage)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize a new page: %s", err.Error()), t)
		}

		if pgeIdLeaf == 0 {
			helpers.PrintTestErrorMsg("PageId 0 reserved for etadata page.", t)
		}

		if testPage.PageId != pgeIdLeaf {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Curr pageId: %d not updated to new pageId: %d", testPage.PageId, pgeIdLeaf), t)
		}

		// ensure page Ids are assigned monotonically
		if pgeIdLeaf <= pgeIdInternal {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected page  Id assignment to be monotonical\nGot first id: %d\nSecond Id: %d", pgeIdInternal, pgeIdLeaf), t)
		}
		helpers.PrintInfoMsg(fmt.Sprintf("Leaf page assigned pageId --> %d", pgeIdLeaf))

		helpers.PrintSuccessMsg("test_initialize_page success")
	})

	//  write page to disk
	t.Run("test_writeread_rootpage", func(t *testing.T) {
		// add cell and cell offsets
		startOff := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES

		cellCount := 0
		cellData := make([]byte, 13)
		cellPtr := make([]byte, CELL_POINTER_SIZE_BYTE)
		for i := range testPageCellCount {
			key := fmt.Appendf(make([]byte, 0), "key_%d", i)
			val := fmt.Appendf(make([]byte, 0), "val_%d", i)
			klen := len(key)
			vlen := len(val)
			cellData[0] = 0x32
			binary.LittleEndian.PutUint32(cellData[1:5], uint32(klen))
			binary.LittleEndian.PutUint32(cellData[5:9], uint32(vlen))
			cellData = append(cellData, key...)
			cellData = append(cellData, val...)

			startOff -= len(cellData)

			copy(testRootPage.pgeData[startOff:(startOff+len(cellData))], cellData)

			// write cell offset/pointer
			binary.LittleEndian.PutUint32(cellPtr[1:5], uint32(startOff))
			cellPtrOff := HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
			copy(testRootPage.pgeData[cellPtrOff:cellPtrOff+CELL_POINTER_SIZE_BYTE], cellPtr)

			cellCount++
			cellData = make([]byte, 13)
		}

		// write header
		binary.LittleEndian.PutUint32(testRootPage.pgeData[17:21], uint32(cellCount))

		// write page
		rootByteSlice := testRootPage.pgeData[:]
		err = pgr.WritePage(testRootPage.PageId, &rootByteSlice, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write test root page: %s", err.Error()), t)
		}

		// read page
		rootPageOnDiskData := make([]byte, PAGE_SIZE_BYTES)
		err = pgr.ReadPage(testRootPage.PageId, &rootPageOnDiskData)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to read test root page:: %s", err.Error()), t)
		}

		// verify page contents.
		// pageId
		if id := binary.LittleEndian.Uint32(rootPageOnDiskData[1:5]); id != testRootPage.PageId {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected pageId: %d, got %d", testRootPage.PageId, id), t)
		}

		// item count
		if c := binary.LittleEndian.Uint32(rootPageOnDiskData[17:21]); c != uint32(cellCount) {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected item count of %d, got %d", cellCount, c), t)
		}

		// cells
		var tKeySize uint32
		var tValSize uint32
		for i := range testPageCellCount {
			expectedKey := fmt.Appendf(make([]byte, 0), "key_%d", i)
			expectedVal := fmt.Appendf(make([]byte, 0), "val_%d", i)
			off := HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
			tupleOffset := binary.LittleEndian.Uint32(rootPageOnDiskData[off+1 : (off + 1 + CELL_POINTER_SIZE_BYTE)])

			// read tuple
			tKeySize = binary.LittleEndian.Uint32(rootPageOnDiskData[tupleOffset+1 : tupleOffset+5])
			tValSize = binary.LittleEndian.Uint32(rootPageOnDiskData[tupleOffset+5 : tupleOffset+9])

			key := rootPageOnDiskData[tupleOffset+13 : tupleOffset+13+tKeySize]
			val := rootPageOnDiskData[tupleOffset+13+tKeySize : tupleOffset+13+tKeySize+tValSize]

			// ensure key and val match expected
			if !bytes.Equal(key, expectedKey) {
				helpers.PrintTestErrorMsg(fmt.Sprintf("%d: Expected key %s but got %s", i, expectedKey, key), t)
			}

			if !bytes.Equal(val, expectedVal) {
				helpers.PrintTestErrorMsg(fmt.Sprintf("%d: Expected val %s but got %s", i, expectedVal, val), t)
			}
		}

		helpers.PrintSuccessMsg("test_writeread_rootpage success")
	})

	// non-root leaf page
	t.Run("test_writeread_normalpage", func(t *testing.T) {
		// add cell and cell offsets
		startOff := PAGE_SIZE_BYTES - LOWER_PADDING_BYTES

		cellCount := 0
		cellData := make([]byte, 13)
		cellPtr := make([]byte, CELL_POINTER_SIZE_BYTE)
		for i := range testPageCellCount {
			key := fmt.Appendf(make([]byte, 0), "key_%d", i)
			val := fmt.Appendf(make([]byte, 0), "val_%d", i)
			klen := len(key)
			vlen := len(val)
			cellData[0] = 0x32
			binary.LittleEndian.PutUint32(cellData[1:5], uint32(klen))
			binary.LittleEndian.PutUint32(cellData[5:9], uint32(vlen))
			cellData = append(cellData, key...)
			cellData = append(cellData, val...)

			startOff -= len(cellData)

			copy(testPage.pgeData[startOff:(startOff+len(cellData))], cellData)

			// write cell offset/pointer
			binary.LittleEndian.PutUint32(cellPtr[1:5], uint32(startOff))
			cellPtrOff := HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
			copy(testPage.pgeData[cellPtrOff:cellPtrOff+CELL_POINTER_SIZE_BYTE], cellPtr)

			cellCount++
			cellData = make([]byte, 13)
		}

		// write header
		binary.LittleEndian.PutUint32(testPage.pgeData[17:21], uint32(cellCount))

		// write page
		pageByteSlice := testPage.pgeData[:]
		err = pgr.WritePage(testPage.PageId, &pageByteSlice, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write test root page:: %s", err.Error()), t)
		}

		// read page
		testPageOnDiskData := make([]byte, PAGE_SIZE_BYTES)
		err = pgr.ReadPage(testRootPage.PageId, &testPageOnDiskData)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to read test root page:: %s", err.Error()), t)
		}

		// verify page contents.
		// pageId
		if id := binary.LittleEndian.Uint32(testPageOnDiskData[1:5]); id != testRootPage.PageId {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected pageId: %d, got %d", testRootPage.PageId, id), t)
		}

		// item count
		if c := binary.LittleEndian.Uint32(testPageOnDiskData[17:21]); c != uint32(cellCount) {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected item count of %d, got %d", cellCount, c), t)
		}

		// cells
		var tKeySize uint32
		var tValSize uint32
		for i := range testPageCellCount {
			expectedKey := fmt.Appendf(make([]byte, 0), "key_%d", i)
			expectedVal := fmt.Appendf(make([]byte, 0), "val_%d", i)
			off := HEADER_SIZE_BYTES + (i * CELL_POINTER_SIZE_BYTE)
			tupleOffset := binary.LittleEndian.Uint32(testPageOnDiskData[off+1 : (off + 1 + CELL_POINTER_SIZE_BYTE)])

			// read tuple
			tKeySize = binary.LittleEndian.Uint32(testPageOnDiskData[tupleOffset+1 : tupleOffset+5])
			tValSize = binary.LittleEndian.Uint32(testPageOnDiskData[tupleOffset+5 : tupleOffset+9])

			key := testPageOnDiskData[tupleOffset+13 : tupleOffset+13+tKeySize]
			val := testPageOnDiskData[tupleOffset+13+tKeySize : tupleOffset+13+tKeySize+tValSize]

			// ensure key and val match expected
			if !bytes.Equal(key, expectedKey) {
				helpers.PrintTestErrorMsg(fmt.Sprintf("%d: Expected key %s but got %s", i, expectedKey, key), t)
			}

			if !bytes.Equal(val, expectedVal) {
				helpers.PrintTestErrorMsg(fmt.Sprintf("%d: Expected val %s but got %s", i, expectedVal, val), t)
			}
		}

		helpers.PrintSuccessMsg("test_writeread_normalpage success")
	})

	//  ensure flushing metadata syncs to disk
	t.Run("test_flushmetadata", func(t *testing.T) {
		metadataBuff := make([]byte, METADATA_PAGE_SIZE_BYTES)
		err := pgr.FlushMetadata(&metadataBuff)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to flush metadata page:: %s", err.Error()), t)
		}

		// read metadata page
		metadataBuff = nil // reset slice
		metadataBuff = make([]byte, METADATA_PAGE_SIZE_BYTES)
		err = pgr.ReadPage(0, &metadataBuff)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to read metadata page: %s", err.Error()), t)
		}

		// check root page
		if n := binary.LittleEndian.Uint32(metadataBuff[0:4]); n != testRootPage.PageId {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected root page to be %d, got %d", testRootPage.PageId, n), t)
		}

		// ensure page count is correct
		if n := binary.LittleEndian.Uint32(metadataBuff[12:16]); n != 2 {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected page count to be %d, got %d", 2, n), t)
		}

		helpers.PrintSuccessMsg("test_flushmetadata success")
	})
}

// Concurrent read and write on many pages
func TestConcurrentReadWrite(t *testing.T) {
	var wg sync.WaitGroup

	readPageCount := 500
	writePageCount := 600
	cacheLine := 64

	readSet := make([]Page, readPageCount)
	writeSet := make([]Page, writePageCount)

	// initialize pager
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	freelistFile := filepath.Join(dir, "baobab")

	dManConfig := diskmanager.DiskManagerConfig{
		DataFile: dbFile,
	}

	dMan, err := diskmanager.NewDiskManager(dManConfig)
	if err != nil {
		helpers.PrintTestErrorMsg("Unable to initialize disk manager", t)
	}

	// init pager and read metadata
	pgr, err := NewPager(PagerConfig{
		DManager:     dMan,
		FreeListFile: freelistFile,
	})

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize pager: %s", err.Error()), t)
	}

	if pgr == nil {
		helpers.PrintTestErrorMsg("Received nil pager", t)
	}
	defer pgr.Close()

	// initialize & fill read set with 0s
	for i := range readPageCount {
		_, err := pgr.NewPage(false, false, &readSet[i])
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize read set page: %s", err.Error()), t)
		}

		for k := range PAGE_SIZE_BYTES / cacheLine {
			start := cacheLine * k
			end := start + cacheLine
			wg.Add(1)
			go func(arr []byte) {
				defer wg.Done()
				for j := range arr {
					arr[j] |= 1
				}
			}(readSet[i].pgeData[start:end])
		}
	}
	wg.Wait()

	// fill write set with 0s
	for i := range writePageCount {
		_, err := pgr.NewPage(false, false, &writeSet[i])
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize write set page: %s", err.Error()), t)
		}

		for k := range PAGE_SIZE_BYTES / cacheLine {
			start := cacheLine * k
			end := start + cacheLine
			wg.Add(1)
			go func(arr []byte) {
				defer wg.Done()
				for j := range arr {
					arr[j] |= 1
				}
			}(writeSet[i].pgeData[start:end])
		}
	}
	wg.Wait()

	// Write readset to disk.
	for i := range readSet {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			t.Run(fmt.Sprintf("%d_test_readwriteconcurr_writereadset", j), func(t *testing.T) {
				pgeSlice := readSet[j].pgeData[:]
				err := pgr.WritePage(readSet[j].PageId, &pgeSlice, false)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write readset page; %s", err.Error()), t)
				}
			})
		}(i)
	}
	wg.Wait()
	helpers.PrintSuccessMsg(fmt.Sprintf("Successfully written %d readset pages", readPageCount))

	// schedule read and write operations concurrently
	start := make(chan struct{})
	for i := range readPageCount {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			<-start
			t.Run(fmt.Sprintf("%d_test_readwriteconcurr_read_readset", j), func(t *testing.T) {
				readSlice := readSet[j].pgeData[:]
				err := pgr.ReadPage(readSet[j].PageId, &readSlice)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to read page: %s", err.Error()), t)
				}

				for idx, b := range readSlice {
					if b != 0x01 && idx > HEADER_SIZE_BYTES {
						helpers.PrintTestErrorMsg(fmt.Sprintf("Invalid data at index %d in read page: %b", idx, b), t)
					}
				}
				helpers.PrintSuccessMsg(fmt.Sprintf("%d_test_readwriteconcurr_read_readset success", j))
			})
		}(i)
	}

	for i := range writePageCount {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			<-start
			t.Run(fmt.Sprintf("%d_test_readwriteconcurr_write_writeset", j), func(t *testing.T) {
				writeSlice := writeSet[j].pgeData[:]
				err := pgr.WritePage(writeSet[j].PageId, &writeSlice, false)
				if err != nil {
					helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write page: %s", err.Error()), t)
				}

				helpers.PrintSuccessMsg(fmt.Sprintf("%d_test_readwriteconcurr_write_writeset success", j))
			})
		}(i)
	}

	startTime := time.Now()

	close(start)
	wg.Wait()

	dur := time.Since(startTime)

	helpers.PrintSuccessMsg(fmt.Sprintf("TestConcurrentReadWrite success. Successfully written %d pages and read %d pages concurrently in %v", writePageCount, readPageCount, dur))
}

// Test freelist
func TestFreeList(t *testing.T) {
	var page1, page2, page3 Page
	var pageId1, pageId2, pageId3 uint32

	// initialize pager
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "baobab.db")
	freelistFile := filepath.Join(dir, "baobab")

	dManConfig := diskmanager.DiskManagerConfig{
		DataFile: dbFile,
	}

	dMan, err := diskmanager.NewDiskManager(dManConfig)
	if err != nil {
		helpers.PrintTestErrorMsg("Unable to initialize disk manager", t)
	}

	// init pager and read metadata
	pgr, err := NewPager(PagerConfig{
		DManager:     dMan,
		FreeListFile: freelistFile,
	})

	if err != nil {
		helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize pager: %s", err.Error()), t)
	}

	if pgr == nil {
		helpers.PrintTestErrorMsg("Received nil pager", t)
	}
	defer pgr.Close()

	t.Run("test_freelist", func(t *testing.T) {
		// initialize the first two pages
		pageId1, err = pgr.NewPage(false, false, &page1)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize page1: %s", err.Error()), t)
		}

		pageId2, err = pgr.NewPage(false, false, &page2)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize page2: %s", err.Error()), t)
		}

		// set last byte to 1 so they can be written
		page1.pgeData[PAGE_SIZE_BYTES-1] |= 0x01
		page2.pgeData[PAGE_SIZE_BYTES-1] |= 0x01
		page3.pgeData[PAGE_SIZE_BYTES-1] |= 0x01 // page id still unassigned

		// write page1 and page2
		page1Buff := page1.pgeData[:]
		err = pgr.WritePage(pageId1, &page1Buff, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write page1: %s", err.Error()), t)
		}

		page2Buff := page2.pgeData[:]
		err = pgr.WritePage(pageId2, &page2Buff, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write page2: %s", err.Error()), t)
		}

		// delete page 1
		helpers.SetFlag(&(page1.pgeData[0]), Dead)

		// flush page1 to ensure it is persisted
		err = pgr.WritePage(pageId1, &page1Buff, false)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to write page1: %s", err.Error()), t)
		}

		if p := pgr.pageCount; p != 1 {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected page count to be %d, got %d", 1, p), t)
		}

		// initialize new page
		pageId3, err = pgr.NewPage(false, false, &page3)
		if err != nil {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Unable to initialize page3: %s", err.Error()), t)
		}

		//  pageId1 should be reassigned to page3
		if pageId3 != pageId1 {
			helpers.PrintTestErrorMsg(fmt.Sprintf("Expected pageId: %d, got %d", pageId1, pageId3), t)
		}

		helpers.PrintSuccessMsg("Freelist test successful")
	})
}
