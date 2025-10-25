package diskio

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
)

const (
	ENTRY_SIZE_BYTES   = 8
	METADATA_SIZE_BYTE = 5
)

var LookupTable TLookupTable

// Metadata Layout
// 0               1              5
// +---------------+--------------+
// | Flag [byte] | count [int]    |
// +---------------+--------------+
type TLookupTable struct {
	size  uint32
	count uint32
	items map[int]*LookupItem
	fd    *os.File
	mu    sync.RWMutex
}

// Entry Layout
// 0               4              8
// +---------------+--------------+
// | Page ID [int] | offset [int] |
// +---------------+--------------+
type LookupItem struct {
	pageID       uint32
	dataOffset   uint32 // Offset in the data file
	lookupOffset uint32 // offset in the lookup table file
}

func InitLookupTable() {
	fd, err := os.OpenFile("data_lt", os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		fmt.Println("ERR while opening file")
		log.Fatal(err)
	}

	LookupTable = TLookupTable{
		fd:    fd,
		items: make(map[int]*LookupItem),
	}

	LookupTable.setSize()

	// set count
	if LookupTable.size >= METADATA_SIZE_BYTE {
		err = LookupTable.readMetadata()

		if err != nil {
			log.Fatal(err.Error())
		}
	} else {
		err = LookupTable.flushMetadata()

		if err != nil {
			log.Fatal(err.Error())
		}
	}

	itemsCount := LookupTable.count
	fmt.Println("ITEM COUNT => ", itemsCount)
	// Load items
	for i := range itemsCount {
		//fmt.Println("ITERATION: ", i)
		loaded, err := LookupTable.loadLookupItem(uint32(METADATA_SIZE_BYTE) + uint32(ENTRY_SIZE_BYTES*i))

		// If encounters empty spot, do not count this iteration
		// TODO: Rewrite to defragment file
		if !loaded {
			itemsCount++
		}

		if err != nil {
			log.Panic(err.Error())
		}
	}

	// log.Println("Loaded item to lookup table: ", LookupTable)

}

func (t *TLookupTable) flushMetadata() error {
	if t.fd == nil {
		return DiskioError{Message: "File descriptor not set."}
	}

	metadata := make([]byte, METADATA_SIZE_BYTE)

	metadata[0] = byte(0)
	binary.LittleEndian.PutUint32(metadata[1:5], t.count)

	_, err := LookupTable.fd.WriteAt(metadata, 0)

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to persist metadata: %s", err.Error()))
	}

	log.Println("Persisted metadata")

	if t.size < METADATA_SIZE_BYTE {
		t.size += METADATA_SIZE_BYTE
	}

	t.fd.Sync()
	return nil

}

// Reads size of lookup table file and sets it on LookupTable
func (t *TLookupTable) setSize() error {
	if t.fd == nil {
		return DiskioError{Message: "File descriptor not set."}
	}

	info, err := t.fd.Stat()

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to get Size of data file: ", err.Error()))
	}

	t.size = uint32(info.Size())

	log.Printf("size: %d bytes\n", t.size)

	return nil
}

// Reads metadata 0 - 5 byte and sets `count` field
func (t *TLookupTable) readMetadata() error {
	if t.fd == nil {
		return DiskioError{Message: "File descriptor not set."}
	}

	metadata := make([]byte, METADATA_SIZE_BYTE)

	_, err := t.fd.Read(metadata)

	if err != nil {
		return DiskioError{Message: fmt.Sprintf("Unable to read metadata: %s", err.Error())}
	}

	t.count = binary.LittleEndian.Uint32(metadata[1:])

	return nil
}

// Reads lookup item from disk and adds to LookupTable items map
func (t *TLookupTable) loadLookupItem(offset uint32) (bool, error) {
	if t.fd == nil {
		return false, DiskioError{Message: "File descriptor not set."}
	}

	// read from file, create struct and add to item list
	i := make([]byte, ENTRY_SIZE_BYTES)
	_, err := t.fd.ReadAt(i, int64(offset))

	if err != nil {
		return false, DiskioError{Message: fmt.Sprintf("Unable to read item at offset %d: %s", offset, err.Error())}
	}

	pageId := binary.LittleEndian.Uint32(i[:4])
	pageOff := binary.LittleEndian.Uint32(i[4:])
	//fmt.Println("READ DATA AT LOOKUP FILE ==> ", i)

	// 0 filled space indicates deleted item(not yet vaccumed)
	if pageId == 0 || pageOff == 0 {
		return false, nil
	}

	item := LookupItem{
		pageID:       pageId,
		dataOffset:   pageOff,
		lookupOffset: offset,
	}

	t.items[int(pageId)] = &item

	return true, nil
}

func (t *TLookupTable) GetPageOffset(pageId int) (int32, error) {
	if t == nil {
		return 0, DiskioError{Message: "Lookup Table not yet initialized"}
	}

	t.mu.RLock()
	val, ok := t.items[pageId]
	t.mu.RUnlock()

	if !ok {
		return -1, nil
	}

	return int32(val.dataOffset), nil
}

// Add or update item in lookup table
func (t *TLookupTable) AddPageOffset(pageId int, offset uint32) (bool, error) {
	if t == nil {
		return false, DiskioError{Message: "Lookup Table not yet initialized"}
	}
	t.mu.RLock()

	_, ok := t.items[pageId]

	if ok {
		// Update
		t.items[pageId].dataOffset = offset

		return true, nil
	}

	// calculate offset in lookup table
	newItem := LookupItem{
		pageID:       uint32(pageId),
		dataOffset:   offset,
		lookupOffset: t.size,
	}
	t.mu.RUnlock()

	// upgrade to exclusive lock
	t.mu.Lock()
	t.items[pageId] = &newItem

	// update metadata
	t.count += 1
	t.size += ENTRY_SIZE_BYTES
	t.mu.Unlock()

	// Persist
	newItem.persist()
	t.flushMetadata()

	return true, nil
}

// Deletes an item from lookup table
func (t *TLookupTable) DeletePageOffset(pageId int) error {
	if t == nil {
		return DiskioError{Message: "Lookup Table not yet initialized"}
	}

	val, ok := t.items[pageId]

	if !ok {
		return DiskioError{
			Message: "Page ID is not set",
		}
	}

	val.delete()
	delete(t.items, pageId)

	// update metadata
	if t.count > 0 {
		t.count -= 1
	}

	// TODO: Flush updated metadata

	return nil
}

func (t *TLookupTable) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	err := t.fd.Close()

	if err != nil {
		panic(err.Error())
	}
}

// Persists a lookup Item at its offset on lookup table
func (l *LookupItem) persist() {
	// write items at offset
	entry := make([]byte, ENTRY_SIZE_BYTES)

	binary.LittleEndian.PutUint32(entry[:4], l.pageID)
	binary.LittleEndian.PutUint32(entry[4:], l.dataOffset)

	fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
	fmt.Println("(LUT PERSIST) PAGE ID => ", l.pageID)
	fmt.Println("(LUT PERSIST) DATA OFFSET => ", l.dataOffset)
	fmt.Println("(LUT PERSIST) WRITE OFFSET => ", l.lookupOffset)
	fmt.Println("(LUT PERSIST) WRITE ARRAY => ", entry)
	fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

	_, err := LookupTable.fd.WriteAt(entry, int64(l.lookupOffset))

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to persist lookup item: %s", err.Error()))
	}

	LookupTable.fd.Sync()
	log.Println("Persisted lookup item")
}

// Deletes a lookup Item at its offset on lookup table by replacing content with 0s
func (l *LookupItem) delete() {
	// write items at offset
	entry := make([]byte, ENTRY_SIZE_BYTES)

	_, err := LookupTable.fd.WriteAt(entry, int64(l.lookupOffset))

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to delete lookup item: %s", err.Error()))
	}

	LookupTable.fd.Sync()
	log.Println("Persisted lookup item")
}
