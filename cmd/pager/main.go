package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

const (
	PageSize   = 8192 // 8 KB page size
	HeaderSize = 47
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <file> <page_id> <header_bytes>", os.Args[0])
	}

	filePath := "data"

	pageID, err := strconv.Atoi(os.Args[1])
	if err != nil || pageID < 0 {
		log.Fatalf("invalid page_id: %v", err)
	}

	f, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer f.Close()

	// Compute page offset
	offset := int64(pageID) * PageSize

	// Seek to start of page
	_, err = f.Seek(offset, 0)
	if err != nil {
		log.Fatalf("failed to seek to offset %d: %v", offset, err)
	}

	// Read first N bytes (page header)
	buf := make([]byte, HeaderSize)
	readBytes, err := f.Read(buf)
	if err != nil {
		log.Fatalf("failed to read header: %v", err)
	}

	fmt.Printf("Page %d header (%d bytes):\n", pageID, readBytes)
	fmt.Printf("%v\n", buf[:readBytes])
}
