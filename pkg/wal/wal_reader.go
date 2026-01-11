package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/logger"
)

// WalReader reads sections of the wal file and extracts details like log header
type WalReader struct {
	fd     *os.File // wal file decritor in Read Only mode
	mu     sync.Mutex
	logger *logger.BaobabLogger
}

// Calculates the log offset from the lsn and reads the log size info
// from the log header
func (wReader *WalReader) getLogSize(lsn []byte) (uint32, error) {
	if lsn == nil || len(lsn) < LSN_SIZE {
		return 0, WalError{Message: fmt.Sprintf("(wal reader)Invalid LSN provided: %v", lsn)}
	}
	page := binary.LittleEndian.Uint32(lsn[:4])
	inPageOffset := binary.LittleEndian.Uint32(lsn[4:])

	var off uint32

	if page > 0 {
		off = (page * WAL_PAGE_SIZE) + inPageOffset
	} else {
		off = inPageOffset
	}

	// read header
	s, err := wReader.readLogHeader(off)

	if err != nil {
		return 0, err
	}

	return s, nil
}

// Reads the header of a log at the provided offset and returns  the size of the log
func (wReader *WalReader) readLogHeader(off uint32) (uint32, error) {
	if wReader.fd == nil {
		return 0, WalError{Message: "File descriptor not provided."}
	}

	if off < 0 {
		return 0, WalError{Message: "Invalid offset provided"}
	}

	var hdr []byte
	// check if header crosses page boundary
	endOff := off + BLOG_HEADER_SIZE
	pageOff := (endOff) % WAL_PAGE_SIZE
	multiPage := pageOff >= WAL_PAGE_SIZE

	if multiPage {
		hdr = make([]byte, BLOG_HEADER_SIZE+WAL_PAGE_HEADER_SIZE)
	} else {
		hdr = make([]byte, BLOG_HEADER_SIZE)
	}

	// read header
	n, err := wReader.fd.ReadAt(hdr, int64(off))

	if err != nil && !errors.Is(err, io.EOF) {
		panic(fmt.Errorf("unable to read log header: %v", err))
	} else if errors.Is(err, io.EOF) {
		// reached end of log
		return 0, err
	}

	if n <= 0 {
		panic("invalid header size in file.")
	}

	// check if is a checkpoint entry
	if helpers.BitIsSet(hdr[0], LOGTYPE_FLAG_POS) {
		return 0, WalError{Message: "Log is a checkpoint"}
	}

	if multiPage {
		// remove WAL page header info
		idx := BLOG_HEADER_SIZE - (endOff - WAL_PAGE_SIZE)
		hdr = append(hdr[:idx], hdr[idx+WAL_PAGE_HEADER_SIZE:]...)
	}

	lSize := binary.LittleEndian.Uint32(hdr[9:13])

	return lSize, nil
}

// Creates a new wal reader
func NewWalReader(walpath string, l *logger.BaobabLogger) *WalReader {
	if len(walpath) <= 0 {
		panic("Invalid path to wal file provided")
	}

	if l == nil {
		panic("Provided logger is nil")
	}

	fd, err := os.OpenFile(walpath, os.O_CREATE|os.O_RDONLY, 0644)

	if err != nil {
		panic(err)
	}

	return &WalReader{
		fd:     fd,
		logger: l,
	}
}
