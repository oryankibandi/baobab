package recovery

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/oryankibandi/baobab/pkg/bp_tree"
	buffermanager "github.com/oryankibandi/baobab/pkg/buffer_manager"
	"github.com/oryankibandi/baobab/pkg/helpers"
	"github.com/oryankibandi/baobab/pkg/wal"
)

const (
	RECOVERY_FILEPATH = "bb_config"
)

type RecoveryMngr struct {
	bufferMngr    *buffermanager.Cache
	BPTreeIndex   *bp_tree.BTree
	fd            *os.File // WAL file descriptor
	walSize       int64
	checkpointLSN []byte
}

type LogHeaderMetadata struct {
	lsn     []byte
	pageId  uint32
	logSize uint32

	// The multiPage field indicates if a header spans multiple pages.
	// If a log header is written across multiple pages, starts within
	// a page (within 8K) and it's length spans beyond the page size,
	// we factor in the WAL page header which occupies the first 8 bytes
	// of every page. This is useful when walking through the WAL and
	// reading the log entries so as to factor in wal page header when
	// setting the offsets.
	multiPage bool
}

type Operation struct {
	opType wal.OperationType
	key    []byte
	val    []byte
	lsn    []byte

	// The multiPage field, similar to LogHeaderMetadata,
	// indicates if a log entry spans multiple pages.
	multiPage bool
}

// Reads the logs in WAL, from the REDO point, compares page LSNs with LSN in WAL
// and reapplies the missing logs.
func (rMngr *RecoveryMngr) Recover() error {
	if rMngr.fd == nil {
		panic("(Recover) No attached file descriptor.")
	}

	stopChan := make(chan struct{})
	go helpers.StartSpinner(stopChan, "Replaying WAL...")
	defer close(stopChan)

	// time.Sleep(time.Second * 2)

	// Calculate offset of checkpoint
	var checkpointOff uint32
	checkPntPage := binary.LittleEndian.Uint32(rMngr.checkpointLSN[:4])
	checkPntPageOff := binary.LittleEndian.Uint32(rMngr.checkpointLSN[4:])

	if checkPntPage != 0 {
		checkpointOff = (checkPntPage * wal.WAL_PAGE_SIZE) + checkPntPageOff
	} else {
		checkpointOff = checkPntPageOff
	}

	fmt.Println("\n(Recover) Checkpoint page -> ", checkPntPage)
	fmt.Println("(Recover) Checkpoint page Offset -> ", checkPntPageOff)
	fmt.Println("(Recover) Calculated Checkpoint offset -> ", checkpointOff)
	// Read checkpoint
	checkPntData := make([]byte, wal.CHECKPOINT_SIZE)
	n, err := rMngr.fd.ReadAt(checkPntData, int64(checkpointOff))

	if (err != nil && !errors.Is(err, io.EOF)) || n <= 0 {
		panic(fmt.Errorf("Unable to read checkpoint: %v", err))
	}

	fmt.Println("READ CHECKPOINT DATA --> ", checkPntData)

	if n < wal.CHECKPOINT_SIZE {
		panic(fmt.Errorf("Only read %d bytes, expected %d bytes for the checkpoint", n, wal.CHECKPOINT_SIZE))
	}

	// Extract REDO point
	redoOff := binary.LittleEndian.Uint32(checkPntData[1:5])

	fmt.Println("REDO POINT: ", redoOff)
	err = rMngr.walkThroughRecovery(redoOff, uint32(rMngr.walSize))

	if err != nil {
		return err
	}

	return nil
}

// Walks through the log, from startOff to endOff, comparing each Log's LSN
// with the associated page's LSN and applying unapplied logs - logs with LSN greater than
// their respective pages.
func (rMngr *RecoveryMngr) walkThroughRecovery(startOff uint32, endOff uint32) error {
	var isMultiPage bool
	currOff := startOff

	for currOff < endOff {
		// read header
		fmt.Println("CURR oFFSET ---> ", currOff)
		headerMetadata, err := rMngr.readLogHeader(currOff)

		if err != nil {
			if errors.As(err, &InvalidLogError{}) {
				// log is a checkpoint, skip to next log
				currOff += wal.CHECKPOINT_SIZE
				log.Println("Encountered checkpoint, proceeding...")
				continue
			}

			// If reached end of wal, terminate loop
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		if headerMetadata.multiPage {
			isMultiPage = true
		}

		// retrieve page and compare LSN
		f, err := rMngr.bufferMngr.Get(headerMetadata.pageId)

		if err != nil {
			// page does not exist. Crash might have occured before
			// page was created. Reapply the entry

			log.Println("PAGE NOT FOUND....")
			op, err := rMngr.readFullLog(currOff, headerMetadata.logSize, headerMetadata.lsn)

			if err != nil {
				panic(err)
			}

			if op.multiPage {
				isMultiPage = true
			}

			rMngr.reapplyLog(op)

			// increment offset
			if isMultiPage {
				fmt.Println("Is Multipage Log entry...")
				currOff += (headerMetadata.logSize + wal.WAL_PAGE_HEADER_SIZE)
				fmt.Println("Next Offset -> ", currOff)
			} else {
				currOff += headerMetadata.logSize
			}
		} else {
			pLsn, err := f.GetLSN()

			if err != nil {
				return err
			}

			reapply, err := helpers.GreaterLSN(headerMetadata.lsn, pLsn)

			if err != nil {
				log.Println("ERR => ", err)
				return err
			}

			// if the log's LSN is greater than the page's LSN, this entry was not persisted hence we need to reapply
			if reapply {
				fmt.Println("(walkThroughRecovery) logSize  => ", headerMetadata.logSize)
				op, err := rMngr.readFullLog(currOff, headerMetadata.logSize, headerMetadata.lsn)

				if err != nil {
					panic(err)
				}

				if op.multiPage {
					isMultiPage = true
				}

				rMngr.reapplyLog(op)
			}

			// increment offset
			if isMultiPage {
				fmt.Println("Is Multipage Log entry...")
				currOff += (headerMetadata.logSize + wal.WAL_PAGE_HEADER_SIZE)
				fmt.Println("Next Offset -> ", currOff)
			} else {
				currOff += headerMetadata.logSize
			}
		}

		isMultiPage = false
	}

	return nil
}

// reaplies an operation
func (rMngr *RecoveryMngr) reapplyLog(op Operation) {
	fmt.Println("Reapplying log operation.....")
	if len(op.key) <= 0 || op.key == nil {
		panic(fmt.Errorf("(reapply) Invalid key. Received %v", op.key))
	}

	if op.opType == wal.PUT {
		if len(op.val) <= 0 || op.val == nil {
			panic(fmt.Errorf("(reapply) Invalid value. Received %v", op.val))
		}

		inserted, err := rMngr.BPTreeIndex.InsertValue([][]byte{op.key}, [][]byte{op.val}, op.lsn)

		if err != nil {
			panic(fmt.Errorf("Unable to insert value: %v", err))
		}

		if !inserted {
			panic("Could not insert the value")
		}

		fmt.Println("Insert successful....")
	} else {
		// del operation
		deleted, err := rMngr.BPTreeIndex.DeleteValue([][]byte{op.key}, op.lsn)

		if err != nil {
			panic(fmt.Errorf("Unable to delete value: %v", err))
		}

		if !deleted {
			panic("Could not insert the value")
		}
	}
}

func (rMngr *RecoveryMngr) readLogHeader(off uint32) (LogHeaderMetadata, error) {
	if rMngr.fd == nil {
		return LogHeaderMetadata{}, RecoveryError{Message: "File descriptor not provided."}
	}

	if off < 0 {
		return LogHeaderMetadata{}, RecoveryError{Message: "Invalid offset provided"}
	}

	var hdr []byte

	// check if header crosses page boundary
	endOff := off + wal.BLOG_HEADER_SIZE
	pageOff := (endOff) % wal.WAL_PAGE_SIZE
	multiPage := pageOff >= wal.WAL_PAGE_SIZE

	if multiPage {
		fmt.Println("(readLogHeader) Multipage...")
		hdr = make([]byte, wal.BLOG_HEADER_SIZE+wal.WAL_PAGE_HEADER_SIZE)
	} else {
		fmt.Println("(readLogHeader) Single page...")
		hdr = make([]byte, wal.BLOG_HEADER_SIZE)
	}

	// read header
	// fmt.Println("READING LOG AT OFFSET *********************************************> ", off)
	fmt.Println("(readLogHeader) Reading log header at offset => ", off)
	n, err := rMngr.fd.ReadAt(hdr, int64(off))

	if err != nil && !errors.Is(err, io.EOF) {
		panic(fmt.Errorf("unable to read log header: %v", err))
	} else if errors.Is(err, io.EOF) {
		// reached end of log
		return LogHeaderMetadata{}, err
	}

	if n <= 0 {
		panic("invalid header size in file.")
	}

	fmt.Println("(readLogHeader) Read log header -> ", hdr)

	// fmt.Println("LOG HEADER ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++> ", hdr)

	// check if is a checkpoint entry
	if helpers.BitIsSet(hdr[0], wal.LOGTYPE_FLAG_POS) {
		return LogHeaderMetadata{}, InvalidLogError{Message: "Log is a checkpoint"}
	}

	if multiPage {
		// remove WAL page header info
		idx := wal.BLOG_HEADER_SIZE - (endOff - wal.WAL_PAGE_SIZE)
		hdr = append(hdr[:idx], hdr[idx+wal.WAL_PAGE_HEADER_SIZE:]...)
	}

	// extract LSN, pageId and size of the log
	hdrMetadata := LogHeaderMetadata{
		lsn:       hdr[1:9],
		logSize:   binary.LittleEndian.Uint32(hdr[9:13]),
		pageId:    binary.LittleEndian.Uint32(hdr[13:17]),
		multiPage: multiPage,
	}

	// validate data
	if len(hdrMetadata.lsn) != wal.LSN_SIZE {
		panic(fmt.Errorf("Invalid LSN size Received length: %d, expected: %d", len(hdrMetadata.lsn), wal.LSN_SIZE))
	}

	if hdrMetadata.pageId <= 0 {
		panic(fmt.Errorf("Invalid page ID. Received: %d.", hdrMetadata.pageId))
	}

	return hdrMetadata, nil
}

// Reads the full log from startOff and returns the operation to be perfoemd.
func (rMngr *RecoveryMngr) readFullLog(startOff uint32, lSize uint32, lsn []byte) (Operation, error) {
	if rMngr.fd == nil {
		return Operation{}, RecoveryError{Message: "File descriptor not provided."}
	}

	if startOff < 0 {
		return Operation{}, RecoveryError{Message: "Invalid offset provided"}
	}

	var bLog []byte
	// check if header crosses page boundary
	endOff := startOff + lSize
	pageOff := (endOff) % wal.WAL_PAGE_SIZE
	multiPage := pageOff >= wal.WAL_PAGE_SIZE

	if multiPage {
		bLog = make([]byte, lSize+wal.WAL_PAGE_HEADER_SIZE)
	} else {
		bLog = make([]byte, lSize)
	}
	fmt.Println("bLog Length --> ", len(bLog))
	fmt.Println("lSize ----> ", lSize)
	fmt.Println("Reading from offset ---> ", startOff)

	// read full log
	n, err := rMngr.fd.ReadAt(bLog, int64(startOff))

	if err != nil {
		fmt.Println("(readFullLog) n -> ", n)
		fmt.Println("(readFullLog) lSize -> ", lSize)
		fmt.Println("(readFullLog) startOffset -> ", startOff)
		panic(fmt.Errorf("unable to read log: %v", err))
	}

	if n <= 0 {
		panic("invalid log in file.")
	}

	if multiPage {
		// remove WAL page header info
		idx := lSize - (endOff - wal.WAL_PAGE_SIZE)
		bLog = append(bLog[:idx], bLog[idx+wal.WAL_PAGE_HEADER_SIZE:]...)
	}

	op := Operation{
		lsn:       lsn,
		multiPage: multiPage,
	}

	fmt.Println("lSize => ", lSize)
	opType := bLog[wal.BLOG_HEADER_SIZE]
	if opType == byte(wal.PUT) {
		op.opType = wal.PUT
	} else {
		op.opType = wal.DEL
	}

	// read key
	kSize := binary.LittleEndian.Uint32(bLog[wal.BLOG_HEADER_SIZE+1 : wal.BLOG_HEADER_SIZE+5])
	fmt.Println("kSize -> ", kSize)

	kOff := wal.BLOG_HEADER_SIZE + 5
	key := bLog[kOff : kOff+int(kSize)]
	op.key = key

	// read val
	if op.opType == wal.PUT {
		vSize := binary.LittleEndian.Uint32(bLog[kOff+int(kSize) : kOff+int(kSize)+4])
		vOff := kOff + int(kSize) + 4

		val := bLog[vOff : vOff+int(vSize)]

		op.val = val
	}

	return op, nil
}

// closes active  file descriptors
func (rMngr *RecoveryMngr) Close() error {
	if rMngr.fd == nil {
		log.Println("No open file descriptors")
		return nil
	}

	err := rMngr.fd.Close()

	if err != nil {
		return RecoveryError{Message: fmt.Sprintf("Unable to shutdown Recovery Manager: %v", err)}
	}

	log.Println("Recovery Manager closed successfully.")

	return nil
}

func NewRecoveryMngr(bufMngr *buffermanager.Cache, index *bp_tree.BTree) (*RecoveryMngr, error) {
	stopChan := make(chan struct{})
	go helpers.StartSpinner(stopChan, "starting recovery...")
	defer close(stopChan)
	// time.Sleep(time.Second * 2)

	// open and read config file
	fd, err := os.OpenFile(RECOVERY_FILEPATH, os.O_RDONLY, 0644)

	if err != nil {
		return nil, RecoveryError{Message: fmt.Sprintf("Unable to open recovery file: %v", err)}
	}

	defer fd.Close()

	// read the 8 byte checkpoint LSN
	latestLSN := make([]byte, 8)

	n, err := fd.Read(latestLSN)

	if err != nil && !errors.Is(err, io.EOF) {
		panic(err)
	}

	fmt.Printf("\n")
	log.Printf("(NEW  RECOVERY MANAGER) Read %d bytes\n", n)
	// if no checkpoint stored, return
	if n <= 0 {
		log.Println("No recovery checkpoint stored. Proceeding...")
		return nil, nil
	}

	// Open wal file in read only mode
	walFd, err := os.OpenFile(wal.WAL_PATH, os.O_RDONLY, 0644)

	if err != nil {
		panic(fmt.Errorf("(recovery) Unable to open WAL file: %v", err))
	}

	// read and store size of wal file
	info, err := walFd.Stat()

	if err != nil {
		panic(err)
	}

	s := info.Size()
	log.Printf("WAL size: %d bytes\n", s)

	recMngr := RecoveryMngr{
		fd:            walFd,
		bufferMngr:    bufMngr,
		BPTreeIndex:   index,
		walSize:       s,
		checkpointLSN: latestLSN,
	}

	return &recMngr, nil
}
