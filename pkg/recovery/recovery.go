package recovery

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oryankibandi/baobab/pkg/bp_tree"
	buffermanager "github.com/oryankibandi/baobab/pkg/buffer_manager"
	"github.com/oryankibandi/baobab/pkg/wal"
)

const (
	RECOVERY_FILEPATH = "bb_config"
)

type RecoveryMngr struct {
	bufferMngr    *buffermanager.Cache
	BPTreeIndex   *bp_tree.BTree
	fd            *os.File // WAL file descriptor
	checkpointLSN []byte
}

// Reads the logs in WAL, from the REDO point, compares page LSNs with LSN in WAL
// and reapplies the missing logs.
func (rMngr *RecoveryMngr) Recover() error {
	if rMngr.fd == nil {
		panic("(Recover) No attached file descriptor.")
	}

	stopChan := make(chan struct{})
	go startSpinner(stopChan, "Replaying WAL...")
	defer close(stopChan)

	// TODO: Add recovery procedure
}

func NewRecoveryMngr(bufMngr *buffermanager.Cache, index *bp_tree.BTree) *RecoveryMngr {
	stopChan := make(chan struct{})
	go startSpinner(stopChan, "starting recovery...")
	defer close(stopChan)
	// open and read config file
	fd, err := os.OpenFile(RECOVERY_FILEPATH, os.O_RDONLY, 0644)

	if err != nil {
		log.Println("Unable to open recovery file: ", err)
		return nil
	}

	defer fd.Close()

	// read the 8 byte checkpoint LSN
	latestLSN := make([]byte, 8)

	n, err := fd.Read(latestLSN)

	if err != nil {
		panic(err)
	}

	// if no checkpoint stored, return
	if n <= 0 {
		log.Println("No recovery checkpoint stored. Proceeding...")
		return nil
	}

	// Open wal file in read only mode
	walFd, err := os.OpenFile(wal.WAL_PATH, os.O_RDONLY, 0644)

	if err != nil {
		panic(fmt.Errorf("(recovery) Unable to open WAL file: ", err))
	}

	recMngr := RecoveryMngr{
		fd:            walFd,
		bufferMngr:    bufMngr,
		BPTreeIndex:   index,
		checkpointLSN: latestLSN,
	}

	return &recMngr
}

func startSpinner(stop chan struct{}, message string) {
	spin := []rune{'|', '/', '-', '\\'}

	i := 0
	for {
		select {
		case <-stop:
			fmt.Printf("\rDone\n")
		default:
			fmt.Printf("\r%c %s", spin[i%len(spin)], message)
			i++
			time.Sleep(100 * time.Millisecond)
		}
	}
}
