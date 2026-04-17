package tinylfu

import (
	"sync"
)

// "fmt"

type TinyLFU struct {
	Doorkeeper *Bloom
	MainStruct *CMS
	mu         sync.RWMutex
}

// Bloom Filter constants
const (
	BLOOM_BIT_ARR_SIZE = 10000
	HASH_FUNC_COUNT    = 20
)

// Count-Min Sketch (CMS)
const (
	// Sample Size (W). The closer the value of W is to number of operations (N) the higher the accuracy
	SAMPLE_SIZE     = 800000
	CMS_ERROR_RATE  = 0.1  // 0.1%
	CMS_PROBABILITY = 0.01 // 0.01%
)

// Increments count of an item
// Checks if an item is in the Doorkeeper. If not, add to doorkeeper. If it is
// already in the doorkeeper increment in main structure(CMS).
func (t *TinyLFU) IncrementItem(data []byte) error {
	if len(data) == 0 {
		return TinyLFUError{Message: "Invalid/empty data bytes provided"}
	}

	mayExist := t.Doorkeeper.Check(data)

	if !mayExist {
		t.Doorkeeper.Add(data)
		return nil
	}

	// increment in main structure
	opCount, err := t.MainStruct.Increment(data)

	if err != nil {
		return err
	}

	t.mu.Lock()

	if opCount >= SAMPLE_SIZE {
		// Clear Doorkeeper
		t.MainStruct.reset()
		t.Doorkeeper.Clear()
	}

	t.mu.Unlock()
	return nil
}

func (t *TinyLFU) CheckItemCount(data []byte) (int64, error) {
	// Get count
	count, err := t.MainStruct.GetCount(data)

	if err != nil {
		return -1, err
	}

	return count, nil
}

// Creates and returns a new instance of TinyLFU, and error if any.
func NewTinyLFU() (*TinyLFU, error) {
	newHasher := NewMapHash()

	doorKeep, err := NewDoorkeeper(BLOOM_BIT_ARR_SIZE, HASH_FUNC_COUNT, newHasher)

	if err != nil {
		return nil, err
	}

	cms, err := NewCMS(CMS_ERROR_RATE, CMS_PROBABILITY, newHasher)

	if err != nil {
		return nil, err
	}

	return &TinyLFU{
		Doorkeeper: doorKeep,
		MainStruct: cms,
	}, nil
}
