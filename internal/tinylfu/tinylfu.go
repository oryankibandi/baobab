package tinylfu

import (
	"log"
	"sync"
)

// "fmt"

type TinyLFU struct {
	Doorkeeper *Bloom
	MainStruct *CMS
	sampleSize uint64 // sample size(W). When this val is reached, reset op is triggered
	mu         sync.RWMutex
}

// Bloom
const (
	BLOOM_BIT_ARR_SIZE = 10000
	HASH_FUNC_COUNT    = 20
)

// Count-Min Sketch (CMS)
const (
	SAMPLE_SIZE     = 800000 // W. The closer the value of W is to number of operations (N) the higher the accuracy
	CMS_ERROR_RATE  = 0.1    // 0.1%
	CMS_PROBABILITY = 0.01   // 0.01%
)

func New() (*TinyLFU, error) {
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

// Increments count of an item
// Checks if an item is in the Doorkeeper. If not, add to doorkeeper. If it is
// already in the doorkeeper increment in main structure(CMS).
func (t *TinyLFU) IncrementItem(data []byte) error {

	log.Println("Checking doorkeeper....")
	mayExist := t.Doorkeeper.Check(data)
	log.Println("Checked doorkeeper...")

	if !mayExist {
		t.Doorkeeper.Add(data)
		log.Println("Added to doorkeeper....")
		return nil
	}

	// increment in main structure
	log.Println("Incrementing in main structure....")
	opCount, err := t.MainStruct.Increment(data)
	log.Println("Incremented in main structure....")

	if err != nil {
		return err
	}

	t.mu.Lock()

	if opCount >= t.sampleSize {
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

	// Increment count
	// err = t.IncrementItem(data)

	//if err != nil {
	//	return -1, nil
	//}

	return count, nil
}
