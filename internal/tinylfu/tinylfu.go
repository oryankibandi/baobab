package tinylfu

import "log"

// "fmt"

type TinyLFU struct {
	Doorkeeper *Bloom
	MainStruct *CMS
}

// Bloom
const (
	BLOOM_BIT_ARR_SIZE = 10000
	HASH_FUNC_COUNT    = 20
)

// Count-Min Sketch (CMS)
const (
	CMS_HASH_FUNC_COUNT = 20     // k
	SAMPLE_SIZE         = 800000 // W. The closer the value of W is to number of operations (N) the higher the accuracy
	CMS_ARR_WIDTH       = 10000
)

func New() *TinyLFU {
	newHasher := NewMapHash()

	doorKeep := NewDoorkeeper(BLOOM_BIT_ARR_SIZE, HASH_FUNC_COUNT, newHasher)
	cms := NewCMS(CMS_HASH_FUNC_COUNT, SAMPLE_SIZE, CMS_ARR_WIDTH, CMS_HASH_FUNC_COUNT, newHasher)

	return &TinyLFU{
		Doorkeeper: doorKeep,
		MainStruct: cms,
	}
}

// Increments count of an item
// Checks if an item is in the Doorkeeper. If not, add to doorkeeper. If it is
// already in the doorkeeper increment in main structure
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
	isReset, err := t.MainStruct.Increment(data)
	log.Println("Incremented in main structure....")

	if err != nil {
		return err
	}

	if isReset {
		// Clear Doorkeeper
		t.Doorkeeper.Clear()
	}

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
