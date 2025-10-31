package tinylfu

// Doorkeeper is a regular Bloom filter placed in front of the approximate counting scheme. Upon
// item arrival, we first check if the item is contained in the Doorkeeper. If it is not contained in the
// Doorkeeper (as is expected with first timers and tail items), the item is inserted to the Doorkeeper and
// otherwise, it is inserted to the main structure. When querying items, we use both the Doorkeeper and the
// main structures. That is, if the item is included in the Doorkeeper, TinyLFU estimates the frequency of
// this item as its estimation in the main structure plus 1. Otherwise, TinyLFU returns just the estimation
// from the main structure

import (
	"errors"
	"fmt"
	"sync/atomic"

	//	"fmt"
	"math"
	"sync"
)

type hashFunc func(int) int

type Bloom struct {
	m        uint64 // size of bitArray
	k        uint64 // hash func count
	hasher   Hasher
	bitArray []byte
	mu       sync.RWMutex
	wg       *sync.WaitGroup
}

func NewDoorkeeper(m uint64, k uint64, hasher Hasher) *Bloom {
	return &Bloom{
		bitArray: make([]byte, (m+7)/8),
		m:        m,
		k:        k,
		hasher:   hasher,
		wg:       &sync.WaitGroup{},
	}
}

func (b *Bloom) Add(data []byte) {
	// Get indices where to add vbits
	indices := DoubleHashIndices(b.hasher, data, int(b.k), b.m)

	for _, v := range indices {
		// b.wg.Add(1)
		b.setBit(int(v))
	}

	// b.wg.Wait()
}

func (b *Bloom) Check(data []byte) bool {
	// Get indices where to add vbits
	indices := DoubleHashIndices(b.hasher, data, int(b.k), b.m)
	fmt.Println("Indices....=> ", indices)

	var exists atomic.Bool
	exists.Store(true)

	// fmt.Println("(Check) Indices: ", indices)
	for _, v := range indices {
		// b.wg.Add(1)
		// go func(t uint64) {
		// 	defer b.wg.Done()
		// 	if set, err := b.isSet(int(t)); err == nil && !set {
		// 		exists.Store(false)
		// 	} else if err != nil {
		// 		panic(err)
		// 	}
		// }(v)

		if set, err := b.isSet(int(v)); err == nil && !set {
			exists.Store(false)
		} else if err != nil {
			panic(err)
		}
	}

	// b.wg.Wait()

	return exists.Load()
}

func (b *Bloom) Clear() {
	if b == nil {
		panic("Doorkeeper not initialized.")
	}

	b.bitArray = make([]byte, (b.m+7)/8)
}

// Sets bit on the bit array at the provided position (pos)
func (b *Bloom) setBit(pos int) error {
	// defer b.wg.Done()
	l := int(math.Floor(float64(pos / 8)))

	if l >= len(b.bitArray) {
		return errors.New("Invalid index")
	}

	maskPos := (8 * (l + 1)) - (pos + 1)

	var mask byte

	mask = 1 << byte(maskPos)

	// set bit (bitwise OR)
	b.mu.Lock()
	b.bitArray[l] |= mask
	b.mu.Unlock()

	return nil
}

func (b *Bloom) unsetBit(pos int) error {
	l := int(math.Floor(float64(pos / 8)))

	if l >= len(b.bitArray) {
		return errors.New("Invalid index")
	}

	maskPos := (8 * (l + 1)) - (pos + 1)

	var mask byte

	mask = 1 << byte(maskPos)

	// unset bit (^AND)
	b.mu.Lock()
	b.bitArray[l] = b.bitArray[l] & (^mask)
	b.mu.Unlock()

	return nil
}

// Check if bit is set
func (b *Bloom) isSet(pos int) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	l := int(math.Floor(float64(pos / 8)))

	if l >= len(b.bitArray) {
		return false, errors.New("Invalid index")
	}

	maskPos := (8 * (l + 1)) - (pos + 1)

	var mask byte

	mask = 1 << byte(maskPos)

	// Check if set
	r := b.bitArray[l] & mask

	return r > 0, nil
}
