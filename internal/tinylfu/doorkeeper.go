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
	"sync/atomic"

	//	"fmt"
	"math"
	"sync"
)

type Bloom struct {
	// size of bitArray
	m uint64
	// hash func count
	k        uint64
	hasher   Hasher
	bitArray []byte
	mu       sync.RWMutex
	wg       *sync.WaitGroup
}

func (b *Bloom) Add(data []byte) error {
	// Get indices where to add vbits
	indices := DoubleHashIndices(b.hasher, data, int(b.k), b.m)
	err := b.setBit(indices)

	if err != nil {
		return err
	}

	return nil
}

func (b *Bloom) Check(data []byte) bool {
	// Get indices where to add vbits
	indices := DoubleHashIndices(b.hasher, data, int(b.k), b.m)

	var exists atomic.Bool
	exists.Store(true)

	for _, v := range indices {
		if set, err := b.isSet(int(v)); err == nil && !set {
			exists.Store(false)
		} else if err != nil {
			panic(err)
		}
	}

	return exists.Load()
}

func (b *Bloom) Clear() {
	if b == nil {
		panic("Doorkeeper not initialized.")
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.bitArray {
		b.bitArray[i] &= 0x00
	}
}

// Sets bits on the bit array at the provided positions in the uint64 array (pos)
func (b *Bloom) setBit(pos []uint64) error {
	var mask byte

	b.mu.RLock()
	maskArr := make([]byte, (b.m)/8)
	b.mu.RUnlock()

	for _, p := range pos {
		// find the position of the byte where pos is located
		// since b.bitArray is an array of bytes, the position
		// to set the bits will be located within an individual byte.
		l := uint64(math.Floor(float64(p / 8)))
		if l >= uint64(len(b.bitArray)) {
			return errors.New("Invalid index")
		}

		maskPos := (8 * (l + 1)) - (p + 1)
		mask = 1 << byte(maskPos)

		// bitwise OR on maskArr
		maskArr[l] |= mask
	}

	// set bit (bitwise OR)
	b.mu.Lock()
	// bitwise OR on bloom filter's bit array
	for i := range (b.m) / 8 {
		b.bitArray[i] |= maskArr[i]
	}
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

// Returns a new Doorkeeper(bloom filter) with size m bit array and k number of hash functions.
// m must be a multiple of 8 eg. 8, 64, 1024, 4096
func NewDoorkeeper(m uint64, k uint64, hasher Hasher) (*Bloom, error) {
	if hasher == nil {
		return nil, TinyLFUError{Message: "invalid hasher provided"}
	}

	if k == 0 {
		return nil, TinyLFUError{Message: "At least one hash function required. Hash function count is zero"}
	}

	if m == 0 {
		return nil, TinyLFUError{Message: "size of bit array cannot be zero"}
	}

	if m%8 != 0 {
		return nil, TinyLFUError{Message: "Size of bit array must be a multiple of 8"}
	}

	return &Bloom{
		bitArray: make([]byte, (m+7)/8),
		m:        m,
		k:        k,
		hasher:   hasher,
		wg:       &sync.WaitGroup{},
	}, nil
}
