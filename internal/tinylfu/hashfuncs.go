package tinylfu

import (
	"hash"
	"hash/maphash"
	"sync"
)

type Hasher interface {
	Sum64(data []byte) uint64
}

func (m *MapHash) Sum64(data []byte) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.h.Reset()
	m.h.Write(data)
	return m.h.Sum64()
}

type MapHash struct {
	h  hash.Hash64
	mu sync.Mutex
}

func NewMapHash() *MapHash {
	return &MapHash{h: &maphash.Hash{}}
}

// DoubleHashIndices: produce k indices in range [0, m-1] using double hashing.
// Uses two hashes h1,h2 and computes: (h1 + i*h2) % m for i in [0..k-1].
// This is memory/time efficient and statistically good for Bloom & CMS.
func DoubleHashIndices(h Hasher, data []byte, k int, m uint64) []uint64 {
	// compute two 64-bit hashes by varying the input slightly:
	// h1 = hash(data)
	// h2 = hash(data || 0x01)
	h1 := h.Sum64(data)

	// cheap way to get a second hash: append a byte (safe if your Hasher is decent)
	tmp := make([]byte, len(data)+1)
	copy(tmp, data)
	tmp[len(data)] = 1
	h2 := h.Sum64(tmp)

	indices := make([]uint64, k)
	// ensure h2 is odd to avoid cycles when m is power-of-two
	if h2%2 == 0 {
		h2++
	}

	for i := 0; i < k; i++ {
		// use modulo m (bucket count)
		indices[i] = (h1 + uint64(i)*h2) % m
	}

	tmp = nil

	return indices
}
