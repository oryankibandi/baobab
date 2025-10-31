package tinylfu

import (
	"fmt"
	"sync"
)

// Count-Min Sketch(Approximation sketch) is the main structure of the TinyLFU and
// is used to store the frequency of the items accessed. It has a sample size (W),
// which if the counters reach they are divided by 2. This is called a `reset` operation. The DoorKeeper is also cleared.
// Error count = 2N/w where N is number of unique entries
// Probability = 1 - 0.5^d

type CMS struct {
	k          uint64     // hash func count
	m          uint64     // width of the 2D Array
	arr        [][]uint64 // 2D Array
	hasher     Hasher
	mu         sync.RWMutex
	wg         *sync.WaitGroup
	w          uint64 // sample size(W). When this val is reached, reset op is triggered
	op_counter uint64
}

// Create new Count-min sketch
// k - no of  hash funcs
// w - Sample size(W)
// m - width of the 2D array
// d - Depth of the 2D array
// hasher - hasher
func NewCMS(k uint64, w uint64, m uint64, d uint64, hasher Hasher) *CMS {
	c := &CMS{
		k:      k,
		m:      m,
		hasher: hasher,
		arr:    make([][]uint64, d),
		w:      w,
		wg:     &sync.WaitGroup{},
	}

	for i, _ := range c.arr {
		c.arr[i] = make([]uint64, m)
	}

	return c
}

// Increments data and returns an error if any and a boolean indicating whether a reset has happened, in which case the doorkeeper needs to be cleared.
func (c *CMS) Increment(key []byte) (bool, error) {
	indices := DoubleHashIndices(c.hasher, key, int(c.k), c.m)

	// fmt.Println("INDICES: ", indices)
	// increment
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, v := range indices {
		c.arr[i][v] += 1
	}

	c.op_counter += 1

	if c.op_counter >= c.w {
		fmt.Println("RESET AT ==> ", c.op_counter)
		c.reset()

		return true, nil
	}

	return false, nil
}

func (c *CMS) GetCount(key []byte) (int64, error) {
	indices := DoubleHashIndices(c.hasher, key, int(c.k), c.m)

	var count *uint64

	c.mu.RLock()
	defer c.mu.RUnlock()
	for i, v := range indices {
		val := c.arr[i][v]

		if count != nil {
			if *count > val {
				*count = val
			}
		} else {
			count = &val
		}
	}

	return int64(*count), nil
}

// Halves all the counters  in the CMS, reset op_counter and clear doorkeeper
func (c *CMS) reset() {
	// halve all  counters

	for i, v := range c.arr {
		for j, k := range v {
			if k > 0 {
				c.arr[i][j] = k / 2
			}
		}
	}

	c.op_counter = 0
}
