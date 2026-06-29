package tinylfu

import (
	"math"
	"sync"
)

// Count-Min Sketch(Approximation sketch) is the main structure of the TinyLFU and
// is used to store the frequency of the items accessed. It has a sample size (W),
// which if the counters reach they are divided by 2. This is called a `reset` operation where the doorKeeper is also cleared.
type CMS struct {
	k          uint64     // hash func count
	width      uint64     // width of the 2D Array
	arr        [][]uint64 // 2D Array with 64 bit counters
	hasher     Hasher
	mu         sync.RWMutex
	wg         *sync.WaitGroup
	op_counter uint64
}

// Create new Count-min sketch and calculates the sketch's width(w) and depth(d) using
// error rate(ε) and probability(δ) using the formulas from the CM Sketch paper.
//
//	w=2/ε
//	d=ln(δ)/ln(1/2)
//
//	errorRate - error rate in percentage
//	probability - probability rate as a percentage
//	hasher - hasher
func NewCMS(errorRate float64, probability float64, hasher Hasher) (*CMS, error) {
	if errorRate <= 0 {
		return nil, CountMinSketchError{Message: "Error rate cannot be zero."}
	}

	if probability <= 0 {
		return nil, CountMinSketchError{Message: "Error rate cannot be zero."}
	}

	if hasher == nil {
		return nil, CountMinSketchError{Message: "hasher is required"}
	}

	// calculate width(m) and depth(d)
	// w=2/ε
	w := uint64(math.Ceil(2 / (errorRate / 100)))

	// calculate depth
	//  d=ln(δ)/ln(1/2)
	d := uint64(math.Ceil(math.Log((probability / 100)) / math.Log(0.5)))
	c := &CMS{
		k:      d,
		hasher: hasher,
		arr:    make([][]uint64, d),
		width:  uint64(w),
		wg:     &sync.WaitGroup{},
	}

	for i, _ := range c.arr {
		c.arr[i] = make([]uint64, w)
	}

	return c, nil
}

// Increments data and returns the number of operations handled so far, and an error if any.
func (c *CMS) Increment(key []byte) (uint64, error) {
	indices := DoubleHashIndices(c.hasher, key, int(c.k), c.width)

	// fmt.Println("INDICES: ", indices)
	// increment
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, v := range indices {
		c.arr[i][v] += 1
	}

	c.op_counter += 1

	indices = nil
	return c.op_counter, nil
}

// Passes the key through k hash functions which produce k number of indices.
// With theses indices, we check the count at each row of the array in the 2D
// array and return the smallest count.
func (c *CMS) GetCount(key []byte) (int64, error) {
	indices := DoubleHashIndices(c.hasher, key, int(c.k), c.width)

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

	indices = nil
	return int64(*count), nil
}

// Halves all the counters  in the CMS, reset op_counter and clear doorkeeper
func (c *CMS) reset() {
	// halves all counters
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, _ := range c.arr {
		c.wg.Add(1)
		go func(idx int) {
			defer c.wg.Done()
			for j, k := range c.arr[idx] {
				if k > 0 {
					// modify array item in place
					c.arr[i][j] = k / 2
				}
			}
		}(i)
	}

	c.wg.Wait()
	c.op_counter = 0
}
