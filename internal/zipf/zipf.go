package zipf

import (
	"math"
	"math/rand"
	"time"
)

type Zipf struct {
	// Zipf exponent (s)
	s float64
	// offset (v)
	v    float64
	imax float64
	// lower boundary
	xmin    float64
	xmax    float64
	hlow    float64
	hupp    float64
	totArea float64
	rnd     *rand.Rand
}

// Calculates and returns h(x) using: h(x) = ((v + x) ** (1-s))/(1-s))
func (z *Zipf) h(x float64) float64 {
	h_x := (math.Pow(float64(z.v)+x, (1 - z.s))) / (1 - z.s)

	return h_x
}

// Generate U[0, 1]
func (z *Zipf) rng() float64 {
	return z.rnd.Float64()
}

// Maps u to cummuative area -> area = hlow+ u*totalArea
func (z *Zipf) cummulativeArea(u float64) float64 {
	if u < 0 {
		panic("invalid random integer")
	}

	area := z.hlow + (u * z.totArea)
	return area
}

// sovles for x from h inverse using cummulative area
//
//	x = (cummulativeArea ** (1/(1-s)))((1-s)**(1/(1-s))) - v
func (z *Zipf) findX(cArea float64) float64 {
	// x := math.Pow(cArea, (1.0/(1.0-z.s)))*math.Pow((1.0-z.s), (1.0/(1.0-z.s))) - float64(z.v)
	x := math.Exp((math.Log((1-z.s)*cArea))/(1-z.s)) - z.v

	return math.Floor(x + 0.5)
}

// passes k through an acceptance test and returns true if value is accepted else false.
//
//	cArea >= h(k + 0.5) - (v + k) ** (-s)
func (z *Zipf) acceptanceTest(cArea float64, k float64) bool {
	l := z.hlow - math.Pow((z.v+k), (-z.s))
	return cArea >= l
}

func (z *Zipf) GetNext() uint64 {
	accepted := false
	var u float64
	var k float64
	var cArea float64

	for !accepted {
		u = z.rng()
		cArea = z.cummulativeArea(u)
		k = z.findX(cArea)
		accepted = z.acceptanceTest(cArea, k)
	}

	return uint64(k)
}

// Creates a new Zipfian generator
//
//	s - Zipf exponent. Determines how much skew to be applied. Must be > 1.
//	v - offset
//	imax - Max integer value: k ∈ {0..imax}
func NewZipf(s float64, v float64, imax float64) *Zipf {
	if s <= 1 || imax < 1 || v < 0 {
		return nil
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z := &Zipf{
		s:    s,
		v:    v,
		imax: imax,
		xmin: 0.5,
		xmax: 0.5 + float64(imax),
		rnd:  r,
	}

	// bucket boundaries 0.5 - imax + 0.5
	z.hlow = z.h(z.xmin)
	z.hupp = z.h(z.xmax)
	// totalArea = h(upper) - h(lower)
	z.totArea = z.hupp - z.hlow

	return z
}
