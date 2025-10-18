package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"sort"
	"time"

	"github.com/oryankibandi/on_disk_btree/pkg/bp_tree"
)

type NodeData struct {
	key   int32
	value []byte
}

func sortItems(l []NodeData) []NodeData {
	t := make([]NodeData, 0)
	t = append(t, l...)

	sortFunc := func(a, b int) bool {
		aByte := make([]byte, 4)
		bByte := make([]byte, 4)
		binary.LittleEndian.PutUint32(aByte, uint32(t[a].key))
		binary.LittleEndian.PutUint32(bByte, uint32(t[b].key))

		return bytes.Compare(aByte, bByte) == -1
		// return t[a].key < t[b].key
	}

	sort.Slice(t, sortFunc)

	return t
}

func NewRandomNodeData(strLen int) NodeData {
	if strLen <= 0 {
		strLen = 16
	}

	k, err := randomInt32InRange(0, 100)

	if err != nil {
		panic(err.Error())
	}

	return NodeData{
		key:   k,
		value: randomAlphaNumBytes(strLen),
	}
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomInt32Positive() int32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // in production return an error instead of panicking
	}
	// mask off sign bit so result is non-negative
	return int32(binary.BigEndian.Uint32(b[:]) & 0x7fffffff)
}

func randomInt32InRange(min, max int32) (int32, error) {
	if min > max {
		return 0, fmt.Errorf("min must be <= max")
	}
	// rangeSize fits in int64 because max-min+1 <= 2^32
	rangeSize := int64(max) - int64(min) + 1
	n := big.NewInt(rangeSize)

	r, err := rand.Int(rand.Reader, n) // returns 0 <= r < rangeSize
	if err != nil {
		return 0, err
	}
	return int32(int64(min) + r.Int64()), nil
}

func randomAlphaNumBytes(n int) []byte {
	out := make([]byte, n)
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	for i := 0; i < n; i++ {
		out[i] = letters[int(buf[i])%len(letters)]
	}
	return out
}

// runServer starts a basic HTTP server on port 8080
func runServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, world! Server is running on port 8080.")
	})

	fmt.Println("🚀🚀🚀🚀🚀🚀🚀  Server running on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("Error starting server:", err)
	}
}

func main() {
	start := time.Now()
	fmt.Println("Hello world")
	btree, err := bp_tree.InitBTree[int32]()

	if err != nil {
		panic(err.Error())
	}

	log.Println("NEW BTREE => ", *btree)

	if btree.Root != nil {
		log.Println("BTree Root => ", btree.Root)
	}

	items := make([]NodeData, 0)

	items = append(items, NodeData{key: 25, value: []byte("CAPETOWN systems")})
	items = append(items, NodeData{key: 5, value: []byte("AMSTERDAN systems")})
	items = append(items, NodeData{key: 520, value: []byte("DC systems")})
	items = append(items, NodeData{key: 50, value: []byte("Bengaluru systems")})
	items = append(items, NodeData{key: 45, value: []byte("Amsterdam systems")})

	sorted := sortItems(items)
	fmt.Println("(main) SORTED ==> ", sorted)

	keySlice := make([][]byte, 0)
	valSlice := make([][]byte, 0)

	for _, v := range sorted {
		k := make([]byte, 0)
		n := binary.LittleEndian.AppendUint32(k, uint32(v.key))
		fmt.Println("(main) AFTER LITTLE ENDIAN ==> ", n)
		keySlice = append(keySlice, n)

		valSlice = append(valSlice, v.value)
	}

	fmt.Println("(main) => KEYS ", keySlice)
	inserted, err := bp_tree.InsertValue(keySlice, valSlice)

	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("Page Inserted: ", inserted)
	fmt.Println("--------------------------------------------------------------------------------------------------------------")
	fmt.Println("--------------------------------------------------------------------------------------------------------------")
	fmt.Println("--------------------------------------------------------------------------------------------------------------")

	l := make([]NodeData, 0)

	for i := range 50 {
		r := NewRandomNodeData(12)
		fmt.Println("Random DATA: ", r)
		l = append(l, r)
		k := make([]byte, 0)
		n := binary.LittleEndian.AppendUint32(k, uint32(r.key))

		inserted, err := bp_tree.InsertValue([][]byte{n}, [][]byte{r.value})

		if err != nil {
			panic(err)
		}

		fmt.Printf("<><><><><><><><><><><><><><>><><><><><><>><><><><>><><><><><>><<><>><><><><><><><><><><><><><><><><><><><><%d inserted -> %v\n", i, inserted)
		time.Sleep(time.Millisecond * 500)
	}

	fmt.Println("------------------------------------------------------------------------------------------------------------------------------------------------")
	fmt.Println("INSERTED DATA:")
	for _, v := range l {
		fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
		fmt.Printf("key: %d\tval: %s\n", v.key, v.value)
	}

	duration := time.Since(start)
	fmt.Println()
	fmt.Println("Done in ", duration)

	runServer()
}
