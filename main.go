package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"sort"

	"github.com/oryankibandi/on_disk_btree/pkg/bp_tree"
)

type NodeData struct {
	key   int32
	value []byte
}

func sortItems(l []NodeData) []NodeData {
	t := make([]NodeData, 0)
	t = append(t, l...)

	sortFunc := func(a, b int) bool { return t[a].key < t[b].key }

	sort.Slice(t, sortFunc)

	return t
}

func NewRandomNodeData(strLen int) NodeData {
	if strLen <= 0 {
		strLen = 16
	}
	return NodeData{
		key:   randomInt32Positive(),
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

func main() {
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

	for i := range 1 {
		r := NewRandomNodeData(12)
		fmt.Println("Random DATA: ", r)
		k := make([]byte, 0)
		n := binary.LittleEndian.AppendUint32(k, uint32(r.key))

		inserted, err := bp_tree.InsertValue([][]byte{n}, [][]byte{r.value})

		if err != nil {
			panic(err)
		}

		fmt.Printf("%d inserted -> %v\n", i, inserted)
	}
}
