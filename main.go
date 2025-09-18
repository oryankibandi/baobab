package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"sort"

	"github.com/oryankibandi/on_disk_btree/pkg/bp_tree"
	"github.com/oryankibandi/on_disk_btree/pkg/disk_io"
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

func main() {
	fmt.Println("Hello world")
	btree, err := bp_tree.InitBTree[int32]()

	if err != nil {
		panic(err.Error())
	}

	log.Println("NEW BTREE => ", *btree)
	log.Println("BTree Root => ", *btree.Root)

	items := make([]NodeData, 0)

	items = append(items, NodeData{key: 25, value: []byte("NYC systems")})
	items = append(items, NodeData{key: 5, value: []byte("NBO systems")})
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
	pge, err := diskio.New[int32](keySlice, &valSlice, nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Pge => ", pge)
}
