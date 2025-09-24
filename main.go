package main

import (
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
	// items = append(items, NodeData{key: 50, value: []byte("Bengaluru systems")})
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

	// second insert
	//items = make([]NodeData, 0)
	//items = append(items, NodeData{key: 520, value: []byte("DC systems")})
	//items = append(items, NodeData{key: 50, value: []byte("Bengaluru systems")})

	//keySlice = make([][]byte, 0)
	//valSlice = make([][]byte, 0)

	//sorted = sortItems(items)

	//for _, v := range sorted {
	//	k := make([]byte, 0)
	//	n := binary.LittleEndian.AppendUint32(k, uint32(v.key))
	//	fmt.Printf("(main) %d TO LITTLE ENDIAN ==> %v\n", v.key, n)
	//	keySlice = append(keySlice, n)

	//	valSlice = append(valSlice, v.value)
	//}

	//fmt.Println("INSERTING LIST -> ", keySlice, valSlice)
	//inserted, err = bp_tree.InsertValue(keySlice, valSlice)

	//if err != nil {
	//	log.Fatal(err.Error())
	//}

	//fmt.Println("Second Insertion: ", inserted)

}
