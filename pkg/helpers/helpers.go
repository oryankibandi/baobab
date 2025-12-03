package helpers

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
)

const (
	DEGREE = 2
)

// sorting
func swap[T cmp.Ordered](list []T) ([]T, bool) {
	hasSwapped := false
	for idx, v := range list {
		if idx != 0 {
			if v < list[idx-1] {
				temp := list[idx-1]
				list[idx-1] = v
				list[idx] = temp
				hasSwapped = true
			}
		}
	}

	return list, hasSwapped
}

func BubbleSort[T cmp.Ordered](list []T) []T {
	swapped := true
	newList := make([]T, len(list))
	copy(newList, list)

	for swapped {
		newList, swapped = swap[T](newList)
	}

	return newList
}

// Search
func LookupBinarySearch[T cmp.Ordered](sortedList []T, searchKey T, startIdx int) (idx int, err error) {
	l := len(sortedList)
	m := l / 2

	// compare
	if sortedList[m] == searchKey {
		fmt.Println("Found item at index => ", (m + startIdx))
		return (m + startIdx), nil
	}

	if l == 1 {
		fmt.Println("LAST ARR => ", sortedList)
		e := fmt.Sprintf("Key %v does not exist", searchKey)
		return -1, HelperError{Message: e}
	}

	if sortedList[m] > searchKey {
		// recurse with items prior
		fmt.Println("Midpoint is greater => ", sortedList[m])
		return LookupBinarySearch[T](sortedList[0:m], searchKey, startIdx)
	}

	if sortedList[m] < searchKey {
		// recurse with items after
		fmt.Println("Midpoint is less => ", sortedList[m])
		return LookupBinarySearch[T](sortedList[m:], searchKey, m)
	}

	return -1, nil
}

// insertion binary search
func InsertBinarySearch(sortedList [][]byte, searchKey []byte, startIdx int) (idx int, err error) {
	l := len(sortedList)
	m := l / 2

	// compare
	if bytes.Compare(sortedList[m], searchKey) == 0 {
		return (m + startIdx), nil
	}

	if l == 1 {
		return (m + startIdx), nil
	}

	if bytes.Compare(searchKey, sortedList[m]) == -1 && bytes.Compare(searchKey, sortedList[m-1]) == 1 {
		return (m + startIdx), nil
	}

	if bytes.Compare(sortedList[m], searchKey) == 1 {
		// recurse with items prior
		return InsertBinarySearch(sortedList[0:m], searchKey, startIdx)
	}

	if bytes.Compare(sortedList[m], searchKey) == -1 {
		// recurse with items after
		return InsertBinarySearch(sortedList[m:], searchKey, m+startIdx)
	}

	return -1, nil
}

// inserts a value to a slice at specified index
func InsertToList[T any](list *[]T, idx int, val T) (*[]T, error) {

	fmt.Println(fmt.Sprintf("Inserting %v at index %d to %v", val, idx, list))
	if idx < 0 {
		return nil, HelperError{Message: fmt.Sprintf("(insertToList) Index %d out of bounds\n", idx)}
	}

	if idx > len(*list)-1 {
		fmt.Println("INDEX GREATER THAN WHATS AVAILABLE---> ", len(*list))
		n := make([]T, 0)

		n = append(n, *list...)
		n = append(n, val)
		*list = n
		// TODO:  directly appending to list pointer as in below line causes a bug. Find out why
		// *list = append(*list, val)
		fmt.Println("INSERTED LIST ==> ", list)
		return list, nil
	}

	*list = append(*list, *new(T))

	copy((*list)[idx+1:], (*list)[idx:])

	(*list)[idx] = val

	fmt.Println("INSERTED LIST ==> ", list)
	return list, nil
}

func RemoveEmptyValues[T any](l *[]*T, idx int) (*[]*T, error) {
	if len(*l) <= idx+1 || (*l)[idx] != nil {
		fmt.Println("Nothing to replace")
		return l, HelperError{Message: "Nothing to replace"}
	}

	childOrder := (2 * DEGREE) + 1

	fmt.Println("Received list ==> ", *l)

	n := make([]*T, 0)
	fmt.Println("NEW EMPTY LIST ==> ", n)
	n = append((*l)[:idx], (*l)[idx+1:]...)

	fmt.Println("NEW LIST  => ", n)

	if len(n) >= childOrder {
		n = n[:childOrder]
	} else {
		deficit := childOrder - len(n)
		for i := deficit; i <= deficit; i++ {
			n = append(n, *new(*T))
		}
	}

	*l = n
	fmt.Println("FINAL LIST ==> ", n)

	return l, nil
}

// Remove all nil values from slice
func Compact[T any](list []*T) []*T {
	targetLen := 5
	i := 0
	for _, item := range list {
		if item != nil {
			if i < targetLen {
				list[i] = item
				i++
			} else {
				break // We already have enough non-nil items
			}
		}
	}

	// Fill remaining slots with nil if needed
	for i < targetLen {
		list[i] = nil
		i++
	}

	// Ensure slice is exactly targetLen long
	if len(list) < targetLen {
		// Extend with nils if the original list was shorter
		padding := make([]*T, targetLen-len(list))
		list = append(list, padding...)
	}

	return list[:targetLen]
}

func DeleteSliceKey[T cmp.Ordered](l *[]T, idx int) (*[]T, error) {
	if len(*l) <= 0 {
		return nil, HelperError{Message: fmt.Sprintf("(deleteSliceKey) Slice is empty: %v\n", idx)}
	}

	if idx < 0 || idx >= len(*l) {
		fmt.Println("IDX < 0 => ", idx < 0)
		fmt.Println(" idx+1 > len(*l) ==> ", idx+1 > len(*l))
		fmt.Println("LEN(*l) => ", len(*l))
		return nil, HelperError{Message: fmt.Sprintf("(deleteSliceKey) Index %d out of bounds\n", idx)}
	}

	n := make([]T, 0)

	n = append((*l)[:idx], (*l)[idx+1:]...)

	*l = n

	return l, nil
}

func KeysToBytes[T ~int32](s []T) ([][]byte, error) {
	b := make([][]byte, len(s))
	if len(s) <= 0 {
		return b, nil
	}

	for i, v := range s {
		p := make([]byte, 4)
		binary.LittleEndian.PutUint32(p, uint32(v))

		b[i] = p
	}

	return b, nil
}

// Compares to Log Sequence Numbers and returns the greater one, and error if
// unsuccessful
func MaxLSN(lsnA []byte, lsnB []byte) ([]byte, error) {
	if len(lsnA) < 8 {
		return nil, HelperError{Message: fmt.Sprintf("Invalid length for lsnA. Got length %d\n", len(lsnA))}
	}

	if len(lsnB) < 8 {
		return nil, HelperError{Message: fmt.Sprintf("Invalid length for lsnB. Got length %d\n", len(lsnB))}
	}

	pageA := binary.LittleEndian.Uint32(lsnA[:4])
	pageB := binary.LittleEndian.Uint32(lsnB[:4])

	if pageA != pageB {
		// return lsn with greater page
		if pageA > pageB {
			return lsnA, nil
		} else {
			return lsnB, nil
		}
	}

	// use offset to determin greater LSN
	offsetA := binary.LittleEndian.Uint32(lsnA[4:])
	offsetB := binary.LittleEndian.Uint32(lsnB[4:])

	if offsetA > offsetA {
		return lsnA, nil
	} else if offsetA < offsetB {
		return lsnB, nil
	} else {
		// offsets are equal return first
		return lsnA, nil
	}
}
