package manual

/*
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

// allocates memory of size s using calloc
func Alloc(size uintptr) unsafe.Pointer {
	ptr := C.calloc(1, C.size_t(size))

	if ptr == nil {
		panic("Out of memory.")
	}

	return ptr
}

// Frees memory pointed to by d
func FreeMem(d unsafe.Pointer) {
	if d == nil {
		fmt.Println("D is Nil")
		panic("d is NIL")
		// return
	}

	C.free(d)
}

// Retrieves Peak RSS
func GetRssKB() uint64 {
	var r syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return uint64(r.Maxrss)
}

func CurrentRSSBytes() (uint64, error) {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0, fmt.Errorf("unexpected statm")
	}
	resPages, _ := strconv.ParseUint(fields[1], 10, 64)
	return resPages * uint64(os.Getpagesize()), nil
}
