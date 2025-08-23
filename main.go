package main

import (
	diskio "bp_tree_disk/pkg/disk_io"
	"fmt"
	"log"
	"time"
)

func main() {
	fmt.Println("Hello world")

	go func() {

		for v := 1; v <= 100; v++ {
			cycle := v % 4
			var t string
			if cycle == 0 {
				t = "|"
			} else if cycle == 1 {
				t = "/"
			} else if cycle == 2 {
				t = "-"
			} else {
				t = "\\"
			}

			fmt.Printf("Processing %s\r", t)
			time.Sleep(time.Millisecond * 200)
		}
	}()

	pge, err := diskio.New[int32]()

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Pge => ", pge)
}
