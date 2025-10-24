package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/oryankibandi/on_disk_btree/pkg/bp_tree"
)

type NodeData struct {
	key   int32
	value []byte
}

type KV struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

type RangeRes struct {
	Status  string `json:"status"`
	Count   int    `json:"count"`
	Results []KV   `json:"results"`
}

type DelResp struct {
	Status  string `json:"status"`
	Deleted bool   `json:"deleted"`
}

// simple JSON error response
type ErrResp struct {
	Error string `json:"error"`
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

// writeJSON helps send JSON responses
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(v)

	if err != nil {
		log.Println("JSON ENCODE ERR: ", err.Error())
	}

	w.Write([]byte{})
}

func getKey() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, ErrResp{Error: "method not allowed"})
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/kv/")

		if key == "" || strings.Contains(key, "/") {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: "invalid key in path"})
			return
		}

		val, err := bp_tree.Get([]byte(key))

		if err != nil {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: err.Error()})
			return
		}

		writeJSON(w, http.StatusOK, KV{Key: key, Val: string(val)})
	}
}

func getRange() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, ErrResp{Error: "method not allowed"})
			return
		}

		query := r.URL.Query()

		start := query.Get("start")
		end := query.Get("end")
		limitStr := query.Get("limit")

		if start == "" {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: "invalid start query parameter"})
			return
		}

		// Default limit if not provided
		limit := 10
		if limitStr != "" {
			var err error
			limit, err = strconv.Atoi(limitStr)
			if err != nil {
				http.Error(w, "invalid limit parameter", http.StatusBadRequest)
				return
			}
		}

		log.Printf("start: %s , end: %s , limit: %v\n", start, end, limit)

		results, err := bp_tree.RangeSearch([]byte(start), []byte(end), int32(limit))

		if err != nil {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: err.Error()})
			return
		}

		rangeRes := RangeRes{
			Status:  "success",
			Count:   len(results),
			Results: make([]KV, 0),
		}

		for _, r := range results {
			rangeRes.Results = append(rangeRes.Results, KV{
				Key: string(r.Key),
				Val: string(r.Val),
			})
		}

		writeJSON(w, http.StatusOK, rangeRes)
	}
}

func addKey() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			writeJSON(w, http.StatusMethodNotAllowed, ErrResp{Error: "method not allowed"})
			return
		}

		// honour cancellations
		select {
		case <-r.Context().Done():
			writeJSON(w, http.StatusRequestTimeout, ErrResp{Error: "request cancelled"})
			return
		default:
		}

		var payload KV
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&payload); err != nil {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: "invalid JSON: " + err.Error()})
			return
		}
		if payload.Key == "" {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: "key is required"})
			return
		}

		fmt.Printf("Key: %v\nVal: %v\n", payload.Key, payload.Val)
		bp_tree.InsertValue([][]byte{[]byte(payload.Key)}, [][]byte{[]byte(payload.Val)})
		writeJSON(w, http.StatusOK, payload)
	}
}

func removeKey() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			writeJSON(w, http.StatusMethodNotAllowed, ErrResp{Error: "method not allowed"})
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/kv/remove/")

		if key == "" || strings.Contains(key, "/") {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: "invalid key in path"})
			return
		}

		del, err := bp_tree.DeleteValue([][]byte{[]byte(key)})

		if err != nil {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: err.Error()})
			return
		}

		writeJSON(w, http.StatusOK, DelResp{Status: "success", Deleted: del})
	}
}

func setKey() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, ErrResp{Error: "method not allowed"})
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/kv/")

		if key == "" || strings.Contains(key, "/") {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: "invalid key in path"})
			return
		}

		val, err := bp_tree.Get([]byte(key))

		if err != nil {
			writeJSON(w, http.StatusBadRequest, ErrResp{Error: err.Error()})
		}

		writeJSON(w, http.StatusOK, KV{Key: key, Val: string(val)})
	}
}

func runProfiler() {
	profSrv := &http.Server{Addr: ":8020"}

	go func() {
		log.Println("📈📈 Running profiler on port :8020...")
		log.Println(profSrv.ListenAndServe())
	}()
}

// runServer starts a basic HTTP server on port 8080
func runServer() {
	srv := &http.Server{Addr: ":8080"}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, world! Server is running on port 8080.")
	})

	http.HandleFunc("/kv/", getKey())

	http.HandleFunc("/kv", addKey())

	http.HandleFunc("/kv/range", getRange())

	http.HandleFunc("/kv/remove/", removeKey())

	go func() {
		fmt.Println("🚀🚀🚀🚀🚀🚀🚀  Server running on http://localhost:8080")
		if err := srv.ListenAndServe(); err != nil {
			fmt.Println("Error starting server:", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop

	fmt.Println("Shutting down BTree...")
	bp_tree.Shutdown()

	fmt.Println("Gracefully shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		fmt.Println("Forced shutdown:", err)
	}

	fmt.Println("Done.")
}

func main() {
	f, _ := os.Create("trace.out")
	defer f.Close()
	trace.Start(f)
	defer trace.Stop()
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
	items = append(items, NodeData{key: 65, value: []byte("Portland systems")})
	items = append(items, NodeData{key: 55, value: []byte("San Francisco systems")})
	items = append(items, NodeData{key: 67, value: []byte("Austin systems")})
	items = append(items, NodeData{key: 70, value: []byte("NBO systems")})
	items = append(items, NodeData{key: 72, value: []byte("DBX systems")})

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
	//inserted, err := bp_tree.InsertValue(keySlice, valSlice)

	//if err != nil {
	//	panic(err.Error())
	//}

	//inserted, err = bp_tree.InsertValue([][]byte{[]byte("city")}, [][]byte{[]byte("cupertino")})

	//if err != nil {
	//	panic(err.Error())
	//}

	//fmt.Println("Page Inserted: ", inserted)
	fmt.Println("--------------------------------------------------------------------------------------------------------------")
	fmt.Println("--------------------------------------------------------------------------------------------------------------")
	fmt.Println("--------------------------------------------------------------------------------------------------------------")

	//l := make([]NodeData, 0)

	//for i := range 50 {
	//	r := NewRandomNodeData(12)
	//	fmt.Println("Random DATA: ", r)
	//	l = append(l, r)
	//	k := make([]byte, 0)
	//	n := binary.LittleEndian.AppendUint32(k, uint32(r.key))

	//	inserted, err := bp_tree.InsertValue([][]byte{n}, [][]byte{r.value})

	//	if err != nil {
	//		panic(err)
	//	}

	//	fmt.Printf("<><><><><><><><><><><><><><>><><><><><><>><><><><>><><><><><>><<><>><><><><><><><><><><><><><><><><><><><><%d inserted -> %v\n", i, inserted)
	//	time.Sleep(time.Millisecond * 500)
	//}

	//fmt.Println("------------------------------------------------------------------------------------------------------------------------------------------------")
	//fmt.Println("INSERTED DATA:")
	//for _, v := range l {
	//	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	//	fmt.Printf("key: %d\tval: %s\n", v.key, v.value)
	//}

	duration := time.Since(start)
	fmt.Println()
	fmt.Println("Done in ", duration)

	//	go func() {
	//
	//		time.Sleep(time.Second * 5)
	//		fmt.Println("DELETING........")
	//		time.Sleep(time.Second * 1)
	//		k := make([]byte, 0)
	//		n := binary.LittleEndian.AppendUint32(k, uint32(45))
	//		deleted, err := bp_tree.DeleteValue([][]byte{n})
	//
	//		if err != nil {
	//			panic(err.Error())
	//		}
	//
	//		fmt.Println("DELETED: --> ", deleted)
	//
	//	}()

	runProfiler()
	runServer()
}
