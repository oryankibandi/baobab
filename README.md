# Baobab

Baobab is a simple B+ Tree based key-value store written in Go.

## Caution

This is a work in progress and is not intended for production use - yet.

## Getting started

0. Ensure you have go installed. This project uses go v1.24.2 

1. Clone the repo.
```bash
git clone
```

2. Build and run the server.

```bash
make build && make run
```

This will start the server on port `:8080`


This will create two files in the root directory:
 i. data - the data file where all data will be stored.
 ii. data_fl - a free list file. This stores all the available page IDs on a 4K page.

3. PUT a value.

```bash
ubuntu@ubuntu:~$ curl -X PUT -d '{"key":"data:item_1","val":"value_1"}' http://127.0.0.1:8080/kv
{"key":"data:item_1","val":"value_1"}
ubuntu@ubuntu:~$
```

4. GET the value.

```bash
ubuntu@ubuntu:~$ curl http://127.0.0.1:8080/kv/data:item_1

{"key":"data:item_1","val":"value_1"}
```

5. DELETE the value.

```bash
ubuntu@ubuntu:~$ curl -X DELETE http://127.0.0.1:8080/kv/remove/data:item_1

{"status":"success","deleted":true}
```

6. Check again.

```bash
ubuntu@ubuntu:~$ curl http://127.0.0.1:8080/kv/data:item_1
{"error":"Key not found"}
```


## Tasks
- [x] Design page layout and file format
- [x] Disk Manager
- [x] Background writer
- [x] B+ Tree Algorithm - Splits and merges
- ⏳ Buffer Management - W-TinyLFU
- [ ] Recovery - WAL, Checkpointing
- [ ] Transactions
- [ ] Replication - raft
