# Baobab

![Baobab Logo](logos/baobab.png)

Baobab is a simple B+ Tree based key-value store written in Go.

## Caution

This is a work in progress and is not intended for production use - yet.

## License
This project is licensed under the Mozilla Public License 2.0. 
See LICENSE for details.

##  Highlights

1. B+ Tree index - supports adding keys, retreving keys and range queries.
2. Buffer Manager - TinyLFU filter with W-TinyLFU cache eviction.
3. A background writer to flush dirty pages periodically. Similar to [PostgreSQL](https://www.postgresql.org/docs/current/runtime-config-resource.html#RUNTIME-CONFIG-RESOURCE-BACKGROUND-WRITER).
4. A  disk maneger that supports concurrent writes on distinct page IDs.
5. Free list - A record of available page IDs after deletion.
6. A http API for get, put and range queries

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

7. Range Query

A range query will return a number of items(`item_count`)  starting from the `start_item`.

### Format
```bash
curl http://127.0.0.1:8080/kv/range?start=<start_item>&limit=<item_count>
```

```bash
ubuntu@ubuntu:~$ curl http://127.0.0.1:8080/kv/range?start=data:item_1&limit=4
{
  "status": "success",
  "count": 4,
  "results": [
    {
      "key": "data:item_1",
      "val": "Status_1_Value_1"
    },
    {
      "key": "data:item_10",
      "val": "Status_0_Value_10"
    },
    {
      "key": "data:item_11",
      "val": "Status_1_Value_11"
    },
    {
      "key": "data:item_12",
      "val": "Status_2_Value_12"
    }
  ]
}
```


## Tasks
- [x] Design page layout and file format
- [x] Disk Manager
- [x] Background writer
- [x] B+ Tree Algorithm - Splits and merges
- [ ] Buffer Management - W-TinyLFU (90%)
- [ ] Recovery - WAL, Checkpointing
- [ ] Transactions
- [ ] Replication - Raft
