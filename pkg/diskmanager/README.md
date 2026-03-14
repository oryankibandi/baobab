# DISK LAYOUT DESIGN

## Page and Cell Layout

Below shows the layout structure for the metadata page, 8K page and data cell layout

 ### Metadata Page Layout
 ```bash
 +----------------------+  offset 0
 |     Root Page ID     |
 +----------------------+  offset 4
 |      Version         |
 +----------------------+  offset 8
 |   Tree Height/Depth  |
 +----------------------+  offset 12
 |     Page Count       |
 +----------------------+  offset 16
 | Max Page ID Assigned |
 +----------------------+  offset 20
 ```

 ### Page Layout
```bash
 +----------------------+  offset 0
 | PageHeaderData       |  (fixed-size metadata)
 +----------------------+  offset 51 (lower offset)
 | cell pointer         |  ↓ (one entry per tuple)
 +----------------------+
 | ... free space ...   |
 +----------------------+
 | tuple data ("cells") |  ↑ Data cells
 +----------------------+  offset 8176 (upper offset)
 | special space        |  Padding 16 Bytes (rarely used in heap pages)
 +----------------------+  offset 8192
 ```

### Page Header Layout
```bash
+-----------------------+   offset 0
|     Flag              |
+-----------------------+   offset 1
|     Page ID           |
+-----------------------+   offset 5
|     LSN               |
+-----------------------+   offset 17
|     Item Count        |
+-----------------------+   offset 21
|     Free Space        |
+-----------------------+   offset 25
|     Upper Offset      |
+-----------------------+   offset 29
|     Lower Offset      |
+-----------------------+   offset 33
|     Magic No.         |
+-----------------------+   offset 37
|     Checksum          |
+-----------------------+   offset 39
|     Right Child       |
+-----------------------+   offset 43
|     Right Sibling     |
+-----------------------+   offset 47
|     Left Sibling      |
+-----------------------+   offset 51

```

 ### Cell Layout
 ```bash
 0		         1		          5                  9             13        + key_size      + value_size
 +------------------------------------------------------------------------------------------------+
 | [bytes] Flags | [int] Key Size | [int] value size | []int pageId | [bytes] key | [bytes] value |
 +------------------------------------------------------------------------------------------------+
 ```

### Cell Pointer
```bash
0        1         5
+--------+---------+
| Flags  | Offset  |
+--------+---------+
```

## Flags

### Page Header Flags

The page header has a **8-bit** flag representing each of the following items. From MSB to LSB:

    1. Is internal node (1 for internal,0 for leaf node)
    2. Already written on disk. 1 is has allocation, 0 if new
    3. Dirty - 1 if has unflushed data 0 if clean
    4. Dead - 1 if page is deleted. On disk is always 0 since writes to on disk will always delete dead pages.
    5. TBD
    6. TBD
    7. TBD
    8. TBD
