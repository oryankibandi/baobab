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

 ### Page Layout(Heap Page)
```bash
 +----------------------+  offset 0
 | PageHeaderData       |  (fixed-size metadata)
 +----------------------+  offset 27
 | cell pointer         |  ↓ (one entry per tuple)
 +----------------------+
 | ... free space ...   |
 +----------------------+
 | tuple data ("cells") |  ↑ Data cells
 +----------------------+  offset 8176
 | special space        |  Padding 16 Bytes (rarely used in heap pages)
 +----------------------+  offset 8192
 ```

 ### Cell Layout
 ```bash
 0		         1		          5                  9             13        + key_size      + value_size
 +------------------------------------------------------------------------------------------------+
 | [bytes] Flags | [int] Key Size | [int] value size | []int pageId | [bytes] key | [bytes] value |
 +------------------------------------------------------------------------------------------------+
 ```

## Flags

### Page Header Flags

The page header has a **8-bit** flag representing each of the following items. From MSB to LSB:

    1. Is internal node (1 for internal,0 for leaf node)
    2. Already written on disk. 1 is has allocation, 0 if new
    3. Dirty - 1 if has unflushed data 0 if clean
    4. To Be Determined (TBD)
    5. TBD
    6. TBD
    7. TBD
    8. TBD
