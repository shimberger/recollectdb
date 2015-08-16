# RecollectDB

A simple (append-only) key value database engine mainly written for my own educational purposes.

Stay tuned.

## Architecture

### Storage Layer

Implements an append only storage layer. Used to abstract between memory and on-disk storage.
The storage layer allows the writing and reading of raw bytes to and from storage. It is designed
to be used in a single writer / multiple reader context. Meaning all writes should be serialized but
can be performed in parallel to an unlimited amount of readers.

The layer also provides a special BufferedStorage which can wrap an existing storage.
This is useful for writing transactions. It can be used by single thread only though.

### Record Layer

Implements a somewhat structured record layer on top of the storage layer. Allows to add records
to storage and also allows the client to iterate over the records. A record has a type and 
consists of multiple equally n-sized chunks, where the last chunk can be smaller. 
Each chunk contains a footer with the following meta-data:

- type (byte)
- payload size (short)
- index (short)
- isLast (byte)

## Tree Layer

... TBD ...