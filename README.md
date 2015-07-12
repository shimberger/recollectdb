# RecollectDB

A simple key value database engine mainly written for my own educational purposes.

Stay tuned.

## Architecture

### Storage Layer

Implements an append only storage layer. Used to abstract between memory and on-disk storage.
The storage layer allows the writing and reading of raw bytes to and from storage. It is designed
to be used in a single writer / multiple reader context. Meaning all writes are serialized but
can be performed in parallel to an unlimited amount of readers.

### Record Layer

Implements a structured data storage layer on top of the storage layer. Allows to add records
to storage and also allows the client to iterate over the records. 

