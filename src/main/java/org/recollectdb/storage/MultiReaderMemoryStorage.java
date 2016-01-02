package org.recollectdb.storage;

import java.nio.ByteBuffer;

public class MultiReaderMemoryStorage implements Storage {

	private volatile ByteBuffer storage;

	public MultiReaderMemoryStorage(int size) {
		this.storage = ByteBuffer.allocateDirect(size);
		this.storage.limit(0);
	}

	@Override
	public long write(final ByteBuffer buffer) throws StorageException {
		// Event though duplicate is not atomic (because it has to copy
		// position, limit and mark
		// this is the only method where the buffer is updated. This means: As
		// long as calls to
		// this method are serialized this should be safe.
		final ByteBuffer writeCopy = storage.duplicate();

		// Now that we have a duplicate that we can safely use in a single
		// writer / multiple reader context.
		// This is the case because we will only ever write beyond the limit of
		// the
		// initially duplicated buffer thanks to append-only.

		// Save the current offset
		final int currOffset = writeCopy.position();

		// Write the data
		writeCopy.limit(currOffset + buffer.remaining());
		writeCopy.put(buffer);

		// update the reference so other threads can see the new content
		storage = writeCopy;

		// Return the written to offset
		return currOffset;
	}

	@Override
	public void read(long pointer, final ByteBuffer targetBuffer) throws StorageException {

		// Since writes are handled by swapping the buffer reference, once we
		// have a stable
		// reference we can safely use it.
		final ByteBuffer readCopy = storage.asReadOnlyBuffer();

		if (pointer > readCopy.limit()) {
			throw new StorageException();
		}

		// Set up the reading data
		readCopy.position((int) pointer);
		readCopy.limit((int) pointer + targetBuffer.remaining());

		// Copy data into target buffer
		targetBuffer.mark();
		targetBuffer.put(readCopy);
		targetBuffer.reset();
	}

	@Override
	public long flush() throws StorageException {
		return storage.limit();
	}

	@Override
	public long length() throws StorageException {
		return storage.limit();
	}

}
