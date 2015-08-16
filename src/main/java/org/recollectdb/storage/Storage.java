package org.recollectdb.storage;

import java.nio.ByteBuffer;

/**
 * Interface to abstract away different storage mechanisms (e.g. in-memory or disk).
 * 
 */
public interface Storage {
 
	/**
	 * Writes the data from position to limit.
	 * The implementation is free to reset the mark of the buffer.
	 * Does NOT return the buffer ready for reading (e.g position == limit).
	 * 
	 * @param buffer The data to write
	 * @return The offset where the data was written to
	 * @throws StorageException If an error occurs
	 */
	public long write(final ByteBuffer buffer) throws StorageException;
	
	/**
	 * The implementation is free to reset the mark of the buffer.
	 * Returns the buffer ready for reading the data (e.g. correct position and limit).
	 * 
	 * @param pointer The offset to read from
	 * @param targetBuffer The buffer to read into
	 * @throws StorageException If an error occurs
	 */
	public void read(final long pointer, final ByteBuffer targetBuffer) throws StorageException;
	
	/**
	 * Flushes the underlying storage if possible.
	 * 
	 * @return The size of the storage
	 * @throws StorageException If an error occurs
	 */
	public long flush() throws StorageException;
	
	/**
	 * Returns the size of the store.
	 * 
	 * @return The size of the storage.
	 * @throws StorageException If an error occurs
	 */
	public long length() throws StorageException;
		
}
