package org.recollectdb.storage;

import java.nio.ByteBuffer;

// TODO: Concurrency
public class BufferedStorage implements Storage {

	private final Storage underlyingStorage;
	
	private volatile ByteBuffer primaryBuffer;
	
	private volatile ByteBuffer secondaryBuffer;
	
	public BufferedStorage(final Storage storage, final int size) {
		this.primaryBuffer = ByteBuffer.allocateDirect(size);
		this.secondaryBuffer = ByteBuffer.allocateDirect(size);
		this.underlyingStorage = storage;
	}
	
	@Override
	public long write(final ByteBuffer dataBuffer) throws StorageException {
		final long positionBeforeWrite = length();
		final ByteBuffer writeChunk = dataBuffer.duplicate();
		do {
			final int numBytesToWrite = Math.min(dataBuffer.remaining(), primaryBuffer.remaining());
			final int chunkEndPosition = writeChunk.position() + numBytesToWrite; // Current position
			writeChunk.limit(chunkEndPosition);
			underlyingStorage.write(writeChunk);
			dataBuffer.position(chunkEndPosition);
			if (primaryBuffer.remaining() == 0) {
				writeBufferToUnderlyingStorage();
			}
		} while (dataBuffer.remaining() > 0);
		return positionBeforeWrite;
	}

	@Override
	public void read(long pointer, ByteBuffer targetBuffer)
			throws StorageException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long flush() throws StorageException {
		writeBufferToUnderlyingStorage();
		return underlyingStorage.flush();
	}

	@Override
	public long length() throws StorageException {
		return underlyingStorage.length() + primaryBuffer.position();
	}
	
	private void writeBufferToUnderlyingStorage() {
		underlyingStorage.write(primaryBuffer);
		primaryBuffer.rewind();
	}	

}
