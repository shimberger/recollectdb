package org.recollectdb.storage;

import java.nio.ByteBuffer;

// Not Thread Safe
public class BufferedStorage implements Storage {

	private final Storage underlyingStorage;

	private final ByteBuffer writeBuffer;

	public BufferedStorage(final Storage storage, final ByteBuffer buffer) {
		this.underlyingStorage = storage;
		this.writeBuffer = buffer;
	}

	@Override
	public long write(final ByteBuffer originalBuffer) throws StorageException {
		// First duplicate buffer so we can modify limit etc.
		final ByteBuffer dataBuffer = originalBuffer.duplicate();
		
		final long positionBeforeWrite = length();
		final ByteBuffer writeChunk = dataBuffer.duplicate();
		do {
			final int numBytesToWrite = Math.min(dataBuffer.remaining(), writeBuffer.remaining());
			final int chunkEndPosition = writeChunk.position() + numBytesToWrite; // Current
																					// position
			writeChunk.limit(chunkEndPosition);
			underlyingStorage.write(writeChunk);
			dataBuffer.position(chunkEndPosition);
			if (writeBuffer.remaining() == 0) {
				writeBufferToUnderlyingStorage();
			}
		} while (dataBuffer.remaining() > 0);
		return positionBeforeWrite;
	}

	@Override
	public void read(long pointer, ByteBuffer targetBuffer) throws StorageException {
		final long storageSize = underlyingStorage.length();
		final long virtualLength = storageSize + writeBuffer.position();
		if (pointer > virtualLength) {
			throw new StorageException();
		}
		if (pointer <= storageSize) {
			underlyingStorage.read(pointer, targetBuffer);
		}
		if (targetBuffer.remaining() > 0) {
			final long cachePointer = Math.abs(pointer - storageSize);
			if (cachePointer <= writeBuffer.position()) {
				final int lastPos = writeBuffer.position();
				writeBuffer.position((int) cachePointer);
				targetBuffer.put(writeBuffer);
				writeBuffer.position(lastPos);
			}
		}

	}

	@Override
	public long flush() throws StorageException {
		if (writeBuffer != null) {
			writeBufferToUnderlyingStorage();
		}
		return underlyingStorage.flush();
	}

	@Override
	public long length() throws StorageException {
		if (writeBuffer != null) {
			return underlyingStorage.length() + writeBuffer.position();
		}
		return underlyingStorage.length();
	}

	private void writeBufferToUnderlyingStorage() {
		underlyingStorage.write(writeBuffer);
		writeBuffer.rewind();
	}

}
