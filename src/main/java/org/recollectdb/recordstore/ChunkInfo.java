package org.recollectdb.recordstore;

import java.nio.ByteBuffer;

public final class ChunkInfo {
	
	public static final int METADATA_SIZE = 10;
	
	public final byte type;

	public final int index;
	
	public final long offset;
	
	public final int dataLength;
	
	public final boolean isLast;

	public ChunkInfo(byte type, int index, long offset, int dataLength, boolean isLast) {
		super();
		this.type = type;
		this.index = index;
		this.offset = offset;
		this.dataLength = dataLength;
		this.isLast = isLast;
	}

	public static ChunkInfo readInfo(ByteBuffer buffer) {
		final ByteBuffer metadataBuffer = buffer;
		final byte type = metadataBuffer.get(); // 1
		final int index = metadataBuffer.getInt(); // 4
		final int dataSize = metadataBuffer.getInt(); // 4
		final boolean isLast = metadataBuffer.get() == 1; // 1
		return new ChunkInfo(type,index,0L,dataSize,isLast);
	}
	
	public static int dataSize(int totalChunkSize) {
		return totalChunkSize - METADATA_SIZE;
	}
	
	public static ByteBuffer writeBuffer() {
		return ByteBuffer.allocate(METADATA_SIZE);
	}
	
	public void writeInfo(ByteBuffer buffer) {
		buffer.rewind();
		buffer.put(type);
		buffer.putInt(index);
		buffer.putInt(dataLength);
		if (isLast) {
			buffer.put((byte)1);
		} else {
			buffer.put((byte)0);
		}
	}
	
	
	
}
