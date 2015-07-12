package org.recollectdb.recordstore;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.recollectdb.storage.Storage;

public class RecordStore {

	public static class Writer {
		
		// last|notlast + chunk-index + type + crc32 + size
		public static final short HEADER_SIZE = 1 + 2 + 1 + 8 + 2;
				
		private final RecordStore recordStore;
		
		private final byte recordType;
		
		private final CRC32 crc32 = new CRC32();
		
		private final short chunkDataSize;
		
		private short chunkRemainingBytes = 0;
		
		private short chunkIndex = 0;
		
		private Writer(byte recordType, RecordStore recordStore) {
			this.recordStore = recordStore;
			this.recordType = recordType;
			this.chunkDataSize = (short) (recordStore.chunkSize - HEADER_SIZE);
			this.resetRemainingBytes();
		}
		
		public void write(ByteBuffer buffer) {
			ByteBuffer tempBuffer;
			while (buffer.hasRemaining()) {
				if (chunkRemainingBytes == 0) {
					finishChunk(false);
				}
				int bytesToWrite = Math.min(buffer.remaining(), chunkRemainingBytes);
				tempBuffer = buffer.slice();
				tempBuffer.limit(bytesToWrite);
				crc32.update(tempBuffer);
				tempBuffer.position(0);
				recordStore.storage.write(tempBuffer);
				chunkRemainingBytes -= bytesToWrite;			
				buffer.position(buffer.position() + bytesToWrite);
			}
		}
				
		private long finishChunk(boolean lastChunk) {			
			fillRemainingOfChunkWithEmptyData();
			long offset = writeHeaderData(lastChunk);
			resetRemainingBytes();
			chunkIndex++;
			crc32.reset();
			return offset - recordStore.chunkSize;
		}

		private long writeHeaderData(boolean lastChunk) {
			short chunkSize = (short) (chunkDataSize - chunkRemainingBytes);
			recordStore.headerBuffer.rewind();
			if (lastChunk) {
				recordStore.headerBuffer.put((byte)1);
			} else {
				recordStore.headerBuffer.put((byte)0);
			}
			recordStore.headerBuffer.putShort(chunkIndex);
			recordStore.headerBuffer.put(recordType);
			recordStore.headerBuffer.putLong(crc32.getValue());
			recordStore.headerBuffer.putShort(chunkSize);
			recordStore.headerBuffer.flip();
			recordStore.storage.write(recordStore.headerBuffer);
			return recordStore.storage.length();
		}

		private void fillRemainingOfChunkWithEmptyData() {
			ByteBuffer tempBuffer = recordStore.emptyBuffer.slice();
			tempBuffer.limit(chunkRemainingBytes);
			recordStore.storage.write(tempBuffer);
		}
		
		private void resetRemainingBytes() {
			this.chunkRemainingBytes = chunkDataSize;
		}
		
		
		
	}
		
	private final Storage storage;
	
	public final short chunkSize;
	
	private final ByteBuffer emptyBuffer;
		
	private final ByteBuffer headerBuffer;
		
	public RecordStore(short chunkSize, Storage storage) {
		this.storage = storage;
		this.chunkSize = chunkSize;
		this.emptyBuffer = ByteBuffer.wrap(new byte[chunkSize]).asReadOnlyBuffer();
		this.headerBuffer = ByteBuffer.allocate(Writer.HEADER_SIZE);	
	}
	
	public long getChunkCount() {
		return storage.length() / chunkSize;
	}
	
	public void readChunk(long offset, final ByteBuffer data) {
		if (data.remaining() != chunkSize) {
			throw new RuntimeException("Wrong buffer size");
		}
		if (offset % chunkSize != 0) {
			throw new RuntimeException("Wrong offset value");
		}
		if (offset > storage.length() - chunkSize) {
			throw new RuntimeException("Offset value overflow");
		}				
		storage.read(offset, data);
	}
	
	public long writeRecordOfType(byte type,WriterCallback write) {
		final Writer writer = new Writer(type,this);
		write.perform(writer);
		return writer.finishChunk(true);
	}
	
}
