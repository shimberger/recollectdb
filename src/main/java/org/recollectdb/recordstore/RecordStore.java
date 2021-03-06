package org.recollectdb.recordstore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.recollectdb.recordstore.ChunkedStreamReader.Chunk;
import org.recollectdb.storage.Storage;

public final class RecordStore {

	private final Storage storage;

	private final int chunkSize;

	private final int dataSize;

	public RecordStore(final short chunkSize, final Storage storage) {
		this.storage = storage ;
		this.chunkSize = chunkSize;
		this.dataSize = ChunkInfo.dataSize(chunkSize);
	}
	
	public void forEachRecord(final Function<ChunkInfo,Boolean> fun) {
		// determine the end offset of the last chunk, this can not be the exact
		// size
		// of storage if the last write got interrupted mid-chunk
		final long lastChunkEnd = (storage.length() / chunkSize) * chunkSize;

		// iterate through the chunks backwards until we reach the first one
		// the offset will be the last byte of the chunk
		final ByteBuffer currChunkContent = ByteBuffer.allocate(chunkSize);
		for (long currChunkEnd = lastChunkEnd; currChunkEnd >= chunkSize; currChunkEnd -= chunkSize) {
			// compute start offset
			final long currChunkBegin = currChunkEnd - chunkSize;

			// read the data
			currChunkContent.rewind();
			storage.read(currChunkBegin, currChunkContent);

			// Read the info
			final ChunkInfo chunkInfo = ChunkInfo.readInfo(currChunkContent);
			if (chunkInfo.isLast) {
				//final RecordInfo recordInfo = RecordInfo.wrap(chunkInfo);
				final boolean continueIteration = fun.apply(chunkInfo);
				if (!continueIteration) {
					break;
				}
			}
		}
	}
	
	public int getChunkDataSize() {
		return this.dataSize;
	}
	
	public void readRecord(final long recordOffset, Consumer<ByteBuffer> chunkHandler) throws RecordStoreException {
		ensureValidOffset(recordOffset);		
		final ByteBuffer chunk = ByteBuffer.allocate(chunkSize);
		storage.read(recordOffset, chunk);
		final ChunkInfo chunkInfo = ChunkInfo.readInfo(chunk);
		if (chunkInfo.isLast) {			
			chunkHandler.accept(getChunkData(chunk));
		} else {
			for (long o = recordOffset - (chunkInfo.index * chunkSize); o < recordOffset; o += chunkSize) {
				chunk.clear();
				storage.read(o,chunk);				
				chunkHandler.accept(getChunkData(chunk));
			}
		}
	}
	
	public void readRecord(final long recordOffset, final OutputStream os) throws RecordStoreException {
		readRecord(recordOffset,(ByteBuffer buf) -> {
			try {
				os.write(buf.array());
			} catch (IOException e ){
				throw new RuntimeException("Error writing record to stream",e);
			}
		});
	}

	private void ensureValidOffset(final long offset) throws RecordStoreException {
		if (offset % chunkSize != 0) {
			throw new RecordStoreException("Offset is not multiple of chunk size " + offset);
		}
	}
	
	private ByteBuffer getChunkData(final ByteBuffer buffer) {
		final ChunkInfo chunkInfo = ChunkInfo.readInfo(buffer);
		buffer.limit(chunkInfo.dataLength);
		return buffer.slice().asReadOnlyBuffer();
	}
	
	public long addRecord(final byte type, final ByteBuffer data) {
		return addRecord(type,new ByteArrayInputStream(data.array()));
	}

	public long addRecord(final byte type, final InputStream is) {
		final ByteBuffer footer = ChunkInfo.writeBuffer();
		final ChunkedStreamReader dataChunks = new ChunkedStreamReader(is, dataSize);
		final long recordStartOffset = storage.length();
		int chunkIndex = -1;
		while (dataChunks.hasNext()) {
			chunkIndex++;
			final Chunk chunk = dataChunks.next();
			final boolean isLastChunk = !dataChunks.hasNext();
			final long chunkOffset = recordStartOffset + (chunkSize * chunkIndex); 
			final ChunkInfo metadata = new ChunkInfo(type,chunkIndex,chunkOffset,chunk.size,isLastChunk);
			metadata.writeInfo(footer);
			storage.write(chunk.data);
			storage.write(footer);
		}
		return recordStartOffset + (chunkIndex * chunkSize);
	}

}
