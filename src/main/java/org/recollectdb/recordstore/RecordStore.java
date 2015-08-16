package org.recollectdb.recordstore;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.recollectdb.common.Function1;
import org.recollectdb.recordstore.ChunkedStreamReader.Chunk;
import org.recollectdb.storage.Storage;

public final class RecordStore {

	private final Storage storage;

	private final int chunkSize;

	private final int dataSize;

	public RecordStore(final short chunkSize, final Storage storage) {
		this.storage = storage;
		this.chunkSize = chunkSize;
		this.dataSize = ChunkInfo.dataSize(chunkSize);
	}

	public void forEachRecord(final Function1<Boolean, ChunkInfo> fun) {
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
				final boolean stopIterating = fun.apply(null);
				if (stopIterating) {
					break;
				}
			}
		}
	}
	
	public void addRecord(final byte type, final byte[] data) {
		addRecord(type,new ByteArrayInputStream(data));
	}

	public void addRecord(final byte type, final ByteBuffer data) {
		addRecord(type,new ByteArrayInputStream(data.array()));
	}

	public void addRecord(final byte type, final InputStream is) {
		final ByteBuffer footer = ChunkInfo.writeBuffer();
		final ChunkedStreamReader dataChunks = new ChunkedStreamReader(is, dataSize);
		final long recordStartOffset = storage.length();
		int chunkIndex = 0;
		while (dataChunks.hasNext()) {
			final Chunk chunk = dataChunks.next();
			final boolean isLastChunk = !dataChunks.hasNext();
			final long chunkOffset = recordStartOffset + (chunkSize * chunkIndex); 
			final ChunkInfo metadata = new ChunkInfo(type,chunkIndex,chunkOffset,chunk.size,isLastChunk);
			metadata.writeInfo(footer);
			storage.write(chunk.data);
			storage.write(footer);
			chunkIndex++;
		}
	}

}
