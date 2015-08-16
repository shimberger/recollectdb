package org.recollectdb.recordstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.recollectdb.recordstore.ChunkedStreamReader.Chunk;

public final class ChunkedStreamReader implements Iterator<Chunk> {

	public static final class Chunk {
		
		public final ByteBuffer data;
		
		public final int size;
		
		private Chunk(final ByteBuffer buf, final int size) {
			this.data = buf;
			this.size = size;
		}
		
	}
	
	private final PushbackInputStream is;
		
	private final byte[] chunkData;
	
	private final ByteBuffer chunkBuffer;
				
	public ChunkedStreamReader(final InputStream is, int chunkSize) {
		this.is = new PushbackInputStream(is,1);
		this.chunkData = new byte[chunkSize];
		this.chunkBuffer = ByteBuffer.wrap(chunkData);
	}	
	
	@Override
	public boolean hasNext() {
		try {
			final int nextByte = is.read();
			if (nextByte == -1) {
				return false;
			} else {
				is.unread(nextByte);
				return true;	
			}			
		} catch (IOException e) {
			throw new RuntimeException("Error reading chunks from stream",e);
		}
	}


	@Override
	public Chunk next() {
		int offset = 0;
		int lastReadByte = 0;
		try {
			while (offset < chunkData.length && (lastReadByte = is.read(chunkData, offset, chunkData.length - offset)) != -1) {
				if (lastReadByte != -1) {
					// If data was read increase position
					offset += lastReadByte;
				}
			}
			if (offset == 0 && lastReadByte == -1) {
				// No byte was read and end of stream encountered
				throw new NoSuchElementException("Called next() on finished " + this.getClass().getName());
			}
			chunkBuffer.rewind();
			return new Chunk(chunkBuffer,offset);
		} catch (IOException e) {
			throw new RuntimeException("Error reading chunks from stream",e);
		}
	}
	
}
