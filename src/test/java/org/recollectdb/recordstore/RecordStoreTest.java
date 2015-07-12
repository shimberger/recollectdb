package org.recollectdb.recordstore;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.zip.CRC32;

import org.junit.Before;
import org.junit.Test;
import org.recollectdb.storage.MemoryStorage;

public class RecordStoreTest {

	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static final CharsetEncoder ENCODER = UTF8.newEncoder();	
	
	private static final short CHUNK_SIZE = (short) 1024;
	
	private static final int MEGABYTES_32 = 1024*1024*32;
	
	private RecordStore store;
	
	@Before
	public void setup() {
		this.store = new RecordStore(CHUNK_SIZE,new MemoryStorage(MEGABYTES_32));
	}
	
	@Test
	public void emptyStoreHasZeroChunkCount() {
		assertEquals(0,store.getChunkCount());
	}

	@Test
	public void singleChunkWritesCorrect() {
		ByteBuffer chunkData = ByteBuffer.allocate(CHUNK_SIZE);
		store.writeRecordOfType((byte) 99, (RecordStore.Writer cb) -> {
				cb.write(fromString("Hello World"));
			});		
		store.readChunk(0L, chunkData);
		ByteBuffer headerData = ByteBuffer.wrap(chunkData.array(), CHUNK_SIZE-RecordStore.Writer.HEADER_SIZE, RecordStore.Writer.HEADER_SIZE);
		assertEquals(RecordStore.Writer.HEADER_SIZE,headerData.remaining());
		assertEquals((byte)1,headerData.get());
		assertEquals((short)0,headerData.getShort());
		assertEquals((byte)99,headerData.get());
		assertEquals(crc32For(fromString("Hello World")),headerData.getLong());
	}	
	
	@Test
	public void simpleUseCase() {
		long lastOffset = -1L;
		lastOffset = store.writeRecordOfType((byte) 1, (RecordStore.Writer cb) -> {
			cb.write(fromString("Hello"));
		});
		assertEquals(0,lastOffset);
		lastOffset = store.writeRecordOfType((byte) 1, (RecordStore.Writer cb) -> {
			cb.write(fromString("World"));
		});
		assertEquals(1024,lastOffset);
		assertEquals(2,store.getChunkCount());
	}
	
	@Test
	public void overflowWrite() {
		long offset = store.writeRecordOfType((byte) 1, (RecordStore.Writer cb) -> {
			cb.write(ByteBuffer.allocate(2));
			cb.write(ByteBuffer.allocate(2));
			cb.write(ByteBuffer.wrap(new byte[2000]));			
		});		
		assertEquals(0,offset);		
	}
	
	private long crc32For(ByteBuffer buff) {
		CRC32 crc32 = new CRC32();
		crc32.update(buff);
		return crc32.getValue();
	}
	
	private ByteBuffer fromString(String str) {
		try {
			return ENCODER.encode(CharBuffer.wrap(str));
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		}
	}	
	
}
