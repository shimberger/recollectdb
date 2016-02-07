package org.recollectdb.recordstore;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.recollectdb.storage.MemoryStorage;
import org.recollectdb.test.utils.ByteUtils;

public class RecordStoreTest {

	private static final short CHUNK_SIZE = (short) 1024;

	private static final int MEGABYTES_32 = 1024 * 1024 * 32;

	private RecordStore store;

	@Before
	public void setup() {
		this.store = new RecordStore(CHUNK_SIZE, new MemoryStorage(MEGABYTES_32));
	}

	@Test
	public void simpleUseCase() {
		long lastOffset = -1;
		
		// Write first record and expect it to be at offset 0
		lastOffset = store.addRecord((byte) 2, ByteUtils.streamFromString("hello 1"));
		assertEquals(0L, lastOffset);
		
		// Write second record and expect it to be at offset 0 + CHUNK_SIZE
		lastOffset = store.addRecord((byte) 8, ByteUtils.streamFromString("hello 2"));
		assertEquals(CHUNK_SIZE, lastOffset);
		
		// Expect there are two records in the store
		assertEquals(2,countRecords(store));
		
		// Validate the record metadata
		store.forEachRecord(info -> {
			assertEquals(0, info.index);
			assertEquals((byte) 8, info.type);
			assertEquals(true, info.isLast);
			assertEquals(7, info.dataLength);
			return false;
		});
		
		// validate the record data
		store.readRecord(CHUNK_SIZE, chunk -> {
			assertEquals(7,chunk.capacity());
		});
		
	}
	
	private static int countRecords(final RecordStore rs) {
		final AtomicInteger count = new AtomicInteger(0);
		rs.forEachRecord(info -> {
			count.getAndIncrement();
			return true;
		});
		return count.get();
	}

}
