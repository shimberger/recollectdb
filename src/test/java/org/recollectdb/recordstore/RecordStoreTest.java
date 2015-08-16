package org.recollectdb.recordstore;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.recollectdb.storage.ConcurrentMemoryStorage;
import org.recollectdb.test.utils.ByteUtils;

public class RecordStoreTest {

	private static final short CHUNK_SIZE = (short) 1024;

	private static final int MEGABYTES_32 = 1024 * 1024 * 32;

	private RecordStore store;

	@Before
	public void setup() {
		this.store = new RecordStore(CHUNK_SIZE, new ConcurrentMemoryStorage(MEGABYTES_32));
	}

	@Test
	public void simpleUseCase() {
		store.addRecord((byte) 1, ByteUtils.streamFromString("hello 1"));
		store.addRecord((byte) 2, ByteUtils.streamFromString("hello 2"));
		store.forEachRecord(info -> {
			assertEquals(info.index, 0);
			assertEquals(info.type, 1);
			assertEquals(info.isLast, true);
			assertEquals(info.dataLength, 7);
			return false;
		});
	}

}
