package org.recollectdb.storage;

import java.nio.ByteBuffer;

public class BufferedStorageTest extends BaseStorageTest {

	public Storage createEmptyStorage() throws Exception {
		return new BufferedStorage(new ConcurrentMemoryStorage(1024 * 1024 * 32), ByteBuffer.allocateDirect(8));
	}

}
