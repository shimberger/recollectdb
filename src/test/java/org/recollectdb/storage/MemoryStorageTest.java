package org.recollectdb.storage;

public class MemoryStorageTest extends BaseStorageTest {

	public Storage createEmptyStorage() throws Exception {
		return new ConcurrentMemoryStorage(1024 * 1024 * 32);
	}

}
