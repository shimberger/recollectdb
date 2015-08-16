package org.recollectdb.storage;

import java.io.File;

public class FileStorageTest extends BaseStorageTest {

	@Override
	public Storage createEmptyStorage() throws Exception {
		return new ConcurrentFileStorage(new File("test.tmp"));
	}

}
