package org.recollectdb.storage;

import java.nio.file.Paths;

public class FileStorageTest extends BaseStorageTest {

	@Override
	public Storage createEmptyStorage() throws Exception {
		return new FileStorage(Paths.get("test.tmp"));
	}

}
