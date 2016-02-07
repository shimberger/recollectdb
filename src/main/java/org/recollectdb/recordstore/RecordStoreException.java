package org.recollectdb.recordstore;

import org.recollectdb.storage.StorageException;

public class RecordStoreException extends StorageException {

	private static final long serialVersionUID = 1L;

	public RecordStoreException(String string) {
		super(string);
	}	
	
}
