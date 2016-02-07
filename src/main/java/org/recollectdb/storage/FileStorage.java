package org.recollectdb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileStorage implements Storage {

	private final FileChannel file;

	public FileStorage(Path path) throws IOException {		
		this.file = FileChannel.open(path,StandardOpenOption.CREATE,StandardOpenOption.WRITE,StandardOpenOption.READ);
	}

	@Override
	public long write(ByteBuffer originalBuffer) throws StorageException {
		// First duplicate buffer so we can modify limit etc.
		final ByteBuffer buffer = originalBuffer.duplicate();
		
		// Now write content to file
		try {
			long currLen = file.position();
			file.write(buffer);
			return currLen;
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public void read(long pointer, ByteBuffer targetBuffer) throws StorageException {
		try {
			if (pointer > file.position()) {
				throw new StorageException();
			}
			targetBuffer.mark();
			file.read(targetBuffer, pointer);
			targetBuffer.reset();
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public long flush() throws StorageException {
		try {
			file.force(false);
			return file.size();
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public long length() throws StorageException {
		try {
			return file.position();
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

}
