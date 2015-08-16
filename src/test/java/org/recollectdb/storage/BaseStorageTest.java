package org.recollectdb.storage;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.junit.Before;
import org.junit.Test;

public abstract class BaseStorageTest {

	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static final CharsetEncoder ENCODER = UTF8.newEncoder();

	private Storage storage;

	public abstract Storage createEmptyStorage() throws Exception;

	@Before
	public void setup() throws Exception {
		storage = createEmptyStorage();
	}

	@Test(expected = StorageException.class)
	public void readingOutOfBoundsPointerFails() {
		ByteBuffer targetBuf = ByteBuffer.allocate(3);
		storage.read(8L, targetBuf);
	}

	@Test
	public void newStorageIsEmpty() {
		assertEquals(0L, storage.length());
	}

	@Test
	public void writingReturnsCorrectOffset() {
		assertEquals(0L, storage.length());
		assertEquals(0L, storage.write(fromString("test")));
		assertEquals(4L, storage.write(fromString("foo")));
		assertEquals(7L, storage.length());
	}

	@Test
	public void flushingDoesNotFail() {
		storage.write(fromString("test1"));
		storage.write(fromString("test2"));
		storage.write(fromString("test3"));
		storage.flush();
	}

	@Test
	public void bufferLimitIsRespectedd() {
		ByteBuffer string = fromString("test1");
		assertEquals(0, string.position());
		assertEquals(5, string.limit());
		assertEquals(5, string.capacity());
		string.limit(4);
		long offset = storage.write(string);
		assertEquals(0, offset);
		assertEquals(4, storage.length());
		assertEquals(4, string.position());
	}

	@Test
	public void writingAndThenReadingWorks() {
		ByteBuffer targetBuf = ByteBuffer.allocate(3);
		storage.write(fromString("test"));
		long fooOffset = storage.write(fromString("foo"));
		storage.read(fooOffset, targetBuf);
		assertEquals(fromString("foo"), targetBuf);
	}

	@Test
	public void multipleWritingAndReadingWorks() {
		String[] tests = new String[] { "test1", "test2", "test3" };
		int length = 0;
		for (String str : tests) {
			ByteBuffer targetBuf = ByteBuffer.allocate(str.length());
			long offset = storage.write(fromString(str));
			storage.read(offset, targetBuf);
			assertEquals(fromString(str), targetBuf);
			length += str.length();
		}
		assertEquals(length, storage.length());
	}

	private ByteBuffer fromString(String str) {
		try {
			return ENCODER.encode(CharBuffer.wrap(str));
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		}
	}

}
