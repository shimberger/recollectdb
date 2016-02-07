package org.recollectdb.storage;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.junit.Before;
import org.junit.Test;

import static org.recollectdb.test.utils.ByteUtils.bufferFromString;

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
		assertEquals(0L, storage.write(bufferFromString("test")));
		assertEquals(4L, storage.write(bufferFromString("foo")));
		assertEquals(7L, storage.write(bufferFromString("foo")));
		assertEquals(10L, storage.length());
	}

	@Test
	public void flushingDoesNotFail() {
		for (int i = 0; i < 8; i++) {
			storage.write(bufferFromString("test1"));
			storage.write(bufferFromString("test2"));
			storage.write(bufferFromString("test3"));
			storage.flush();
		}
	}

	@Test
	public void writingDoesNotChangeBuffer() {
		ByteBuffer string = bufferFromString("test1");
		ByteBuffer stringCopy = string.duplicate();
		storage.write(string);
		assertEquals(stringCopy.position(),string.position());
		assertEquals(stringCopy.limit(),string.limit());
		assertEquals(stringCopy.capacity(),string.capacity());
		assertEquals(stringCopy.mark(),string.mark());
	}		
	
	@Test
	public void bufferLimitIsRespectedd() {
		ByteBuffer string = bufferFromString("test1");
		assertEquals(0, string.position());
		assertEquals(5, string.limit());
		assertEquals(5, string.capacity());
		string.limit(4);
		long offset = storage.write(string);
		assertEquals(0, offset);
		assertEquals(4, storage.length());
	}

	@Test
	public void writingAndThenReadingWorks() {
		ByteBuffer targetBuf = ByteBuffer.allocate(3);
		storage.write(bufferFromString("test"));
		long fooOffset = storage.write(bufferFromString("foo"));
		storage.read(fooOffset, targetBuf);
		assertEquals(3, targetBuf.remaining());
		assertEquals(0, targetBuf.position());
		assertEquals(bufferFromString("foo"), targetBuf);
	}

	@Test
	public void multipleWritingAndReadingWorks() {
		String[] tests = new String[] { "test1", "test2", "test3" };
		int length = 0;
		for (String str : tests) {
			ByteBuffer targetBuf = ByteBuffer.allocate(str.length());
			long offset = storage.write(bufferFromString(str));
			storage.read(offset, targetBuf);
			assertEquals(bufferFromString(str), targetBuf);
			length += str.length();
		}
		assertEquals(length, storage.length());
	}

}
