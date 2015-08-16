package org.recollectdb.recordstore;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

import org.junit.Test;

public class ChunkedStreamReaderTest {

	private static final int CHUNK_SIZE = 2;

	@Test
	public void emptyStreamDoesNoIterations() {
		assertEquals(0, countIterations(newReader(new ByteArrayInputStream(new byte[0]))));
	}

	@Test
	public void emptyStreamDoesOneIterations() {
		assertEquals(1, countIterations(newReader(new ByteArrayInputStream(new byte[] { 1 }))));
	}

	@Test
	public void emptyStreamDoesOneIterations2() {
		assertEquals(1, countIterations(newReader(new ByteArrayInputStream(new byte[] { 1, 2 }))));
	}

	@Test
	public void emptyStreamDoesTwoIterations() {
		assertEquals(2, countIterations(newReader(new ByteArrayInputStream(new byte[] { 1, 2, 3 }))));
	}

	private ChunkedStreamReader newReader(InputStream is) {
		return new ChunkedStreamReader(is, CHUNK_SIZE);
	}

	private static int countIterations(Iterator<?> iter) {
		int count = 0;
		while (iter.hasNext()) {
			iter.next();
			count++;
		}
		return count;
	}

}
