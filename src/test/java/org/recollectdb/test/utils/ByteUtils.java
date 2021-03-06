package org.recollectdb.test.utils;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

public class ByteUtils {

	private static final Charset UTF8 = Charset.forName("UTF-8");

	private static final CharsetEncoder ENCODER = UTF8.newEncoder();

	public static InputStream streamFromString(String str) {
		try {
			return new ByteArrayInputStream(ENCODER.encode(CharBuffer.wrap(str)).array());
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static ByteBuffer bufferFromString(String str) {
		try {
			ByteBuffer buf = ENCODER.encode(CharBuffer.wrap(str));
			assertEquals(0,buf.position());
			assertEquals(buf.capacity(),buf.remaining());
			assertEquals(buf.capacity(),buf.limit());
			return buf;
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		}
	}

}
