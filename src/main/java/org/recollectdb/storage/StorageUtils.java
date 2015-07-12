package org.recollectdb.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

public class StorageUtils {

	// Only useful for append only
	public static final int writeAndSwap(
			final AtomicReference<ByteBuffer> ref, 
			final ByteBuffer src) {
		// Event though duplicate is not atomic (because it has to copy position, limit and mark
		// this is the only method where the buffer is updated. This means: As long as calls to
		// this method are serialized this should be safe.
		final ByteBuffer writeCopy = ref.get().duplicate();

		// Now that we have a duplicate that we can safely use in a single writer / multiple reader context.
		// This is the case because we will only ever write beyond the limit of the 
		// duplicated buffer thanks to append-only.		
		
		// Save the current offset
		final int currOffset = writeCopy.position();
		
		// Write the data
		writeCopy.limit(currOffset + src.remaining());		
		writeCopy.put(src);
		
		// update the reference so other threads can see the new content
		ref.set(writeCopy);

		// Return the written to offset
		return currOffset;				
	}
	
}
