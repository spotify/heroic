package com.spotify.heroic.rpc.nativerpc;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

public class NativeUtilsTest {
    // sufficiently large to force compression to use multiple reads.
    public static final int SIZE = 4096 * 200;

    @Test
    public void testGzipUtilities() throws IOException {
        byte[] reference = new byte[SIZE];

        for (int i = 0; i < reference.length; i++) {
            reference[i] = (byte) (i % 10);
        }

        final byte[] compressed = NativeUtils.gzipCompress(reference);
        final byte[] result = NativeUtils.gzipDecompress(SIZE, compressed);

        assertArrayEquals(reference, result);
    }
}
