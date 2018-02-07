package com.spotify.heroic.metric.bigtable;

import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import com.spotify.heroic.bigtable.com.google.protobuf.ByteString.ByteIterator;
import eu.toolchain.serializer.HexUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BigtableBackendTest {
    @Test
    public void testAllBoundaries() {
        checkSmaller(0x00L, 0xFFL);
        checkSmaller(0xFFL, 0x100L);
        checkSmaller(0x100L, 0xffffL);
        checkSmaller(0xffffL, 0x10000L);
        checkSmaller(0x10000L, 0xffffffL);
        checkSmaller(0xffffffL, 0x1000000L);
        checkSmaller(0x1000000L, 0xffffffffL);
    }

    private void checkSmaller(final long s, final long l) {
        assertEquals(BigtableBackend.offset(s), s);
        assertEquals(BigtableBackend.offset(l), l);

        final ByteString a = BigtableBackend.serializeOffset(s);
        final ByteString b = BigtableBackend.serializeOffset(l);

        assertTrue(String.format("%s < %s", HexUtils.toHex(a.toByteArray()),
            HexUtils.toHex(b.toByteArray())), compare(a, b) < 0);
        assertEquals(s, BigtableBackend.deserializeOffset(a));
        assertEquals(l, BigtableBackend.deserializeOffset(b));
    }

    int compare(ByteString a, ByteString b) {
        ByteIterator itA = a.iterator();
        ByteIterator itB = b.iterator();

        while (itA.hasNext()) {
            if (!itB.hasNext()) {
                return -1;
            }

            int bA = itA.nextByte() & 0xff;
            int bB = itB.nextByte() & 0xff;

            int c = Integer.compare(bA, bB);

            if (c != 0) {
                return c;
            }
        }

        if (itB.hasNext()) {
            return 1;
        }

        return 0;
    }
}
