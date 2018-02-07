package com.spotify.heroic.metric.bigtable.api;

import static com.spotify.heroic.metric.bigtable.api.RowFilter.compareByteStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import org.junit.Test;

public class RowFilterTest {
    final ByteString s = ByteString.copyFrom(new byte[]{0, 0});
    final ByteString a = ByteString.copyFrom(new byte[]{0, 0, 0, 0});
    final ByteString b = ByteString.copyFrom(new byte[]{0, 0, 0, 1});
    final ByteString c = ByteString.copyFrom(new byte[]{0, 0, 0, 2});
    final ByteString d = ByteString.copyFrom(new byte[]{0, 0, 0, Byte.MIN_VALUE});

    @Test
    public void testColumnRange() {
        final RowFilter.ColumnRange sqo =
            RowFilter.newColumnRangeBuilder("family").startQualifierOpen(a).build();

        assertFalse(sqo.matchesColumn(a));
        assertTrue(sqo.matchesColumn(b));

        final RowFilter.ColumnRange sqc =
            RowFilter.newColumnRangeBuilder("family").startQualifierClosed(a).build();

        assertTrue(sqc.matchesColumn(a));
        assertTrue(sqc.matchesColumn(b));

        final RowFilter.ColumnRange eqo =
            RowFilter.newColumnRangeBuilder("family").endQualifierOpen(b).build();

        assertTrue(eqo.matchesColumn(a));
        assertFalse(eqo.matchesColumn(b));

        final RowFilter.ColumnRange eqc =
            RowFilter.newColumnRangeBuilder("family").endQualifierClosed(b).build();

        assertTrue(eqc.matchesColumn(a));
        assertTrue(eqc.matchesColumn(b));
    }

    @Test
    public void testCompareByteStrings() {
        assertEquals(-1, compareByteStrings(s, a));
        assertEquals(1, compareByteStrings(a, s));
        assertEquals(-1, compareByteStrings(a, b));
        assertEquals(0, compareByteStrings(b, b));
        assertEquals(1, compareByteStrings(c, b));
        assertEquals(-1, compareByteStrings(c, d));
    }
}
