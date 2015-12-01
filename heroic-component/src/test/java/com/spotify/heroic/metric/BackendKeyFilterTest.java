package com.spotify.heroic.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

public class BackendKeyFilterTest {
    @Test
    public void testEmpty() {
        BackendKeyFilter f = BackendKeyFilter.of();
        assertEquals(Optional.empty(), f.getStart());
        assertEquals(Optional.empty(), f.getEnd());
        assertEquals(Optional.empty(), f.getLimit());
    }

    @Test
    public void testMutations() {
        final BackendKeyFilter a = BackendKeyFilter.of();
        assertEquals(Optional.empty(), a.getStart());
        assertEquals(Optional.empty(), a.getEnd());
        assertEquals(Optional.empty(), a.getLimit());

        final BackendKeyFilter.Start start = Mockito.mock(BackendKeyFilter.Start.class);
        final BackendKeyFilter.End end = Mockito.mock(BackendKeyFilter.End.class);
        final int limit = 10;

        final BackendKeyFilter b = a.withStart(start);

        assertNotEquals(a, b);
        assertEquals(Optional.of(start), b.getStart());
        assertEquals(Optional.empty(), b.getEnd());
        assertEquals(Optional.empty(), b.getLimit());

        final BackendKeyFilter c = b.withEnd(end);

        assertNotEquals(b, c);
        assertEquals(Optional.of(start), c.getStart());
        assertEquals(Optional.of(end), c.getEnd());
        assertEquals(Optional.empty(), c.getLimit());

        final BackendKeyFilter d = c.withLimit(limit);

        assertNotEquals(c, d);
        assertEquals(Optional.of(start), d.getStart());
        assertEquals(Optional.of(end), d.getEnd());
        assertEquals(Optional.of(limit), d.getLimit());
    }
}
