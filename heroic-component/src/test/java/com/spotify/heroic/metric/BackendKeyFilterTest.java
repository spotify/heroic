package com.spotify.heroic.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.spotify.heroic.common.OptionalLimit;
import java.util.Optional;
import org.junit.Test;
import org.mockito.Mockito;

public class BackendKeyFilterTest {
    @Test
    public void testEmpty() {
        BackendKeyFilter f = new BackendKeyFilter();
        assertEquals(Optional.empty(), f.getStart());
        assertEquals(Optional.empty(), f.getEnd());
        assertEquals(OptionalLimit.empty(), f.getLimit());
    }

    @Test
    public void testMutations() {
        final BackendKeyFilter a = new BackendKeyFilter();
        assertEquals(Optional.empty(), a.getStart());
        assertEquals(Optional.empty(), a.getEnd());
        assertEquals(OptionalLimit.empty(), a.getLimit());

        final BackendKeyFilter.Start start = Mockito.mock(BackendKeyFilter.Start.class);
        final BackendKeyFilter.End end = Mockito.mock(BackendKeyFilter.End.class);
        final OptionalLimit limit = OptionalLimit.of(10L);

        final BackendKeyFilter b = a.withStart(start);

        assertNotEquals(a, b);
        assertEquals(Optional.of(start), b.getStart());
        assertEquals(Optional.empty(), b.getEnd());
        assertEquals(OptionalLimit.empty(), b.getLimit());

        final BackendKeyFilter c = b.withEnd(end);

        assertNotEquals(b, c);
        assertEquals(Optional.of(start), c.getStart());
        assertEquals(Optional.of(end), c.getEnd());
        assertEquals(OptionalLimit.empty(), c.getLimit());

        final BackendKeyFilter d = c.withLimit(limit);

        assertNotEquals(c, d);
        assertEquals(Optional.of(start), d.getStart());
        assertEquals(Optional.of(end), d.getEnd());
        assertEquals(limit, d.getLimit());
    }
}
