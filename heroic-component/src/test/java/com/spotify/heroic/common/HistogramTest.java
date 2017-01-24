package com.spotify.heroic.common;

import static org.junit.Assert.assertEquals;

import java.util.Optional;
import org.junit.Test;

public class HistogramTest {
    @Test
    public void basic() {
        final Histogram.Builder builder = Histogram.builder();

        builder.add(100L);
        builder.add(200L);
        builder.add(300L);
        builder.add(400L);

        final Histogram h = builder.build();

        assertEquals(Optional.of(100L), h.getMin());
        assertEquals(Optional.of(400L), h.getMax());
        assertEquals(Optional.of(300L), h.getP75());
        assertEquals(Optional.of(400L), h.getP99());
        assertEquals(Optional.of(250.0), h.getMean());
        assertEquals(Optional.of(1000L), h.getSum());
    }

    @Test
    public void empty() {
        final Histogram.Builder builder = Histogram.builder();

        final Histogram h = builder.build();

        assertEquals(Optional.empty(), h.getMin());
        assertEquals(Optional.empty(), h.getMax());
        assertEquals(Optional.empty(), h.getP75());
        assertEquals(Optional.empty(), h.getP99());
        assertEquals(Optional.empty(), h.getMean());
        assertEquals(Optional.empty(), h.getSum());
    }

    @Test
    public void one() {
        final Histogram.Builder builder = Histogram.builder();

        builder.add(100L);

        final Histogram h = builder.build();

        assertEquals(Optional.of(100L), h.getMin());
        assertEquals(Optional.of(100L), h.getMax());
        assertEquals(Optional.of(100L), h.getP75());
        assertEquals(Optional.of(100L), h.getP99());
        assertEquals(Optional.of(100.0), h.getMean());
        assertEquals(Optional.of(100L), h.getSum());
    }
}
