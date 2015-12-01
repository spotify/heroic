package com.spotify.heroic.common;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class DurationTest {
    @Test
    public void testParseDuration() {
        assertEquals(new Duration(42, TimeUnit.MILLISECONDS), Duration.parseDuration("42ms"));
        assertEquals(new Duration(42, TimeUnit.SECONDS), Duration.parseDuration("42s"));
        assertEquals(new Duration(42, TimeUnit.MINUTES), Duration.parseDuration("42m"));
        assertEquals(new Duration(42, TimeUnit.HOURS), Duration.parseDuration("42h"));
        assertEquals(new Duration(42, TimeUnit.HOURS), Duration.parseDuration("42H"));
        assertEquals(new Duration(42, TimeUnit.DAYS), Duration.parseDuration("42d"));
        assertEquals(new Duration(42 * 7, TimeUnit.DAYS), Duration.parseDuration("42w"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeDuration() {
        Duration.parseDuration("-42ms");
    }
}
