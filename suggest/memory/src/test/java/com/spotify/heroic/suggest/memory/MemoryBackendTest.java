package com.spotify.heroic.suggest.memory;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MemoryBackendTest {
    @Test
    public void testAnalyze() {
        assertEquals(
            ImmutableSet.of("w", "wo", "wor", "worl", "world", "h", "he", "hel", "hell", "hello"),
            MemoryBackend.analyze("HelloWorld"));

        assertEquals(ImmutableSet.of("a", "b"), MemoryBackend.analyze("a-b"));
    }
}
