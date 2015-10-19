package com.spotify.heroic.consumer.schemas;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

public class TestSpotify100 {
    @Test
    public void testIgnoreNullAttributes() throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        final Spotify100.JsonMetric m = mapper.readValue("{\"attributes\": {\"foo\": \"value\", \"bar\": null}, \"key\": \"a key\"}", Spotify100.JsonMetric.class);
        Assert.assertEquals(ImmutableMap.of("foo", "value"), m.getAttributes());
        Assert.assertEquals("a key", m.getKey());
    }
}