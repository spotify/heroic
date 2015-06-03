package com.spotify.heroic.metric.datastax;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.metric.datastax.serializer.SeriesSerializer;
import com.spotify.heroic.model.Series;

public class SeriesSerializerTest {
    private static final SeriesSerializer serializer = new SeriesSerializer();

    private Series roundTrip(Series series) {
        final ByteBuffer bb = serializer.serialize(series);
        final Series after = serializer.deserialize(bb);
        bb.rewind();
        Assert.assertEquals(bb, serializer.serialize(after));
        return after;
    }

    @Test
    public void testEmpty() throws Exception {
        final Series series = new Series(null, new HashMap<String, String>());
        Assert.assertEquals(series, roundTrip(series));
    }

    @Test
    public void testTagsWithNull() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put(null, null);
        final Series series = new Series(null, tags);
        Assert.assertEquals(series, roundTrip(series));
    }

    @Test
    public void testTagsWithMixed() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put(null, null);
        tags.put("foo", "bar");
        tags.put("bar", null);
        final Series series = new Series(null, tags);
        Assert.assertEquals(series, roundTrip(series));
    }

    @Test
    public void testStoreSomeValues() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("a", "b");
        tags.put("b", "c");
        final Series series = new Series("baz", tags);
        Assert.assertEquals(series, roundTrip(series));
    }
}
