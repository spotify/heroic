package com.spotify.heroic.metric.datastax.serialization.legacy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.datastax.schema.legacy.SeriesSerializer;

public class SeriesSerializerTest {
    private static final SeriesSerializer serializer = new SeriesSerializer();

    private Series roundTrip(Series series) throws IOException {
        final ByteBuffer bb = serializer.serialize(series);
        final Series after = serializer.deserialize(bb);
        bb.rewind();
        Assert.assertEquals(bb, serializer.serialize(after));
        return after;
    }

    @Test
    public void testEmpty() throws Exception {
        final Series series = Series.of(null, new HashMap<String, String>());
        Assert.assertEquals(series, roundTrip(series));
    }

    @Test
    public void testStoreSomeValues() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("a", "b");
        tags.put("b", "c");
        final Series series = Series.of("baz", tags);
        Assert.assertEquals(series, roundTrip(series));
    }
}
