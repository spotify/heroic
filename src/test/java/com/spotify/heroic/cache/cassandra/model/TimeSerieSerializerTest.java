package com.spotify.heroic.cache.cassandra.model;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.spotify.heroic.model.TimeSerie;

public class TimeSerieSerializerTest {
    private static final TimeSerieSerializer serializer = TimeSerieSerializer.get();

    private TimeSerie roundTrip(TimeSerie timeSerie) {
        final ByteBuffer bb = serializer.toByteBuffer(timeSerie);
        final TimeSerie after = serializer.fromByteBuffer(bb);
        bb.rewind();
        Assert.assertEquals(bb, serializer.toByteBuffer(after));
        return after;
    }

    @Test
    public void testEmpty() throws Exception {
        final TimeSerie timeSerie = new TimeSerie(null, new HashMap<String, String>());
        Assert.assertEquals(timeSerie, roundTrip(timeSerie));
    }

    @Test
    public void testTagsWithNull() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put(null, null);
        final TimeSerie timeSerie = new TimeSerie(null, tags);
        Assert.assertEquals(timeSerie, roundTrip(timeSerie));
    }

    @Test
    public void testTagsWithMixed() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put(null, null);
        tags.put("foo", "bar");
        tags.put("bar", null);
        final TimeSerie timeSerie = new TimeSerie(null, tags);
        Assert.assertEquals(timeSerie, roundTrip(timeSerie));
    }

    @Test
    public void testStoreSomeValues() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("a", "b");
        tags.put("b", "c");
        final TimeSerie timeSerie = new TimeSerie("baz", tags);
        Assert.assertEquals(timeSerie, roundTrip(timeSerie));
    }
}
