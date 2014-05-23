package com.spotify.heroic.model;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TimeSerieSerializerTest {
    private static final TimeSerieSerializer serializer = TimeSerieSerializer.get();

    private TimeSerie roundTrip(TimeSerie timeSerie) {
        return serializer.fromByteBuffer(serializer.toByteBuffer(timeSerie));
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
    public void testStoreSomeValues() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("foo", "bar");
        final TimeSerie timeSerie = new TimeSerie("baz", tags);
        Assert.assertEquals(timeSerie, roundTrip(timeSerie));
    }
}
