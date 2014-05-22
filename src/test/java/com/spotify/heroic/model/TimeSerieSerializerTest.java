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
        final TimeSerie timeSerie = new TimeSerie("", new HashMap<String, String>());
        Assert.assertEquals(timeSerie, roundTrip(timeSerie));
    }

    @Test
    public void testTagsWithNull() throws Exception {
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("foo", null);
        final TimeSerie timeSerie = new TimeSerie("", tags);
        Assert.assertEquals(timeSerie, roundTrip(timeSerie));
    }
}
