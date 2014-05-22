package com.spotify.heroic.model;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

public class TimeSerieSerializerTest {
    private static final TimeSerieSerializer serializer = TimeSerieSerializer.get();

    private TimeSerie roundTrip(TimeSerie timeSerie) {
        return serializer.fromByteBuffer(serializer.toByteBuffer(timeSerie));
    }

    @Test
    public void testEmpty() throws Exception {
        final TimeSerie timeSerie = new TimeSerie("foo", new HashMap<String, String>());
        Assert.assertEquals(timeSerie, roundTrip(timeSerie));
    }
}
