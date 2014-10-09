package com.spotify.heroic.aggregationcache.cassandra.model;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.StartsWithFilter;
import com.spotify.heroic.filter.cassandra.FilterSerializer;

public class FilterSerializerTest {
    private static final FilterSerializer serializer = FilterSerializer.get();

    private Filter roundTrip(Filter filter) {
        final ByteBuffer bb = serializer.toByteBuffer(filter);
        final Filter after = serializer.fromByteBuffer(bb);
        bb.rewind();
        // Assert.assertEquals(bb, serializer.toByteBuffer(after));
        return after;
    }

    final Filter MATCH_TAG = new MatchTagFilter("foo", "bar");
    final Filter STARTS_WITH = new StartsWithFilter("foo", "bar");

    final Filter OR = new OrFilter(Arrays.asList(new Filter[] { MATCH_TAG,
            STARTS_WITH }));

    final Filter AND = new AndFilter(Arrays.asList(new Filter[] { MATCH_TAG,
            STARTS_WITH }));

    @Test
    public void testOne() throws Exception {
        Assert.assertEquals(AND, roundTrip(AND));
    }

    @Test
    public void testMatchTag() throws JsonProcessingException {
        Assert.assertEquals(MATCH_TAG, roundTrip(MATCH_TAG));
    }

    @Test
    public void testSerializeNull() throws JsonProcessingException {
        Assert.assertEquals(null, roundTrip(null));
    }
}