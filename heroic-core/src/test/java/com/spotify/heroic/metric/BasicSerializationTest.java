package com.spotify.heroic.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.test.Resources;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class BasicSerializationTest {
    private ObjectMapper mapper = HeroicMappers.json(Mockito.mock(QueryParser.class));

    @Test
    public void testEvent() throws Exception {
        final Event expected = new Event(1024L, ImmutableMap.of("string", "foo"));
        assertSerialization("Event.json", expected, Event.class);
    }

    @Test
    public void testPoint() throws Exception {
        final Point expected = new Point(1024L, 3.14);
        assertSerialization("Point.json", expected, Point.class);
    }

    @Test
    public void testMetricCollection() throws Exception {
        final MetricCollection expected = MetricCollection.points(
            ImmutableList.of(new Point(1000, 10.0d), new Point(2000, 20.0d)));
        assertSerialization("MetricCollection.json", expected, MetricCollection.class);
    }

    @Test
    public void testResultGroup() throws Exception {
        final Set<Series> series = ImmutableSet.of();
        final ResultGroup expected =
            new ResultGroup(ImmutableMap.of(), series, MetricCollection.points(new ArrayList<>()),
                0L);
        assertSerialization("ResultGroup.json", expected, ResultGroup.class);
    }

    @Test
    public void testFullQuery() throws Exception {
        final List<ResultGroup> groups = new ArrayList<>();
        final List<RequestError> errors = new ArrayList<>();
        final FullQuery expected =
            new FullQuery(QueryTrace.of(QueryTrace.identifier("test"), 0L), errors, groups,
                Statistics.empty(), ResultLimits.of());

        assertSerialization("FullQuery.json", expected, FullQuery.class);
    }

    private <T> void assertSerialization(final String json, final T expected, final Class<T> type)
        throws IOException {
        // verify that it is equal to the local file.
        try (final InputStream in = Resources.openResource(getClass(), json)) {
            assertEquals(expected, mapper.readValue(in, type));
        }

        // roundtrip
        final String string = mapper.writeValueAsString(expected);
        assertEquals(expected, mapper.readValue(string, type));
    }
}
