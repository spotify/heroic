package com.spotify.heroic.metric;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.test.FakeModuleLoader;
import com.spotify.heroic.test.Resources;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class BasicSerializationTest {
    private ObjectMapper mapper = FakeModuleLoader.builder().build().json();

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
    public void testMetricCollection2() throws Exception {
        final MetricCollection expected = MetricCollection.events(
            ImmutableList.of(new Event(1000, ImmutableMap.of("foo", "bar")),
                new Event(2000, ImmutableMap.of("bar", "baz"))));
        assertSerialization("MetricCollection.Event.json", expected, MetricCollection.class);
    }

    @Test
    public void testMetricCollection3() throws Exception {
        final byte[] foo = "foo\n".getBytes(StandardCharsets.UTF_8);
        final byte[] bar = "bar\n".getBytes(StandardCharsets.UTF_8);

        final MetricCollection expected = MetricCollection.cardinality(
            ImmutableList.of(new Payload(1000, foo), new Payload(2000, bar)));
        assertSerialization("MetricCollection.Payload.json", expected, MetricCollection.class);
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
                Statistics.empty(), ResultLimits.of(), Optional.empty());

        assertSerialization("FullQuery.json", expected, FullQuery.class);
    }

    @Test
    public void testQueryMetricsResponse() throws Exception {
        final UUID queryId = UUID.fromString("d11d0ad7-cc27-4667-a617-67a481f61c30");
        final DateRange range = DateRange.create(1484027520000L, 1484038320000L);

        final Set<Series> series = ImmutableSet.of();
        final MetricCollection metrics = MetricCollection.points(new ArrayList<>());
        final List<ShardedResultGroup> result = ImmutableList.of(
            new ShardedResultGroup(ImmutableMap.of(), ImmutableMap.of(), series, metrics, 0L));

        final List<RequestError> errors = new ArrayList<>();
        final QueryTrace trace = QueryTrace.of(QueryTrace.identifier("test"), 0L);
        final ResultLimits limits = ResultLimits.of();

        final QueryMetricsResponse toVerify =
            new QueryMetricsResponse(queryId, range, result, errors, trace, limits,
                Optional.empty(), Optional.empty());

        assertSerialization("QueryMetricsResponse.json", toVerify);
    }

    private <T> void assertSerialization(
        final String expectedFile, final T toVerify
    ) throws IOException {
        // verify that it is equal to the local file.
        try (final InputStream in = Resources.openResource(getClass(), expectedFile)) {
            final JsonNode expectedNode = mapper.readTree(in);
            final JsonNode toVerifyNode = mapper.valueToTree(toVerify);
            // Serialize both, to avoid false negative when comparing above two, in the case of
            // one tree using IntNode and the other LongNode for what should be equal trees.
            final String expectedString = mapper.writeValueAsString(expectedNode);
            final String toVerifyString = mapper.writeValueAsString(toVerifyNode);
            assertEquals(expectedString, toVerifyString);
        }
    }

    private <T> void assertSerialization(
        final String expectedFile, final T toVerify, final Class<T> type
    ) throws IOException {
        // verify that it is equal to the local file.
        try (final InputStream in = Resources.openResource(getClass(), expectedFile)) {
            assertEquals(toVerify, mapper.readValue(in, type));
        }

        // roundtrip
        final String string = mapper.writeValueAsString(toVerify);
        assertEquals(toVerify, mapper.readValue(string, type));
    }
}
