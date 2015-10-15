package com.spotify.heroic.metric;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.HeroicPrimaryModule;
import com.spotify.heroic.common.Statistics;

public class BasicSerializationTest {
    private ObjectMapper mapper;

    @Before
    public void setup() {
        mapper = new ObjectMapper();
        mapper.registerModule(HeroicPrimaryModule.serializerModule());
    }

    @Test
    public void testEvent() throws Exception {
        final Event expected = new Event(1024L, ImmutableMap.<String, Object> of("int", 1234, "float", 0.25d,
                "string", "foo"));
        assertSerialization("Event.json", expected, Event.class);
    }

    @Test
    public void testPoint() throws Exception {
        final Point expected = new Point(1024L, 3.14);
        assertSerialization("Point.json", expected, Point.class);
    }

    @Test
    public void testMetricTypedGroup() throws Exception {
        final MetricCollection expected = MetricCollection.points(new ArrayList<>());
        assertSerialization("MetricTypedGroup.json", expected, MetricCollection.class);
    }

    @Test
    public void testResultGroup() throws Exception {
        final List<TagValues> tags = new ArrayList<>();
        final ResultGroup expected = new ResultGroup(tags, MetricCollection.points(new ArrayList<>()), 0l);
        assertSerialization("ResultGroup.json", expected, ResultGroup.class);
    }

    @Test
    public void testResultGroups() throws Exception {
        final List<ResultGroup> groups = new ArrayList<>();
        final List<RequestError> errors = new ArrayList<>();
        final ResultGroups expected = new ResultGroups(groups, errors, Statistics.empty(), new QueryTrace(QueryTrace.identifier("test")));

        assertSerialization("ResultGroups.json", expected, ResultGroups.class);
    }

    private <T> void assertSerialization(final String json, final T expected, final Class<T> type) throws IOException, JsonParseException, JsonMappingException {
        // verify that it is equal to the local file.
        try (InputStream in = openResource(json)) {
            assertEquals(expected, mapper.readValue(in, type));
        }

        // roundtrip
        final String string = mapper.writeValueAsString(expected);
        assertEquals(expected, mapper.readValue(string, type));
    }

    private InputStream openResource(String path) {
        final Class<?> cls = BasicSerializationTest.class;
        final String fullPath = cls.getPackage().getName().replace('.', '/') + "/" + path;
        return cls.getClassLoader().getResourceAsStream(fullPath);
    }
}