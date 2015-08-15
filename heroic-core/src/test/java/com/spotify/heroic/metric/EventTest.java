package com.spotify.heroic.metric;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.EventSerialization;

public class EventTest {
    private static final Event ref = new Event(1021L, ImmutableMap.<String, Object> of("int", 1234, "float", 0.1d,
            "string", "foo"));

    @Test
    public void testSerialization() throws IOException {
        final ObjectMapper mapper = new ObjectMapper();

        final SimpleModule m = new SimpleModule();

        m.addSerializer(Event.class, new EventSerialization.Serializer());
        m.addDeserializer(Event.class, new EventSerialization.Deserializer());

        mapper.registerModule(m);

        final String content = mapper.writeValueAsString(ref);
        final Event result = mapper.readValue(content, Event.class);
        Assert.assertEquals(ref, result);
    }
}