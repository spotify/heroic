package com.spotify.heroic.metric;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;

public class MetricGroupTest {
    ObjectMapper mapper;

    @Before
    public void setup() {
        mapper = new ObjectMapper();

        final SimpleModule m = new SimpleModule();

        m.addSerializer(Event.class, new EventSerialization.Serializer());
        m.addDeserializer(Event.class, new EventSerialization.Deserializer());

        m.addSerializer(Point.class, new PointSerialization.Serializer());
        m.addDeserializer(Point.class, new PointSerialization.Deserializer());

        m.addSerializer(Point.class, new PointSerialization.Serializer());
        m.addDeserializer(Point.class, new PointSerialization.Deserializer());

        m.addSerializer(MetricType.class, new MetricTypeSerialization.Serializer());
        m.addDeserializer(MetricType.class, new MetricTypeSerialization.Deserializer());

        m.addSerializer(MetricTypedGroup.class, new MetricTypedGroupSerialization.Serializer());
        m.addDeserializer(MetricTypedGroup.class, new MetricTypedGroupSerialization.Deserializer());

        m.addSerializer(MetricGroup.class, new MetricGroupSerialization.Serializer());
        m.addDeserializer(MetricGroup.class, new MetricGroupSerialization.Deserializer());

        mapper.registerModule(m);
    }

    @Test
    public void testSerialization() throws IOException {
        final MetricGroup ref = new MetricGroup(1000, ImmutableList.of(new MetricTypedGroup(MetricType.POINT,
                ImmutableList.of(new Point(2000, 10.0d)))));

        final String content = mapper.writeValueAsString(ref);
        final MetricGroup result = mapper.readValue(content, MetricGroup.class);
        Assert.assertEquals(ref, result);
    }
}