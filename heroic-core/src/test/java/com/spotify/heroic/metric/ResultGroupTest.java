package com.spotify.heroic.metric;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.PointSerialization;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.ResultGroup;
import com.spotify.heroic.metric.TagValues;

public class ResultGroupTest {

    private ObjectMapper mapper;

    @Before
    public void before() {
        mapper = new ObjectMapper();
        final SimpleModule module = new SimpleModule();
        module.addSerializer(Point.class, new PointSerialization.Serializer());
        module.addDeserializer(Point.class, new PointSerialization.Deserializer());
        mapper.registerModule(module);
    }

    /**
     * This is to verify that serialization and deserialization of {@link ResultGroup} work
     * @throws IOException
     */
    @Test
    public void testResultGroupSerializer() throws IOException {
        final List<TagValues> tags = ImmutableList.of();
        final List<Point> values = ImmutableList.of(new Point(System.currentTimeMillis(), 123));
        final ResultGroup group = new ResultGroup.DataPointResultGroup(tags, values);

        final String json = mapper.writeValueAsString(group);
        final ResultGroup group2 = mapper.readValue(json, ResultGroup.class);

        Assert.assertEquals(group, group2);
    }
}
