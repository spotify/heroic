package com.spotify.heroic.metric.model;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DataPointSerializer;

public class ResultGroupTest {

    private ObjectMapper mapper;

    @Before
    public void before() {
        mapper = new ObjectMapper();
        final SimpleModule module = new SimpleModule();
        module.addSerializer(DataPoint.class, new DataPointSerializer.Serializer());
        module.addDeserializer(DataPoint.class, new DataPointSerializer.Deserializer());
        mapper.registerModule(module);
    }

    /**
     * This is to verify that serialization and deserialization of {@link ResultGroup} work
     * @throws IOException
     */
    @Test
    public void testResultGroupSerializer() throws IOException {
        final List<TagValues> tags = ImmutableList.of();
        final List<DataPoint> values = ImmutableList.of(new DataPoint(System.currentTimeMillis(), 123));
        final ResultGroup group = new ResultGroup.DataPointResultGroup(tags, values);

        final String json = mapper.writeValueAsString(group);
        final ResultGroup group2 = mapper.readValue(json, ResultGroup.class);

        Assert.assertEquals(group, group2);
    }
}
