package com.spotify.heroic.metric;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicCore;

public class ResultGroupTest {
    private ObjectMapper mapper;

    @Before
    public void before() {
        mapper = new ObjectMapper();
        mapper.registerModule(HeroicCore.serializerModule());
    }

    /**
     * This is to verify that serialization and deserialization of {@link ResultGroup} work
     * @throws IOException
     */
    @Test
    public void testResultGroupSerializer() throws IOException {
        final List<TagValues> tags = ImmutableList.of();
        final List<Point> values = ImmutableList.of(new Point(System.currentTimeMillis(), 123));
        final ResultGroup group = new ResultGroup(tags, new MetricTypedGroup(MetricType.EVENT, values));

        final String json = mapper.writeValueAsString(group);
        final ResultGroup group2 = mapper.readValue(json, ResultGroup.class);

        Assert.assertEquals(group, group2);
    }
}
