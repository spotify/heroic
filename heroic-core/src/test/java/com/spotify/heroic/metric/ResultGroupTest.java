package com.spotify.heroic.metric;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.HeroicPrimaryModule;

public class ResultGroupTest {
    private ObjectMapper mapper;

    @Before
    public void before() {
        mapper = new ObjectMapper();
        mapper.registerModule(HeroicPrimaryModule.serializerModule());
    }

    /**
     * This is to verify that serialization and deserialization of {@link ResultGroup} work
     * @throws IOException
     */
    @Test
    public void testResultGroupSerializer() throws IOException {
        final List<TagValues> tags = ImmutableList.of();
        final List<Event> values = ImmutableList.of(new Event(1000, ImmutableMap.<String, Object> of()));
        final ResultGroup group = new ResultGroup(tags, new MetricTypedGroup(MetricType.EVENT, values));

        final String json = mapper.writeValueAsString(group);
        final ResultGroup group2 = mapper.readValue(json, ResultGroup.class);

        Assert.assertEquals(group, group2);
    }
}
