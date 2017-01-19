package com.spotify.heroic.aggregation.simple;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.test.FakeModuleLoader;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FilterAggregationTest {
    private ObjectMapper mapper;

    @Before
    public void setup() {
        mapper = FakeModuleLoader.builder().module(Module.class).build().json();
    }

    @Test
    public void testAboveKInstance() throws Exception {
        // TODO: support @JsonCreator
        // verifyClassBuilder(AboveKInstance.class).checkGetters(false).verify();
        verifyRoundtrip("{\"type\":\"abovek\",\"k\":0.0}", new AboveKInstance(0),
            AboveKInstance.class);
    }

    @Test
    public void testBelowKInstance() throws Exception {
        // TODO: support @JsonCreator
        // verifyClassBuilder(BelowKInstance.class).checkGetters(false).verify();
        verifyRoundtrip("{\"type\":\"belowk\",\"k\":0.0}", new BelowKInstance(0),
            BelowKInstance.class);
    }

    @Test
    public void testBottomKInstance() throws Exception {
        // TODO: support @JsonCreator
        // verifyClassBuilder(BottomKInstance.class).checkGetters(false).verify();
        verifyRoundtrip("{\"type\":\"bottomk\",\"k\":0}", new BottomKInstance(0),
            BottomKInstance.class);
    }

    @Test
    public void testTopKInstance() throws Exception {
        // TODO: support @JsonCreator
        // verifyClassBuilder(TopKInstance.class).checkGetters(false).verify();
        verifyRoundtrip("{\"type\":\"topk\",\"k\":0}", new TopKInstance(0), TopKInstance.class);
    }

    private <T> void verifyRoundtrip(
        final String json, final T reference, final Class<T> cls
    ) throws Exception {
        assertEquals(reference, mapper.readValue(json, cls));
        assertEquals(json, mapper.writeValueAsString(reference));
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    public interface TypeNameMixin {
    }
}
