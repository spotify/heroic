package com.spotify.heroic.aggregation.simple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.TDigestBucket;
import com.spotify.heroic.test.FakeModuleLoader;
import java.util.Optional;
import org.junit.Test;

public class AggregationSerializationTest {
    private final FakeModuleLoader m = FakeModuleLoader.builder().module(Module.class).build();
    private final ObjectMapper mapper = m.json();

    private void assertSerializes(String json, AggregationInstance aggregation) throws Exception {
        assertEquals(json, mapper.writeValueAsString(aggregation));
        assertEquals(aggregation, mapper.readValue(json, AggregationInstance.class));
    }

    @Test
    public void testTDigestInstance() throws Exception {
        final double [] quantiles = {0.5,0.75,0.99};
        final String json = "{\"type\":\"tdigeststat\",\"size\":1,\"extent\":2,\"quantiles\":[0.5,0.75,0.99]}";
        AggregationInstance aggregationInstance = new TdigestStatInstance(1, 2, quantiles );
        assertEquals(json, mapper.writeValueAsString(aggregationInstance));
        assertEquals(aggregationInstance.toString(), mapper.readValue(json, TdigestStatInstance.class).toString());
    }

    @Test
    public void testAboveK() throws Exception {
        assertEquals(new AboveK(1.5, Optional.empty()),
            mapper.readValue(m.jsonObject().put("type", AboveK.NAME).put("k", 1.5).string(),
                Aggregation.class));

        assertEquals(new AboveKInstance(1.5),
            mapper.readValue(m.jsonObject().put("type", AboveK.NAME).put("k", 1.5).string(),
                AggregationInstance.class));

        final String expected = "{\"type\":\"abovek\",\"k\":1.5}";
        assertEquals(expected, mapper.writeValueAsString(new AboveK(1.5, Optional.empty())));
        assertEquals(expected, mapper.writeValueAsString(new AboveKInstance(1.5)));
    }

    @Test
    public void testBelowK() throws Exception {
        assertEquals(new BelowK(1.5, Optional.empty()),
            mapper.readValue(m.jsonObject().put("type", BelowK.NAME).put("k", 1.5).string(),
                Aggregation.class));

        assertEquals(new BelowKInstance(1.5),
            mapper.readValue(m.jsonObject().put("type", BelowK.NAME).put("k", 1.5).string(),
                AggregationInstance.class));

        final String expected = "{\"type\":\"belowk\",\"k\":1.5}";
        assertEquals(expected, mapper.writeValueAsString(new BelowK(1.5, Optional.empty())));
        assertEquals(expected, mapper.writeValueAsString(new BelowKInstance(1.5)));
    }

    @Test
    public void testBottomK() throws Exception {
        assertEquals(new BottomK(1, Optional.empty()),
            mapper.readValue(m.jsonObject().put("type", BottomK.NAME).put("k", 1).string(),
                Aggregation.class));

        assertEquals(new BottomKInstance(1),
            mapper.readValue(m.jsonObject().put("type", BottomK.NAME).put("k", 1).string(),
                AggregationInstance.class));

        final String expected = "{\"type\":\"bottomk\",\"k\":1}";
        assertEquals(expected, mapper.writeValueAsString(new BottomK(1, Optional.empty())));
        assertEquals(expected, mapper.writeValueAsString(new BottomKInstance(1)));
    }

    @Test
    public void testTopK() throws Exception {
        assertEquals(new TopK(1, Optional.empty()),
            mapper.readValue(m.jsonObject().put("type", TopK.NAME).put("k", 1).string(),
                Aggregation.class));

        assertEquals(new TopKInstance(1),
            mapper.readValue(m.jsonObject().put("type", TopK.NAME).put("k", 1).string(),
                AggregationInstance.class));

        final String expected = "{\"type\":\"topk\",\"k\":1}";
        assertEquals(expected, mapper.writeValueAsString(new TopK(1, Optional.empty())));
        assertEquals(expected, mapper.writeValueAsString(new TopKInstance(1)));
    }

    @Test
    public void testMaxInstance() throws Exception {
        final String expected = "{\"type\":\"max\",\"size\":1,\"extent\":2}";
        assertSerializes(expected, new MaxInstance(1, 2));
    }

    @Test
    public void testMinInstance() throws Exception {
        final String expected = "{\"type\":\"min\",\"size\":1,\"extent\":2}";
        assertSerializes(expected, new MinInstance(1, 2));
    }

    @Test
    public void testCountInstance() throws Exception {
        final String expected = "{\"type\":\"count\",\"size\":1,\"extent\":2}";
        assertSerializes(expected, new CountInstance(1, 2));
    }

    @Test
    public void testSumInstance() throws Exception {
        final String expected = "{\"type\":\"sum\",\"size\":1,\"extent\":2}";
        assertSerializes(expected, new SumInstance(1, 2));
    }

    @Test
    public void testAverageInstance() throws Exception {
        final String expected = "{\"type\":\"average\",\"size\":1,\"extent\":2}";
        assertSerializes(expected, new AverageInstance(1, 2));
    }

    @Test
    public void testSpreadInstance() throws Exception {
        final String expected = "{\"type\":\"spread\",\"size\":1,\"extent\":2}";
        assertSerializes(expected, new SpreadInstance(1, 2));
    }

    @Test
    public void testQuantileInstance() throws Exception {
        final String expected = "{\"type\":\"quantile\",\"size\":1,\"extent\":2,\"q\":3.0,\"error\":4.0}";
        assertSerializes(expected, new QuantileInstance(1, 2, 3.0, 4.0));
    }

    @Test
    public void testStdDevInstance() throws Exception {
        final String expected = "{\"type\":\"stddev\",\"size\":1,\"extent\":2}";
        assertSerializes(expected, new StdDevInstance(1, 2));
    }

    @Test
    public void testGroupUniqueInstance() throws Exception {
        final String expected = "{\"type\":\"group-unique\",\"size\":1,\"extent\":2}";
        assertSerializes(expected, new GroupUniqueInstance(1, 2));
    }
}
