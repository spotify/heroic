package com.spotify.heroic.aggregation;

import eu.toolchain.serializer.Serializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CoreAggregationRegistryTest {
    @Mock
    Serializer<String> string;

    @Mock
    Serializer<AI> aiSerializer;

    @Mock
    Serializer<BI> biSerializer;

    @Mock
    AggregationDSL dsl;

    CoreAggregationRegistry registry;

    @Before
    public void setup() {
        registry = new CoreAggregationRegistry(string);
    }

    @Test
    public void testRegisterQuery() {
        registry.register("foo", A.class, AI.class, dsl);

        assertEquals("foo", registry.definitionMap.get(A.class));
        assertEquals("foo", registry.instanceMap.get(AI.class));
        assertEquals(dsl, registry.builderMap.get("foo"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSameId() {
        registry.register("foo", A.class, AI.class, dsl);
        registry.register("foo", B.class, BI.class, dsl);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSameAggregation() {
        registry.register("foo", A.class, AI.class, dsl);
        registry.register("bar", A.class, BI.class, dsl);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSameInstance() {
        registry.register("foo", A.class, AI.class, dsl);
        registry.register("bar", B.class, AI.class, dsl);
    }

    interface A extends Aggregation {
    }

    interface AI extends AggregationInstance {
    }

    interface B extends Aggregation {
    }

    interface BI extends AggregationInstance {
    }
}
