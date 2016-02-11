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
        registry.register("foo", A.class, AI.class, aiSerializer, dsl);

        assertEquals("foo", registry.definitionMap.get(A.class));
        assertEquals("foo", registry.instanceMap.get(AI.class));
        assertEquals(aiSerializer, registry.serializerMap.get("foo"));
        assertEquals(dsl, registry.builderMap.get("foo"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSameId() {
        registry.register("foo", A.class, AI.class, aiSerializer, dsl);
        registry.register("foo", B.class, BI.class, biSerializer, dsl);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSameAggregation() {
        registry.register("foo", A.class, AI.class, aiSerializer, dsl);
        registry.register("bar", A.class, BI.class, biSerializer, dsl);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSameInstance() {
        registry.register("foo", A.class, AI.class, aiSerializer, dsl);
        registry.register("bar", B.class, AI.class, aiSerializer, dsl);
    }

    static interface A extends Aggregation {
    }

    static interface AI extends AggregationInstance {
    }

    static interface B extends Aggregation {
    }

    static interface BI extends AggregationInstance {
    }
}
