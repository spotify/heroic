package com.spotify.heroic.rpc.nativerpc;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import org.junit.Test;

import eu.toolchain.async.AsyncFuture;

public class NativeRpcContainerTest {
    @Test
    public void testTypeReference() {
        final NativeRpcContainer.Endpoint<Map<String, Integer>, String> endpoint = new NativeRpcContainer.Endpoint<Map<String, Integer>, String>() {
            @Override
            public AsyncFuture<String> handle(Map<String, Integer> request) throws Exception {
                return null;
            }
        };

        final Type type = endpoint.requestType().getType();
        assertTrue(type instanceof ParameterizedType);

        final ParameterizedType t = (ParameterizedType) type;
        assertTrue(t.getRawType() == Map.class);
        assertTrue(t.getActualTypeArguments().length == 2);
        assertTrue(t.getActualTypeArguments()[0] == String.class);
        assertTrue(t.getActualTypeArguments()[1] == Integer.class);
    }
}