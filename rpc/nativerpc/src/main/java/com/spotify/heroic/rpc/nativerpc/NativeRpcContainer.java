package com.spotify.heroic.rpc.nativerpc;

import java.util.HashMap;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.type.TypeReference;

import eu.toolchain.async.AsyncFuture;

public class NativeRpcContainer {
    private final Map<String, EndpointSpec<Object, Object>> endpoints = new HashMap<>();

    @SuppressWarnings("unchecked")
    public void register(final String endpoint, final EndpointSpec<?, ?> handle) {
        if (endpoints.containsKey(endpoint))
            throw new IllegalStateException("Endpoint already registered: " + endpoint);

        endpoints.put(endpoint, (EndpointSpec<Object, Object>) handle);
    }

    public EndpointSpec<Object, Object> get(String endpoint) {
        return endpoints.get(endpoint);
    }

    public static interface EndpointSpec<Q, R> {
        public AsyncFuture<R> handle(final Q request) throws Exception;

        public TypeReference<Q> requestType();
    }

    @RequiredArgsConstructor
    public static abstract class Endpoint<Q, R> implements EndpointSpec<Q, R> {
        private final TypeReference<Q> type;

        public TypeReference<Q> requestType() {
            return type;
        }
    }
}