/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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