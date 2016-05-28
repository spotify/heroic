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

package com.spotify.heroic.rpc.grpc;

import eu.toolchain.async.AsyncFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class GrpcRpcContainer {
    private final List<GrpcEndpointHandle<?, ?>> endpoints = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public void register(final GrpcRpcEndpointHandleBase<?, ?> handle) {
        endpoints.add(handle);
    }

    public <Q, R> void register(
        final GrpcDescriptor<Q, R> spec, Function<Q, AsyncFuture<R>> handle
    ) {
        register(new GrpcRpcEndpointHandleBase<Q, R>(spec) {
            @Override
            public AsyncFuture<R> handle(final Q request) throws Exception {
                return handle.apply(request);
            }
        });
    }

    public List<GrpcEndpointHandle<?, ?>> getEndpoints() {
        return endpoints;
    }
}
