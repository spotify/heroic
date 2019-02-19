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

import com.fasterxml.jackson.core.type.TypeReference;
import io.grpc.MethodDescriptor;

public class GrpcRpcEndpointSpec<Q, R> implements GrpcDescriptor<Q, R> {
    private final TypeReference<Q> queryType;
    private final TypeReference<R> responseType;
    private final MethodDescriptor<byte[], byte[]> descriptor;

    @java.beans.ConstructorProperties({ "queryType", "responseType", "descriptor" })
    public GrpcRpcEndpointSpec(final TypeReference<Q> queryType,
                               final TypeReference<R> responseType,
                               final MethodDescriptor<byte[], byte[]> descriptor) {
        this.queryType = queryType;
        this.responseType = responseType;
        this.descriptor = descriptor;
    }

    @Override
    public TypeReference<Q> queryType() {
        return queryType;
    }

    @Override
    public TypeReference<R> responseType() {
        return responseType;
    }

    @Override
    public MethodDescriptor<byte[], byte[]> descriptor() {
        return descriptor;
    }
}
