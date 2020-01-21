/*
 * Copyright (c) 2020 Spotify AB.
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

import static io.opencensus.trace.AttributeValue.stringAttributeValue;

import com.spotify.heroic.tracing.EnvironmentMetadata;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

/**
 * A {@link ServerInterceptor} that adds metadata to gRPC spans. Since gRPC is instrumented with
 * OpenCensus, the spans will already exist for all requests.
 */
public class GrpcOpenCensusInterceptor implements ServerInterceptor {
    private final Tracer tracer = Tracing.getTracer();

    private static final EnvironmentMetadata environmentMetadata = EnvironmentMetadata.create();
    public GrpcOpenCensusInterceptor() { }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        final ServerCall<ReqT, RespT> call, final Metadata headers,
        final ServerCallHandler<ReqT, RespT> next) {

        final Span span = tracer.getCurrentSpan();
        span.putAttributes(environmentMetadata.toAttributes());
        span.putAttribute("protocol", stringAttributeValue("grpc"));
        span.putAttribute("span.kind", stringAttributeValue("server"));

        return next.startCall(call, headers);
    }
}
