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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.metrics.Meter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ResolvableFuture;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import java.io.IOException;
import java.net.InetSocketAddress;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class GrpcRpcClient {
    private final AsyncFramework async;
    private final InetSocketAddress address;
    private final ObjectMapper mapper;
    private final Managed<ManagedChannel> channel;
    private final Meter errors = new Meter();

    private static final GrpcRpcEmptyBody EMPTY = new GrpcRpcEmptyBody();

    public AsyncFuture<Void> close() {
        return channel.stop();
    }

    public <R> AsyncFuture<R> request(
        final GrpcDescriptor<GrpcRpcEmptyBody, R> endpoint, final CallOptions options
    ) {
        return request(endpoint, EMPTY, options);
    }

    public <Q, R> AsyncFuture<R> request(
        final GrpcDescriptor<Q, R> endpoint, final Q entity, final CallOptions options
    ) {
        return channel.doto(channel -> {
            final byte[] body;

            try {
                body = mapper.writeValueAsBytes(entity);
            } catch (JsonProcessingException e) {
                return async.failed(e);
            }

            final ClientCall<byte[], byte[]> call = channel.newCall(endpoint.descriptor(), options);

            final Metadata metadata = new Metadata();

            final ResolvableFuture<R> future = async.future();

            call.start(new ClientCall.Listener<byte[]>() {
                @Override
                public void onMessage(final byte[] message) {
                    final R response;

                    try {
                        response = mapper.readValue(message, endpoint.responseType());
                    } catch (IOException e) {
                        future.fail(e);
                        return;
                    }

                    future.resolve(response);
                }

                @Override
                public void onClose(final Status status, final Metadata trailers) {
                    if (status.isOk()) {
                        if (!future.isDone()) {
                            future.fail(new RuntimeException("Request finished without response"));
                        }
                    } else {
                        future.fail(new RuntimeException(
                            "Request finished with status code (" + status + ")"));
                    }
                }

                @Override
                public void onHeaders(final Metadata headers) {
                }

                @Override
                public void onReady() {
                }
            }, metadata);

            call.sendMessage(body);
            call.setMessageCompression(true);
            call.request(1);
            call.halfClose();

            return future.onFailed(e -> errors.mark());
        });
    }

    public boolean isAlive() {
        return errors.getFiveMinuteRate() < 1.0D;
    }

    @Override
    public String toString() {
        return getUri();
    }

    public String getUri() {
        return "grpc://" + address.getHostString() +
            (address.getPort() != -1 ? ":" + address.getPort() : "");
    }
}
