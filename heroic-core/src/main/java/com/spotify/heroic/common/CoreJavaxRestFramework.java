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

package com.spotify.heroic.common;

import com.spotify.heroic.ws.InternalErrorMessage;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.ConnectionCallback;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;

public final class CoreJavaxRestFramework implements JavaxRestFramework {
    private static final Logger log =
        org.slf4j.LoggerFactory.getLogger(CoreJavaxRestFramework.class);

    private static final Resume<?, ?> PASSTHROUGH =
        (Resume<Object, Object>) value -> value;

    @Override
    public <T> void bind(final AsyncResponse response, final AsyncFuture<T> callback) {
        this.bind(response, callback, this.passthrough());
    }

    @Override
    public <T> void bind(final AsyncResponse response, final CompletionStage<T> callback) {
        this.bind(response, callback, this.passthrough());
    }

    /**
     * Helper function to correctly wire up async response management.
     *
     * @param response The async response object.
     * @param callback Callback for the pending request.
     * @param resume The resume implementation.
     */
    @Override
    public <T, R> void bind(
        final AsyncResponse response, final AsyncFuture<T> callback, final Resume<T, R> resume
    ) {
        callback.onDone(new FutureDone<>() {
            @Override
            public void failed(Throwable e) {
                log.error("Request failed", e);
                response.resume(Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new InternalErrorMessage(e.getMessage(),
                        Response.Status.INTERNAL_SERVER_ERROR))
                    .build());
            }

            @Override
            public void resolved(T result) throws Exception {
                if (response.isDone()) {
                    return;
                }

                response.resume(
                    Response.status(Response.Status.OK).entity(resume.resume(result)).build());
            }

            @Override
            public void cancelled() {
                log.error("Request cancelled");
                response.resume(Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new InternalErrorMessage("request cancelled",
                        Response.Status.INTERNAL_SERVER_ERROR))
                    .build());
            }
        });

        response.setTimeoutHandler(asyncResponse -> {
            log.debug("client timed out");
            callback.cancel();
        });

        response.register((CompletionCallback) throwable -> {
            log.debug("client completed");
            callback.cancel();
        });

        response.register((ConnectionCallback) disconnected -> {
            log.debug("client disconnected");
            callback.cancel();
        });
    }

    /**
     * Helper function to correctly wire up async response management.
     *
     * @param response The async response object.
     * @param callback Callback for the pending request.
     * @param resume The resume implementation.
     */
    @Override
    public <T, R> void bind(
        final AsyncResponse response, final CompletionStage<T> callback, final Resume<T, R> resume
    ) {
        CompletableFuture<Boolean> future = callback
            .toCompletableFuture()
            .thenApply(result -> {
                if (response.isDone()) {
                    return null;
                }

                try {
                    return response.resume(
                        Response.status(Response.Status.OK).entity(resume.resume(result)).build());
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            })
            .exceptionally(ex -> {
                final String message;
                if (ex instanceof CancellationException) {
                    log.error("Request cancelled");
                    message = "request cancelled";
                } else if (ex instanceof CompletionException) {
                    log.error("Request failed", ex.getCause());
                    message = ex.getCause().getMessage();
                } else {
                    log.error("Request failed", ex);
                    message = ex.getMessage();
                }

                response.resume(Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new InternalErrorMessage(message,
                        Response.Status.INTERNAL_SERVER_ERROR))
                    .build());
                return null;
            });

        response.setTimeoutHandler(asyncResponse -> {
            log.debug("client timed out");
            future.cancel(true);
        });

        response.register((CompletionCallback) throwable -> {
            log.debug("client completed");
            future.cancel(true);
        });

        response.register((ConnectionCallback) disconnected -> {
            log.debug("client disconnected");
            future.cancel(true);
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Resume<T, T> passthrough() {
        return (Resume<T, T>) PASSTHROUGH;
    }
}
