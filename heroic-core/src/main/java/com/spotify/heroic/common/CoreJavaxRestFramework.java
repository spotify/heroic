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

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.ConnectionCallback;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.Response;

import com.spotify.heroic.http.InternalErrorMessage;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class CoreJavaxRestFramework implements JavaxRestFramework {
    @Override
    public <T> void bind(final AsyncResponse response, final AsyncFuture<T> callback) {
        this.<T, T> bind(response, callback, this.<T> passthrough());
    }

    /**
     * Helper function to correctly wire up async response management.
     *
     * @param response The async response object.
     * @param callback Callback for the pending request.
     * @param resume The resume implementation.
     */
    @Override
    public <T, R> void bind(final AsyncResponse response, final AsyncFuture<T> callback,
            final Resume<T, R> resume) {
        callback.onDone(new FutureDone<T>() {
            @Override
            public void failed(Throwable e) throws Exception {
                log.error("Request failed", e);
                response.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
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
            public void cancelled() throws Exception {
                log.error("Request cancelled");
                response.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new InternalErrorMessage("request cancelled",
                                Response.Status.INTERNAL_SERVER_ERROR))
                        .build());
            }
        });

        doBind(response, callback);
    }

    void doBind(final AsyncResponse response, final AsyncFuture<?> callback) {
        response.setTimeoutHandler(new TimeoutHandler() {
            @Override
            public void handleTimeout(AsyncResponse asyncResponse) {
                log.warn("Client timed out");
                callback.cancel();
            }
        });

        response.register(new CompletionCallback() {
            @Override
            public void onComplete(Throwable throwable) {
                log.warn("Client completed");
                callback.cancel();
            }
        });

        response.register(new ConnectionCallback() {
            @Override
            public void onDisconnect(AsyncResponse disconnected) {
                log.warn("Client disconnected");
                callback.cancel();
            }
        });
    }

    static final Resume<? extends Object, ? extends Object> PASSTHROUGH =
            new Resume<Object, Object>() {
                @Override
                public Object resume(Object value) throws Exception {
                    return value;
                }
            };

    @Override
    @SuppressWarnings("unchecked")
    public <T> Resume<T, T> passthrough() {
        return (Resume<T, T>) PASSTHROUGH;
    }
}
