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

package com.spotify.heroic.httpclient;

import java.net.URI;
import java.util.concurrent.ExecutorService;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;
import lombok.ToString;

import org.glassfish.jersey.client.ClientConfig;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Data
@ToString(exclude = { "uri", "base" })
public class HttpClientSessionImpl implements HttpClientSession {
    private final AsyncFramework async;
    private final ClientConfig config;
    private final ExecutorService executor;
    private final URI uri;
    private final String base;

    public <R, T> AsyncFuture<T> post(R request, Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return async.call(new HttpPostRequestResolver<R, T>(request, clazz, target), executor);
    }

    public <T> AsyncFuture<T> get(Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return async.call(new HttpGetRequestResolver<T>(clazz, target), executor);
    }
}
