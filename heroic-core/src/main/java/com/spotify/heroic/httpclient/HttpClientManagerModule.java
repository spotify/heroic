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

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

import lombok.Data;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.concurrrency.ThreadPool;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.HttpClientManagerReporter;

import eu.toolchain.async.AsyncFramework;

@Data
public class HttpClientManagerModule extends PrivateModule {
    // 100 requests allowed to be pending.
    // these will be mostly blocking for a response.
    public static final int DEFAULT_THREADS = 100;
    // + 100 requests are allowed to be pending in queue.
    public static final int DEFAULT_QUEUE_SIZE = 100;
    // allow 2 seconds to connect
    public static final int DEFAULT_CONNECT_TIMEOUT = 2000;
    // allow each request to take at most 2 minutes.
    public static final int DEFAULT_READ_TIMEOUT = 120000;

    private final int threads;
    private final int queueSize;
    private final int connectTimeout;
    private final int readTimeout;

    @JsonCreator
    public HttpClientManagerModule(@JsonProperty("threads") Integer threads,
            @JsonProperty("queueSize") Integer queueSize, @JsonProperty("connectTimeout") Integer connectTimeout,
            @JsonProperty("readTimeout") Integer readTimeout) {
        this.threads = Optional.fromNullable(threads).or(DEFAULT_THREADS);
        this.queueSize = Optional.fromNullable(queueSize).or(DEFAULT_QUEUE_SIZE);
        this.connectTimeout = Optional.fromNullable(connectTimeout).or(DEFAULT_CONNECT_TIMEOUT);
        this.readTimeout = Optional.fromNullable(readTimeout).or(DEFAULT_READ_TIMEOUT);
    }

    /**
     * Create a default instance.
     */
    public static Supplier<HttpClientManagerModule> defaultSupplier() {
        return new Supplier<HttpClientManagerModule>() {
            @Override
            public HttpClientManagerModule get() {
                return new HttpClientManagerModule(null, null, null, null);
            }
        };
    }

    @Provides
    @Singleton
    public ClientConfig config(@Named("application/json+internal") ObjectMapper mapper) {
        final ClientConfig config = new ClientConfig();

        config.register(JsonParseExceptionMapper.class);
        config.register(JsonMappingExceptionMapper.class);
        config.register(new JacksonJsonProvider(mapper), MessageBodyReader.class, MessageBodyWriter.class);

        config.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
        config.property(ClientProperties.READ_TIMEOUT, readTimeout);

        return config;
    }

    @Inject
    @Provides
    @Singleton
    public HttpClientManagerReporter reporter(HeroicReporter reporter) {
        return reporter.newHttpClientManager();
    }

    @Provides
    @Singleton
    public ThreadPool executor(AsyncFramework async, HttpClientManagerReporter reporter) {
        return ThreadPool.create(async, "request", reporter.newThreadPool(), threads, queueSize);
    }

    @Override
    protected void configure() {
        bind(HttpClientManager.class).to(HttpClientManagerImpl.class).in(Scopes.SINGLETON);
        expose(HttpClientManager.class);
    }
}
