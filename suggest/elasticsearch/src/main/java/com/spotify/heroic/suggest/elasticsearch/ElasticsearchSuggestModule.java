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

package com.spotify.heroic.suggest.elasticsearch;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.ofNullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.BackendTypeFactory;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.DefaultRateLimitedCache;
import com.spotify.heroic.elasticsearch.DisabledRateLimitedCache;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestModule;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import lombok.Data;

@Data
public final class ElasticsearchSuggestModule implements SuggestModule {
    public static final String ELASTICSEARCH_CONFIGURE_PARAM = "elasticsearch.configure";

    private static final double DEFAULT_WRITES_PER_SECOND = 3000d;
    private static final long DEFAULT_WRITES_CACHE_DURATION_MINUTES = 240L;
    public static final String DEFAULT_GROUP = "elasticsearch";
    public static final String DEFAULT_TEMPLATE_NAME = "heroic-suggest";
    public static final String DEFAULT_BACKEND_TYPE = "default";

    private final String id;
    private final Groups groups;
    private final ManagedConnectionFactory connection;
    private final double writesPerSecond;
    private final long writeCacheDurationMinutes;
    private final String templateName;
    private final String backendType;

    private static BackendTypeFactory<SuggestBackend> defaultSetup = SuggestBackendKV.factory();

    private static final Map<String, BackendTypeFactory<SuggestBackend>> backendTypes =
            new HashMap<>();

    static {
        backendTypes.put("kv", defaultSetup);
        backendTypes.put("v1", SuggestBackendV1.factory());
    }

    public static final List<String> types() {
        return ImmutableList.copyOf(backendTypes.keySet());
    }

    @JsonIgnore
    private final BackendTypeFactory<SuggestBackend> backendTypeBuilder;

    @JsonCreator
    public ElasticsearchSuggestModule(@JsonProperty("id") String id,
            @JsonProperty("groups") Groups groups,
            @JsonProperty("connection") ManagedConnectionFactory connection,
            @JsonProperty("writesPerSecond") Double writesPerSecond,
            @JsonProperty("writeCacheDurationMinutes") Long writeCacheDurationMinutes,
            @JsonProperty("templateName") String templateName,
            @JsonProperty("backendType") String backendType) {
        this.id = id;
        this.groups = ofNullable(groups).orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.connection = ofNullable(connection).orElseGet(ManagedConnectionFactory::buildDefault);
        this.writesPerSecond = ofNullable(writesPerSecond).orElse(DEFAULT_WRITES_PER_SECOND);
        this.writeCacheDurationMinutes =
                ofNullable(writeCacheDurationMinutes).orElse(DEFAULT_WRITES_CACHE_DURATION_MINUTES);
        this.templateName = ofNullable(templateName).orElse(DEFAULT_TEMPLATE_NAME);
        this.backendType = ofNullable(backendType).orElse(DEFAULT_BACKEND_TYPE);
        this.backendTypeBuilder = ofNullable(backendTypes.get(backendType)).orElse(defaultSetup);
    }

    @Override
    public Module module(final Key<SuggestBackend> key, final String id) {
        final BackendType<SuggestBackend> backendType = backendTypeBuilder.setup();

        return new PrivateModule() {
            @Provides
            @Singleton
            public Groups groups() {
                return groups;
            }

            @Provides
            @Singleton
            public LocalMetadataBackendReporter reporter(LocalMetadataManagerReporter reporter) {
                return reporter.newMetadataBackend(id);
            }

            @Provides
            @Singleton
            public Managed<Connection> connection(ManagedConnectionFactory connection)
                    throws IOException {
                return connection.construct(templateName, backendType.mappings(),
                        backendType.settings());
            }

            @Provides
            @Singleton
            @Named("configure")
            public boolean configure(ExtraParameters params) {
                return params.contains(ExtraParameters.CONFIGURE)
                        || params.contains(ELASTICSEARCH_CONFIGURE_PARAM);
            }

            @Provides
            @Singleton
            public RateLimitedCache<Pair<String, Series>, AsyncFuture<WriteResult>> writeCache()
                    throws IOException {
                final Cache<Pair<String, Series>, AsyncFuture<WriteResult>> cache = CacheBuilder
                        .newBuilder().concurrencyLevel(4)
                        .expireAfterWrite(writeCacheDurationMinutes, TimeUnit.MINUTES).build();

                if (writesPerSecond <= 0d) {
                    return new DisabledRateLimitedCache<>(cache);
                }

                return new DefaultRateLimitedCache<>(cache, RateLimiter.create(writesPerSecond));
            }

            @Override
            protected void configure() {
                bind(ManagedConnectionFactory.class).toInstance(connection);
                bind(key).to(backendType.type()).in(Scopes.SINGLETON);
                expose(key);
            }
        };
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("elasticsearch-suggest#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private Groups groups;
        private ManagedConnectionFactory connection;
        private Double writesPerSecond;
        private Long writeCacheDurationMinutes;
        private String templateName;
        private String backendType;

        public Builder id(final String id) {
            this.id = checkNotNull(id, "id");
            return this;
        }

        public Builder group(final Groups groups) {
            this.groups = checkNotNull(groups, "groups");
            return this;
        }

        public Builder connection(final ManagedConnectionFactory connection) {
            this.connection = checkNotNull(connection, "connection");
            return this;
        }

        public Builder writesPerSecond(double writesPerSecond) {
            this.writesPerSecond = writesPerSecond;
            return this;
        }

        public Builder writeCacheDurationMinutes(long writeCacheDurationMinutes) {
            this.writeCacheDurationMinutes = writeCacheDurationMinutes;
            return this;
        }

        public Builder templateName(final String templateName) {
            this.templateName = checkNotNull(templateName, "templateName");
            return this;
        }

        public Builder backendType(final String backendType) {
            this.backendType = checkNotNull(backendType, "backendType");
            return this;
        }

        public ElasticsearchSuggestModule build() {
            return new ElasticsearchSuggestModule(id, groups, connection, writesPerSecond,
                    writeCacheDurationMinutes, templateName, backendType);
        }
    }
}
