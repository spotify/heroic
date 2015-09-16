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

package com.spotify.heroic.metadata.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.BackendTypeFactory;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.DefaultRateLimitedCache;
import com.spotify.heroic.elasticsearch.DisabledRateLimitedCache;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import lombok.Data;

@Data
public final class ElasticsearchMetadataModule implements MetadataModule {
    private static final double DEFAULT_WRITES_PER_SECOND = 3000d;
    private static final long DEFAULT_WRITES_CACHE_DURATION_MINUTES = 240l;
    public static final String DEFAULT_GROUP = "elasticsearch";
    public static final String DEFAULT_TEMPLATE_NAME = "heroic-metadata";

    private final String id;
    private final Groups groups;
    private final ManagedConnectionFactory connection;
    private final String templateName;

    private final double writesPerSecond;
    private final long writeCacheDurationMinutes;

    private static BackendTypeFactory<ElasticsearchMetadataModule, MetadataBackend> defaultSetup = MetadataBackendKV.factory();

    private static final Map<String, BackendTypeFactory<ElasticsearchMetadataModule, MetadataBackend>> backendTypes = new HashMap<>();

    static {
        backendTypes.put("kv", defaultSetup);
        backendTypes.put("v1", MetadataBackendV1.factory());
    }

    @JsonIgnore
    private final BackendTypeFactory<ElasticsearchMetadataModule, MetadataBackend> backendTypeBuilder;

    @JsonCreator
    public ElasticsearchMetadataModule(@JsonProperty("id") String id, @JsonProperty("group") String group,
            @JsonProperty("groups") Set<String> groups, @JsonProperty("connection") ManagedConnectionFactory connection,
            @JsonProperty("writesPerSecond") Double writesPerSecond,
            @JsonProperty("writeCacheDurationMinutes") Long writeCacheDurationMinutes,
            @JsonProperty("templateName") String templateName, @JsonProperty("backendType") String backendType) {
        this.id = id;
        this.groups = Groups.groups(group, groups, DEFAULT_GROUP);
        this.connection = Optional.fromNullable(connection).or(ManagedConnectionFactory.provideDefault());
        this.writesPerSecond = Optional.fromNullable(writesPerSecond).or(DEFAULT_WRITES_PER_SECOND);
        this.writeCacheDurationMinutes = Optional.fromNullable(writeCacheDurationMinutes)
                .or(DEFAULT_WRITES_CACHE_DURATION_MINUTES);
        this.templateName = Optional.fromNullable(templateName).or(DEFAULT_TEMPLATE_NAME);
        this.backendTypeBuilder = Optional.fromNullable(backendTypes.get(backendType)).or(defaultSetup);
    }

    @Override
    public Module module(final Key<MetadataBackend> key, final String id) {
        final BackendType<MetadataBackend> backendType = backendTypeBuilder.setup(this);

        return new PrivateModule() {
            @Provides
            @Singleton
            public LocalMetadataBackendReporter reporter(LocalMetadataManagerReporter reporter) {
                return reporter.newMetadataBackend(id);
            }

            @Provides
            @Singleton
            public Groups groups() {
                return groups;
            }

            @Provides
            @Inject
            public Managed<Connection> connection(ManagedConnectionFactory builder) throws IOException {
                return builder.construct(templateName, backendType.mappings());
            }

            @Provides
            @Singleton
            public RateLimitedCache<Pair<String, Series>, AsyncFuture<WriteResult>> writeCache() throws IOException {
                Cache<Pair<String, Series>, AsyncFuture<WriteResult>> cache = CacheBuilder.newBuilder()
                        .concurrencyLevel(4)
                        .expireAfterWrite(writeCacheDurationMinutes, TimeUnit.MINUTES).build();

                if (writesPerSecond == 0d)
                    return new DisabledRateLimitedCache<Pair<String, Series>, AsyncFuture<WriteResult>>(cache);

                RateLimiter rateLimiter = RateLimiter.create(writesPerSecond);
                return new DefaultRateLimitedCache<>(cache, rateLimiter);
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
        return String.format("elasticsearch#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String group;
        private Set<String> groups;
        private ManagedConnectionFactory connection;
        private Double writesPerSecond;
        private Long writeCacheDurationMinutes;
        private String templateName;
        private String backendType;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder group(Set<String> groups) {
            this.groups = groups;
            return this;
        }

        public Builder connection(ManagedConnectionFactory connection) {
            this.connection = connection;
            return this;
        }

        public Builder writesPerSecond(Double writesPerSecond) {
            this.writesPerSecond = writesPerSecond;
            return this;
        }

        public Builder writeCacheDurationMinutes(Long writeCacheDurationMinutes) {
            this.writeCacheDurationMinutes = writeCacheDurationMinutes;
            return this;
        }

        public Builder templateName(String templateName) {
            this.templateName = templateName;
            return this;
        }

        public Builder backendType(String backendType) {
            this.backendType = backendType;
            return this;
        }

        public ElasticsearchMetadataModule build() {
            return new ElasticsearchMetadataModule(id, group, groups, connection, writesPerSecond,
                    writeCacheDurationMinutes, templateName, backendType);
        }
    }
}
