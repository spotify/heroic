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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import lombok.Data;

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
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

@Data
public final class ElasticsearchSuggestModule implements SuggestModule {
    private static final double DEFAULT_WRITES_PER_SECOND = 3000d;
    private static final long DEFAULT_WRITES_CACHE_DURATION_MINUTES = 240l;
    public static final String DEFAULT_GROUP = "elasticsearch";
    public static final String DEFAULT_TEMPLATE_NAME = "heroic-suggest";
    public static final String DEFAULT_BACKEND_TYPE = "default";

    private final String id;
    private final Set<String> groups;
    private final ManagedConnectionFactory connection;
    private final double writesPerSecond;
    private final long writeCacheDurationMinutes;
    private final String templateName;
    private final String backendType;

    @JsonIgnore
    private final BackendTypeBuilder backendTypeBuilder;

    @JsonCreator
    public ElasticsearchSuggestModule(@JsonProperty("id") String id, @JsonProperty("group") String group,
            @JsonProperty("groups") Set<String> groups,
            @JsonProperty("connection") ManagedConnectionFactory connection,
            @JsonProperty("writesPerSecond") Double writesPerSecond,
            @JsonProperty("writeCacheDurationMinutes") Long writeCacheDurationMinutes,
            @JsonProperty("templateName") String templateName, @JsonProperty("backendType") String backendType) {
        this.id = id;
        this.groups = Groups.groups(group, groups, DEFAULT_GROUP);
        this.connection = Optional.fromNullable(connection).or(ManagedConnectionFactory.provideDefault());
        this.writesPerSecond = Optional.fromNullable(writesPerSecond).or(DEFAULT_WRITES_PER_SECOND);
        this.writeCacheDurationMinutes = Optional.fromNullable(writeCacheDurationMinutes).or(DEFAULT_WRITES_CACHE_DURATION_MINUTES);
        this.templateName = Optional.fromNullable(templateName).or(DEFAULT_TEMPLATE_NAME);
        this.backendType = Optional.fromNullable(backendType).or(DEFAULT_BACKEND_TYPE);
        this.backendTypeBuilder = Optional.fromNullable(backendTypeBuilders.get(backendType)).or(defaultSetup);
    }

    private static Map<String, Object> loadJsonResource(String path) throws IOException {
        final String fullPath = ElasticsearchSuggestModule.class.getPackage().getName() + "/" + path;

        try (final InputStream input = ElasticsearchSuggestModule.class.getClassLoader().getResourceAsStream(fullPath)) {
            if (input == null)
                return ImmutableMap.of();

            return JsonXContent.jsonXContent.createParser(input).map();
        }
    }

    @Override
    public Module module(final Key<SuggestBackend> key, final String id) {
        final BackendType backendType = backendTypeBuilder.setup(this);

        return new PrivateModule() {
            @Provides
            @Singleton
            public LocalMetadataBackendReporter reporter(LocalMetadataManagerReporter reporter) {
                return reporter.newMetadataBackend(id);
            }

            @Provides
            @Singleton
            public Managed<Connection> connection(ManagedConnectionFactory connection) throws IOException {
                return connection.construct(templateName, backendType.mappings(), backendType.settings());
            }

            @Provides
            @Singleton
            public RateLimitedCache<Pair<String, Series>, AsyncFuture<WriteResult>> writeCache() throws IOException {
                final Cache<Pair<String, Series>, AsyncFuture<WriteResult>> cache = CacheBuilder.newBuilder()
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
                bind(key).toInstance(backendType.instance());
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

        public ElasticsearchSuggestModule build() {
            return new ElasticsearchSuggestModule(id, group, groups, connection, writesPerSecond,
                    writeCacheDurationMinutes, templateName, backendType);
        }
    }

    private static BackendTypeBuilder defaultSetup = new BackendTypeBuilder() {
        @Override
        public BackendType setup(final ElasticsearchSuggestModule module) {
            return new BackendType() {
                @Override
                public Map<String, Map<String, Object>> mappings() throws IOException {
                    final Map<String, Map<String, Object>> mappings = new HashMap<>();
                    mappings.put("tag", loadJsonResource("kv/tag.json"));
                    mappings.put("series", loadJsonResource("kv/series.json"));
                    return mappings;
                }

                @Override
                public Map<String, Object> settings() throws IOException {
                    return loadJsonResource("kv/settings.json");
                }

                @Override
                public SuggestBackend instance() {
                    return new ElasticsearchSuggestKVBackend(module.groups);
                }
            };
        }
    };

    private static final Map<String, BackendTypeBuilder> backendTypeBuilders = new HashMap<>();

    static {
        backendTypeBuilders.put("default", defaultSetup);
    }

    static interface BackendType {
        Map<String, Map<String, Object>> mappings() throws IOException;

        Map<String, Object> settings() throws IOException;

        SuggestBackend instance();
    }

    static interface BackendTypeBuilder {
        BackendType setup(ElasticsearchSuggestModule module);
    }
}
