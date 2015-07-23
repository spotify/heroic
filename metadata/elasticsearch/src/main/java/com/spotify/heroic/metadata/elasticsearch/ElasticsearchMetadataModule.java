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
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Named;
import javax.inject.Singleton;

import lombok.Data;

import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.DefaultRateLimitedCache;
import com.spotify.heroic.elasticsearch.DisabledRateLimitedCache;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.utils.GroupedUtils;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.Managed;

@Data
public final class ElasticsearchMetadataModule implements MetadataModule {
    private static final double DEFAULT_WRITES_PER_SECOND = 3000d;
    private static final long DEFAULT_WRITES_CACHE_DURATION_MINUTES = 240l;
    public static final String DEFAULT_GROUP = "elasticsearch";
    public static final String DEFAULT_TEMPLATE_NAME = "heroic-metadata";

    private final String id;
    private final Set<String> groups;
    private final ManagedConnectionFactory connection;
    private final ReadWriteThreadPools.Config pools;
    private final String templateName;

    private final double writesPerSecond;
    private final long writeCacheDurationMinutes;

    @JsonCreator
    public ElasticsearchMetadataModule(@JsonProperty("id") String id, @JsonProperty("group") String group,
            @JsonProperty("groups") Set<String> groups,
            @JsonProperty("connection") ManagedConnectionFactory connection,
            @JsonProperty("pools") ReadWriteThreadPools.Config pools, @JsonProperty("writesPerSecond") Double writesPerSecond, @JsonProperty("writeCacheDurationMinutes") Long writeCacheDurationMinutes,
            @JsonProperty("templateName") String templateName) {
        this.id = id;
        this.groups = GroupedUtils.groups(group, groups, DEFAULT_GROUP);
        this.connection = Optional.fromNullable(connection).or(ManagedConnectionFactory.provideDefault());
        this.pools = Optional.fromNullable(pools).or(ReadWriteThreadPools.Config.provideDefault());
        this.writesPerSecond = Optional.fromNullable(writesPerSecond).or(DEFAULT_WRITES_PER_SECOND);
        this.writeCacheDurationMinutes = Optional.fromNullable(writeCacheDurationMinutes).or(DEFAULT_WRITES_CACHE_DURATION_MINUTES);
        this.templateName = Optional.fromNullable(templateName).or(DEFAULT_TEMPLATE_NAME);

    }

    private Map<String, Map<String, Object>> mappings() throws IOException {
        final Map<String, Map<String, Object>> mappings = new HashMap<>();
        mappings.put("metadata", loadJsonResource("default/metadata.json"));
        return mappings;
    }

    private Map<String, Object> loadJsonResource(String string) throws IOException {
        final String fullPath = getClass().getPackage().getName() + "/" + string;

        try (final InputStream input = getClass().getClassLoader().getResourceAsStream(fullPath)) {
            if (input == null)
                return ImmutableMap.of();

            return JsonXContent.jsonXContent.createParser(input).map();
        }
    }

    @Override
    public Module module(final Key<MetadataBackend> key, final String id) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public LocalMetadataBackendReporter reporter(LocalMetadataManagerReporter reporter) {
                return reporter.newMetadataBackend(id);
            }

            @Provides
            @Singleton
            @Named("groups")
            public Set<String> groups() {
                return groups;
            }

            @Provides
            @Singleton
            public ReadWriteThreadPools pools(AsyncFramework async, LocalMetadataBackendReporter reporter) {
                return pools.construct(async, reporter.newThreadPool());
            }

            @Provides
            @Inject
            public Managed<Connection> connection(ManagedConnectionFactory builder) throws IOException {
                return builder.construct(templateName, mappings());
            }

            @Provides
            @Singleton
            public RateLimitedCache<Pair<String, Series>, Boolean> writeCache() throws IOException {
                Cache<Pair<String, Series>, Boolean> cache = CacheBuilder.newBuilder().concurrencyLevel(4)
                        .expireAfterWrite(writeCacheDurationMinutes, TimeUnit.MINUTES).build();

                if (writesPerSecond == 0d)
                    return new DisabledRateLimitedCache<Pair<String, Series>, Boolean>(cache);

                RateLimiter rateLimiter = RateLimiter.create(writesPerSecond);
                return new DefaultRateLimitedCache<>(cache, rateLimiter);
            }
            @Override
            protected void configure() {
                bind(ManagedConnectionFactory.class).toInstance(connection);
                bind(key).to(ElasticsearchMetadataBackend.class);
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
        private ReadWriteThreadPools.Config pools;
        private Double writesPerSecond;
        private Long writeCacheDurationMinutes;
        private String templateName;

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

        public Builder pools(ReadWriteThreadPools.Config pools) {
            this.pools = pools;
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

        public ElasticsearchMetadataModule build() {
            return new ElasticsearchMetadataModule(id, group, groups, connection, pools, writesPerSecond,
                    writeCacheDurationMinutes, templateName);
        }
    }
}
