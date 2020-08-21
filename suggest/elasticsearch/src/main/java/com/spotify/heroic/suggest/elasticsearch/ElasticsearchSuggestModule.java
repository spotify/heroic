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
import static java.lang.Math.toIntExact;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.DynamicModuleId;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.ModuleId;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.DefaultRateLimitedCache;
import com.spotify.heroic.elasticsearch.DisabledRateLimitedCache;
import com.spotify.heroic.elasticsearch.DistributedRateLimitedCache;
import com.spotify.heroic.elasticsearch.MemcachedConnection;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.suggest.NumSuggestionsLimit;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestModule;
import dagger.Component;
import dagger.Lazy;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.Managed;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.inject.Named;
import org.apache.commons.lang3.tuple.Pair;

@ModuleId("elasticsearch")
public final class ElasticsearchSuggestModule implements SuggestModule, DynamicModuleId {
    public static final String ELASTICSEARCH_CONFIGURE_PARAM = "elasticsearch.configure";

    private static final double DEFAULT_WRITES_PER_SECOND = 3000d;
    private static final long DEFAULT_RATE_LIMIT_SLOW_START_SECONDS = 0L;

    private static final long DEFAULT_WRITES_CACHE_DURATION_MINUTES = 240L;
    private static final int DEFAULT_WRITE_CACHE_CONCURRENCY = 4;
    private static final long DEFAULT_WRITE_CACHE_MAX_SIZE = 30_000_000L;

    private static final String DEFAULT_GROUP = "elasticsearch";
    private static final String DEFAULT_TEMPLATE_NAME = "heroic-suggest";
    private static final boolean DEFAULT_CONFIGURE = false;

    private final Optional<String> id;
    private final Groups groups;
    private final ConnectionModule connection;
    private final double writesPerSecond;
    private final Long rateLimitSlowStartSeconds;
    private final long writeCacheDurationMinutes;
    private final Integer writeCacheConcurrency;
    private final Long writeCacheMaxSize;
    private final String distributedCacheSrvRecord;
    private final String templateName;
    private final boolean configure;
    private final NumSuggestionsLimit numSuggestionsLimit;

    private static Supplier<BackendType> defaultSetup = SuggestBackendKV.factory();

    private static final Map<String, Supplier<BackendType>> backendTypes = new HashMap<>();

    static {
        backendTypes.put("kv", defaultSetup);
    }

    public static List<String> types() {
        return ImmutableList.copyOf(backendTypes.keySet());
    }

    @JsonIgnore
    private final Supplier<BackendType> type;

    @JsonCreator
    public ElasticsearchSuggestModule(
        @JsonProperty("id") Optional<String> id,
        @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("connection") Optional<ConnectionModule> connection,
        @JsonProperty("writesPerSecond") Optional<Double> writesPerSecond,
        @JsonProperty("rateLimitSlowStartSeconds") Optional<Long> rateLimitSlowStartSeconds,
        @JsonProperty("writeCacheDurationMinutes") Optional<Long> writeCacheDurationMinutes,
        @JsonProperty("writeCacheConcurrency") Optional<Integer> writeCacheConcurrency,
        @JsonProperty("writeCacheMaxSize") Optional<Long> writeCacheMaxSize,
        @JsonProperty("distributedCacheSrvRecord") Optional<String> distributedCacheSrvRecord,
        @JsonProperty("templateName") Optional<String> templateName,
        @JsonProperty("backendType") Optional<String> backendType,
        @JsonProperty("configure") Optional<Boolean> configure,
        @JsonProperty("numSuggestionsLimit") Optional<Integer> numSuggestionsLimit
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.connection = connection.orElseGet(ConnectionModule::buildDefault);
        this.writesPerSecond = writesPerSecond.orElse(DEFAULT_WRITES_PER_SECOND);
        this.rateLimitSlowStartSeconds =
            rateLimitSlowStartSeconds.orElse(DEFAULT_RATE_LIMIT_SLOW_START_SECONDS);

        this.writeCacheDurationMinutes =
            writeCacheDurationMinutes.orElse(DEFAULT_WRITES_CACHE_DURATION_MINUTES);
        this.writeCacheConcurrency = writeCacheConcurrency.orElse(DEFAULT_WRITE_CACHE_CONCURRENCY);
        this.writeCacheMaxSize = writeCacheMaxSize.orElse(DEFAULT_WRITE_CACHE_MAX_SIZE);

        this.distributedCacheSrvRecord = distributedCacheSrvRecord.orElse("");

        this.templateName = templateName.orElse(DEFAULT_TEMPLATE_NAME);
        this.type = backendType.map(this::lookupBackendType).orElse(defaultSetup);
        this.configure = configure.orElse(DEFAULT_CONFIGURE);

        this.numSuggestionsLimit = NumSuggestionsLimit.of(numSuggestionsLimit);
    }

    private Supplier<BackendType> lookupBackendType(final String bt) {
        final Supplier<BackendType> type = backendTypes.get(bt);

        if (type == null) {
            throw new IllegalArgumentException(
                "Invalid backend type (" + bt + "), must be one of " + backendTypes.keySet());
        }

        return type;
    }

    @Override
    public Exposed module(PrimaryComponent primary,
                          Depends depends,
                          final String id) {
        final BackendType backendType = type.get();

        return DaggerElasticsearchSuggestModule_C
                .builder()
                .primaryComponent(primary)
                .depends(depends)
                .connectionModule(connection)
                .m(new M(backendType))
                .o(new O())
                .build();
    }

    @ElasticsearchScope
    @Component(modules = {M.class, O.class, ConnectionModule.class},
            dependencies = {PrimaryComponent.class, Depends.class})
    interface C extends Exposed {
        @Override
        SuggestBackend backend();

        @Override
        LifeCycle life();
    }

    @Module
    class M {
        private final BackendType backendType;

        public M(final BackendType backendType) {
            this.backendType = backendType;
        }

        @Provides
        @ElasticsearchScope
        public Groups groups() {
            return groups;
        }

        @Provides
        @ElasticsearchScope
        public Managed<Connection> connection(ConnectionModule.Provider provider) {
            return provider.construct(templateName, backendType);
        }

        @Provides
        @ElasticsearchScope
        @Named("configure")
        public boolean configure(ExtraParameters params) {
            return configure || params.contains(ExtraParameters.CONFIGURE) ||
                params.contains(ELASTICSEARCH_CONFIGURE_PARAM);
        }

        @Provides
        @ElasticsearchScope
        @Named("numSuggestionsLimit")
        public int numSuggestionsLimit(ExtraParameters params) {
            return numSuggestionsLimit.getLimit();
        }

        @Provides
        @ElasticsearchScope
        public RateLimitedCache<Pair<String, HashCode>> writeCache(final HeroicReporter reporter) {
            final Cache<Pair<String, HashCode>, Boolean> cache = CacheBuilder
                .newBuilder()
                .concurrencyLevel(writeCacheConcurrency)
                .maximumSize(writeCacheMaxSize)
                .expireAfterWrite(writeCacheDurationMinutes, MINUTES)
                .build();

            reporter.registerCacheSize("elasticsearch-suggest-write-through", cache::size);


            if (writesPerSecond <= 0d) {
                return new DisabledRateLimitedCache<>(cache.asMap());
            }

            if (distributedCacheSrvRecord.length() > 0) {
                return new DistributedRateLimitedCache<>(
                  cache.asMap(),
                  RateLimiter.create(writesPerSecond, rateLimitSlowStartSeconds, SECONDS),
                  MemcachedConnection.create(distributedCacheSrvRecord),
                  toIntExact(Duration.of(writeCacheDurationMinutes, MINUTES).convert(SECONDS)),
                  reporter.newMemcachedReporter("suggest")
                );
            }

            return new DefaultRateLimitedCache<>(cache.asMap(),
                RateLimiter.create(writesPerSecond, rateLimitSlowStartSeconds, SECONDS));
        }

        @Provides
        @ElasticsearchScope
        public SuggestBackend suggestBackend(Lazy<SuggestBackendKV> kv) {
            return kv.get();
        }

        @Provides
        @ElasticsearchScope
        public LifeCycle life(
                LifeCycleManager manager, Lazy<SuggestBackendKV> kv
        ) {
            return manager.build(kv.get());
        }
    }

    @Module
    class O {
        @Provides
        @ElasticsearchScope
        public Integer numSuggestionsLimit() {
            return numSuggestionsLimit.getLimit();
        }
    }

    @Override
    public Optional<String> id() {
        return id;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<String> id = empty();
        private Optional<Groups> groups = empty();
        private Optional<ConnectionModule> connection = empty();
        private Optional<Double> writesPerSecond = empty();
        private Optional<Long> rateLimitSlowStartSeconds = empty();
        private Optional<Long> writeCacheDurationMinutes = empty();
        private Optional<Integer> writeCacheConcurrency = empty();
        private Optional<Long> writeCacheMaxSize = empty();
        private Optional<String> distributedCacheSrvRecord = empty();
        private Optional<String> templateName = empty();
        private Optional<String> backendType = empty();
        private Optional<Boolean> configure = empty();
        private Optional<NumSuggestionsLimit> numSuggestionsLimit =
                Optional.of(NumSuggestionsLimit.of());

        public Builder id(final String id) {
            checkNotNull(id, "id");
            this.id = of(id);
            return this;
        }

        public Builder numSuggestionsLimit(final NumSuggestionsLimit numSuggestionsLimit) {
            checkNotNull(numSuggestionsLimit, "numSuggestionsLimit");
            this.numSuggestionsLimit = of(numSuggestionsLimit);
            return this;
        }

        public Builder group(final Groups groups) {
            checkNotNull(groups, "groups");
            this.groups = of(groups);
            return this;
        }

        public Builder connection(final ConnectionModule connection) {
            checkNotNull(connection, "connection");
            this.connection = of(connection);
            return this;
        }

        public Builder writesPerSecond(double writesPerSecond) {
            checkNotNull(writesPerSecond, "writesPerSecond");
            this.writesPerSecond = of(writesPerSecond);
            return this;
        }

        public Builder rateLimitSlowStartSeconds(final long rateLimitSlowStartSeconds) {
            checkNotNull(rateLimitSlowStartSeconds, "rateLimitSlowStartSeconds");
            this.rateLimitSlowStartSeconds = of(rateLimitSlowStartSeconds);
            return this;
        }

        public Builder writeCacheDurationMinutes(long writeCacheDurationMinutes) {
            checkNotNull(writeCacheDurationMinutes, "writeCacheDurationMinutes");
            this.writeCacheDurationMinutes = of(writeCacheDurationMinutes);
            return this;
        }

        public Builder writeCacheConcurrency(final int writeCacheConcurrency) {
            this.writeCacheConcurrency = of(writeCacheConcurrency);
            return this;
        }

        public Builder writeCacheMaxSize(final long writeCacheMaxSize) {
            this.writeCacheMaxSize = of(writeCacheMaxSize);
            return this;
        }

        public Builder distributedCacheSrvRecord(final String distributedCacheSrvRecord) {
            checkNotNull(distributedCacheSrvRecord, "distributedCacheSrvRecord");
            this.distributedCacheSrvRecord = of(distributedCacheSrvRecord);
            return this;
        }

        public Builder templateName(final String templateName) {
            checkNotNull(templateName, "templateName");
            this.templateName = of(templateName);
            return this;
        }

        public Builder backendType(final String backendType) {
            checkNotNull(backendType, "backendType");
            this.backendType = of(backendType);
            return this;
        }

        public Builder configure(final boolean configure) {
            this.configure = of(configure);
            return this;
        }

        public ElasticsearchSuggestModule build() {
            return new ElasticsearchSuggestModule(
                id,
                groups,
                    connection,
                    writesPerSecond,
                    rateLimitSlowStartSeconds,
                    writeCacheDurationMinutes,
                    writeCacheConcurrency,
                    writeCacheMaxSize,
                    distributedCacheSrvRecord,
                    templateName,
                    backendType,
                    configure,
                    numSuggestionsLimit.get().asOptionalInt());
        }
    }
}
