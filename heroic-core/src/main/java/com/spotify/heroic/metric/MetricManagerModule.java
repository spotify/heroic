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

package com.spotify.heroic.metric;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.spotify.heroic.common.BackendGroups;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendGroupReporter;

@Data
public class MetricManagerModule extends PrivateModule {
    private static final List<MetricModule> DEFAULT_BACKENDS = ImmutableList.of();
    public static final boolean DEFAULT_UPDATE_METADATA = false;
    public static final int DEFAULT_GROUP_LIMIT = 500;
    public static final int DEFAULT_SERIES_LIMIT = 10000;
    public static final long DEFAULT_FLUSHING_INTERVAL = 1000;
    public static final long DEFAULT_AGGREGATION_LIMIT = 10000;
    public static final long DEFAULT_DATA_LIMIT = 30000000;
    public static final int DEFAULT_FETCH_PARALLELISM = 100;

    private final List<MetricModule> backends;
    private final List<String> defaultBackends;

    /**
     * Limit in how many groups we are allowed to return.
     */
    private final int groupLimit;

    /**
     * Limit in the number of series we may fetch from the metadata backend.
     */
    private final int seriesLimit;

    /**
     * Limit in how many datapoints a single aggregation is allowed to output.
     */
    private final long aggregationLimit;

    /**
     * Limit in how many datapoints a session is allowed to fetch in total.
     */
    private final long dataLimit;

    /**
     * How many data fetches are performed in parallel.
     */
    private final int fetchParallelism;

    @JsonCreator
    public MetricManagerModule(@JsonProperty("backends") List<MetricModule> backends,
            @JsonProperty("defaultBackends") List<String> defaultBackends,
            @JsonProperty("groupLimit") Integer groupLimit, @JsonProperty("seriesLimit") Integer seriesLimit,
            @JsonProperty("aggregationLimit") Long aggregationLimit, @JsonProperty("dataLimit") Long dataLimit,
            @JsonProperty("groupFetchParallelism") Integer groupFetchParallelism) {
        this.backends = Optional.fromNullable(backends).or(DEFAULT_BACKENDS);
        this.defaultBackends = defaultBackends;
        this.groupLimit = Optional.fromNullable(groupLimit).or(DEFAULT_GROUP_LIMIT);
        this.seriesLimit = Optional.fromNullable(seriesLimit).or(DEFAULT_SERIES_LIMIT);
        this.aggregationLimit = Optional.fromNullable(aggregationLimit).or(DEFAULT_AGGREGATION_LIMIT);
        this.dataLimit = Optional.fromNullable(dataLimit).or(DEFAULT_DATA_LIMIT);
        this.fetchParallelism = Optional.fromNullable(groupFetchParallelism).or(DEFAULT_FETCH_PARALLELISM);
    }

    public static Supplier<MetricManagerModule> defaultSupplier() {
        return new Supplier<MetricManagerModule>() {
            @Override
            public MetricManagerModule get() {
                return new MetricManagerModule(null, null, null, null, null, null, null);
            }
        };
    }

    @Inject
    @Provides
    @Singleton
    public LocalMetricManagerReporter localReporter(HeroicReporter reporter) {
        return reporter.newLocalMetricBackendManager();
    }

    @Inject
    @Provides
    @Singleton
    public MetricBackendGroupReporter metricBackendsReporter(HeroicReporter reporter) {
        return reporter.newMetricBackendsReporter();
    }

    @Inject
    @Provides
    @Singleton
    public ClusteredMetricManagerReporter clusteredReporter(HeroicReporter reporter) {
        return reporter.newClusteredMetricBackendManager();
    }

    @Provides
    public BackendGroups<MetricBackend> defaultBackends(Set<MetricBackend> configured) {
        return BackendGroups.build(configured, defaultBackends);
    }

    @Override
    protected void configure() {
        bindBackends(backends);
        bind(MetricManager.class).toInstance(
                new LocalMetricManager(groupLimit, seriesLimit, aggregationLimit, dataLimit, fetchParallelism));
        expose(MetricManager.class);
    }

    private void bindBackends(final Collection<MetricModule> configs) {
        final Multibinder<MetricBackend> bindings = Multibinder.newSetBinder(binder(), MetricBackend.class);

        int i = 0;

        for (final MetricModule config : configs) {
            final String id = config.id() != null ? config.id() : config.buildId(i++);

            final Key<MetricBackend> key = Key.get(MetricBackend.class, Names.named(id));

            install(config.module(key, id));

            bindings.addBinding().to(key);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<MetricModule> backends;
        private List<String> defaultBackends;
        private Integer groupLimit;
        private Integer seriesLimit;
        private Long aggregationLimit;
        private Long dataLimit;
        private Integer groupFetchParallelism;

        public Builder backends(List<MetricModule> backends) {
            this.backends = backends;
            return this;
        }

        public Builder defaultBackends(List<String> defaultBackends) {
            this.defaultBackends = defaultBackends;
            return this;
        }

        public Builder groupLimit(Integer groupLimit) {
            this.groupLimit = groupLimit;
            return this;
        }

        public Builder seriesLimit(Integer seriesLimit) {
            this.seriesLimit = seriesLimit;
            return this;
        }

        public Builder aggregationLimit(Long aggregationLimit) {
            this.aggregationLimit = aggregationLimit;
            return this;
        }

        public Builder dataLimit(Long dataLimit) {
            this.dataLimit = dataLimit;
            return this;
        }

        public Builder groupFetchParallelism(Integer groupFetchParallelism) {
            this.groupFetchParallelism = groupFetchParallelism;
            return this;
        }

        public MetricManagerModule build() {
            return new MetricManagerModule(backends, defaultBackends, groupLimit, seriesLimit, aggregationLimit,
                    dataLimit, groupFetchParallelism);
        }
    }
}
