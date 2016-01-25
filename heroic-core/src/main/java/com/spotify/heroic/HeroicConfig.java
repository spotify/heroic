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

package com.spotify.heroic;

import static com.spotify.heroic.common.Optionals.mergeOptional;
import static com.spotify.heroic.common.Optionals.mergeOptionalList;
import static com.spotify.heroic.common.Optionals.pickOptional;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregationcache.AggregationCacheModule;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.ingestion.IngestionModule;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.shell.ShellServerModule;
import com.spotify.heroic.suggest.SuggestManagerModule;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import jersey.repackaged.com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final List<ConsumerModule> DEFAULT_CONSUMERS = ImmutableList.of();
    public static final boolean DEFAULT_ENABLE_CORS = true;
    public static final Duration DEFAULT_START_TIMEOUT = Duration.of(5, TimeUnit.MINUTES);
    public static final Duration DEFAULT_STOP_TIMEOUT = Duration.of(1, TimeUnit.MINUTES);

    /**
     * The time core will wait for all services (implementing
     * {@link com.spotify.heroic.common.LifeCycle} to start before giving up.
     */
    private final Duration startTimeout;

    /**
     * The time core will wait for all services (implementing
     * {@link com.spotify.heroic.common.LifeCycle} to stop before giving up.
     */
    private final Duration stopTimeout;
    private final Optional<String> host;
    private final Optional<Integer> port;
    private final Optional<Boolean> disableMetrics;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;
    private final Set<String> features;
    private final ClusterManagerModule cluster;
    private final MetricManagerModule metric;
    private final MetadataManagerModule metadata;
    private final SuggestManagerModule suggest;
    private final AggregationCacheModule cache;
    private final IngestionModule ingestion;
    private final List<ConsumerModule> consumers;
    private final Optional<ShellServerModule> shellServer;

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class Builder {
        private Optional<Duration> startTimeout = empty();
        private Optional<Duration> stopTimeout = empty();
        private Optional<String> host = empty();
        private Optional<Integer> port = empty();
        private Optional<Boolean> disableMetrics = empty();
        private Optional<Boolean> enableCors = empty();
        private Optional<String> corsAllowOrigin = empty();
        private Set<String> features = ImmutableSet.of();
        private Optional<ClusterManagerModule.Builder> cluster = empty();
        private Optional<MetricManagerModule.Builder> metric = empty();
        private Optional<MetadataManagerModule.Builder> metadata = empty();
        private Optional<SuggestManagerModule.Builder> suggest = empty();
        private Optional<AggregationCacheModule.Builder> cache = empty();
        private Optional<IngestionModule.Builder> ingestion = empty();
        private Optional<List<ConsumerModule.Builder>> consumers = empty();
        private Optional<ShellServerModule.Builder> shellServer = empty();

        @JsonCreator
        public Builder(@JsonProperty("startTimeout") Duration startTimeout,
                @JsonProperty("stopTimeout") Duration stopTimeout,
                @JsonProperty("host") String host, @JsonProperty("port") Integer port,
                @JsonProperty("disableMetrics") Boolean disableMetrics,
                @JsonProperty("enableCors") Boolean enableCors,
                @JsonProperty("corsAllowOrigin") String corsAllowOrigin,
                @JsonProperty("features") Set<String> features,
                @JsonProperty("cluster") ClusterManagerModule.Builder cluster,
                @JsonProperty("metrics") MetricManagerModule.Builder metrics,
                @JsonProperty("metadata") MetadataManagerModule.Builder metadata,
                @JsonProperty("suggest") SuggestManagerModule.Builder suggest,
                @JsonProperty("cache") AggregationCacheModule.Builder cache,
                @JsonProperty("ingestion") IngestionModule.Builder ingestion,
                @JsonProperty("consumers") List<ConsumerModule.Builder> consumers,
                @JsonProperty("shellServer") ShellServerModule.Builder shellServer) {
            this.startTimeout = ofNullable(startTimeout);
            this.stopTimeout = ofNullable(stopTimeout);
            this.host = ofNullable(host);
            this.port = ofNullable(port);
            this.disableMetrics = ofNullable(disableMetrics);
            this.enableCors = ofNullable(enableCors);
            this.corsAllowOrigin = ofNullable(corsAllowOrigin);
            this.features = ofNullable(features).orElseGet(ImmutableSet::of);
            this.cluster = ofNullable(cluster);
            this.metric = ofNullable(metrics);
            this.metadata = ofNullable(metadata);
            this.suggest = ofNullable(suggest);
            this.cache = ofNullable(cache);
            this.ingestion = ofNullable(ingestion);
            this.consumers = ofNullable(consumers);
            this.shellServer = ofNullable(shellServer);
        }

        public Builder startTimeout(Duration startTimeout) {
            this.startTimeout = of(startTimeout);
            return this;
        }

        public Builder stopTimeout(Duration stopTimeout) {
            this.stopTimeout = of(stopTimeout);
            return this;
        }

        public Builder disableMetrics(boolean disableMetrics) {
            this.disableMetrics = of(disableMetrics);
            return this;
        }

        public Builder host(String host) {
            this.host = of(host);
            return this;
        }

        public Builder port(Integer port) {
            this.port = of(port);
            return this;
        }

        public Builder features(Set<String> features) {
            this.features = features;
            return this;
        }

        public Builder cluster(ClusterManagerModule.Builder cluster) {
            this.cluster = of(cluster);
            return this;
        }

        public Builder metric(MetricManagerModule.Builder metric) {
            this.metric = of(metric);
            return this;
        }

        public Builder metadata(MetadataManagerModule.Builder metadata) {
            this.metadata = of(metadata);
            return this;
        }

        public Builder suggest(SuggestManagerModule.Builder suggest) {
            this.suggest = of(suggest);
            return this;
        }

        public Builder cache(AggregationCacheModule.Builder cache) {
            this.cache = of(cache);
            return this;
        }

        public Builder ingestion(IngestionModule.Builder ingestion) {
            this.ingestion = of(ingestion);
            return this;
        }

        public Builder consumers(List<ConsumerModule.Builder> consumers) {
            this.consumers = of(consumers);
            return this;
        }

        public Builder shellServer(ShellServerModule.Builder shellServer) {
            this.shellServer = of(shellServer);
            return this;
        }

        public Builder merge(Builder o) {
            // @formatter:off
            return new Builder(
                pickOptional(startTimeout, o.startTimeout),
                pickOptional(stopTimeout, o.stopTimeout),
                pickOptional(host, o.host),
                pickOptional(port, o.port),
                pickOptional(disableMetrics, o.disableMetrics),
                pickOptional(enableCors, o.enableCors),
                pickOptional(corsAllowOrigin, o.corsAllowOrigin),
                ImmutableSet.copyOf(Sets.union(features, o.features)),
                mergeOptional(cluster, o.cluster, (a, b) -> a.merge(b)),
                mergeOptional(metric, o.metric, (a, b) -> a.merge(b)),
                mergeOptional(metadata, o.metadata, (a, b) -> a.merge(b)),
                mergeOptional(suggest, o.suggest, (a, b) -> a.merge(b)),
                mergeOptional(cache, o.cache, (a, b) -> a.merge(b)),
                mergeOptional(ingestion, o.ingestion, (a, b) -> a.merge(b)),
                mergeOptionalList(consumers, o.consumers),
                mergeOptional(shellServer, o.shellServer, (a, b) -> a.merge(b))
            );
            // @formatter:on
        }

        public HeroicConfig build() {
            // @formatter:off
            return new HeroicConfig(
                startTimeout.orElse(DEFAULT_START_TIMEOUT),
                stopTimeout.orElse(DEFAULT_STOP_TIMEOUT),
                host,
                port,
                disableMetrics,
                enableCors.orElse(DEFAULT_ENABLE_CORS),
                corsAllowOrigin,
                features,
                cluster.orElseGet(ClusterManagerModule::builder).build(),
                metric.orElseGet(MetricManagerModule::builder).build(),
                metadata.orElseGet(MetadataManagerModule::builder).build(),
                suggest.orElseGet(SuggestManagerModule::builder).build(),
                cache.orElseGet(AggregationCacheModule::builder).build(),
                ingestion.orElseGet(IngestionModule::builder).build(),
                consumers.map(cl -> {
                    return ImmutableList.copyOf(cl.stream().map(c -> c.build()).iterator());
                }).orElseGet(ImmutableList::of),
                shellServer.map(ShellServerModule.Builder::build)
            );
            // @formatter:on
        }
    }
}
