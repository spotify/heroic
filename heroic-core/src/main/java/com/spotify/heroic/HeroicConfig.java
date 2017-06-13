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
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.analytics.AnalyticsModule;
import com.spotify.heroic.analytics.NullAnalyticsModule;
import com.spotify.heroic.cache.CacheModule;
import com.spotify.heroic.cache.noop.NoopCacheModule;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.generator.CoreGeneratorModule;
import com.spotify.heroic.ingestion.IngestionModule;
import com.spotify.heroic.jetty.JettyServerConnector;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.querylogging.QueryLoggingModule;
import com.spotify.heroic.querylogging.noop.NoopQueryLoggingModule;
import com.spotify.heroic.shell.ShellServerModule;
import com.spotify.heroic.statistics.StatisticsModule;
import com.spotify.heroic.statistics.noop.NoopStatisticsModule;
import com.spotify.heroic.suggest.SuggestManagerModule;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Data
public class HeroicConfig {
    public static final List<ConsumerModule> DEFAULT_CONSUMERS = ImmutableList.of();
    public static final boolean DEFAULT_ENABLE_CORS = true;
    public static final Duration DEFAULT_START_TIMEOUT = Duration.of(5, TimeUnit.MINUTES);
    public static final Duration DEFAULT_STOP_TIMEOUT = Duration.of(1, TimeUnit.MINUTES);

    public static final String DEFAULT_VERSION = "HEAD";
    public static final String DEFAULT_SERVICE = "The Heroic Time Series Database";

    private final Optional<String> id;

    /**
     * The time core will wait for all services (implementing
     * {@link com.spotify.heroic.lifecycle.LifeCycle}
     * to start before giving up.
     */
    private final Duration startTimeout;

    /**
     * The time core will wait for all services (implementing
     * {@link com.spotify.heroic.lifecycle.LifeCycle}
     * to stop before giving up.
     */
    private final Duration stopTimeout;
    private final Optional<String> host;
    private final Optional<Integer> port;
    private final List<JettyServerConnector> connectors;
    private final Optional<Boolean> disableMetrics;
    private final boolean enableCors;
    private final Optional<String> corsAllowOrigin;
    private final FeatureSet features;
    private final ClusterManagerModule cluster;
    private final MetricManagerModule metric;
    private final MetadataManagerModule metadata;
    private final SuggestManagerModule suggest;
    private final CacheModule cache;
    private final IngestionModule ingestion;
    private final List<ConsumerModule> consumers;
    private final Optional<ShellServerModule> shellServer;
    private final AnalyticsModule analytics;
    private final CoreGeneratorModule generator;
    private final StatisticsModule statistics;
    private final QueryLoggingModule queryLogging;

    private final String version;
    private final String service;

    public static Builder builder() {
        return new Builder();
    }

    static Optional<HeroicConfig.Builder> loadConfig(final ObjectMapper mapper, final Path path) {
        try (final InputStream in = Files.newInputStream(path)) {
            return loadConfig(mapper, in);
        } catch (final JsonMappingException e) {
            final JsonLocation location = e.getLocation();
            final String message =
                String.format("%s[%d:%d]: %s", path, location == null ? null : location.getLineNr(),
                    location == null ? null : location.getColumnNr(), e.getOriginalMessage());
            throw new RuntimeException(message, e);
        } catch (final Exception e) {
            final String message = String.format("%s: %s", path, e.getMessage());
            throw new RuntimeException(message, e);
        }
    }

    static Optional<HeroicConfig.Builder> loadConfigStream(
        final ObjectMapper mapper, final InputStream in
    ) {
        try {
            return loadConfig(mapper, in);
        } catch (final JsonMappingException e) {
            final JsonLocation location = e.getLocation();
            final String message =
                String.format("[%d:%d]: %s", location == null ? null : location.getLineNr(),
                    location == null ? null : location.getColumnNr(), e.getOriginalMessage());
            throw new RuntimeException(message, e);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    static Optional<HeroicConfig.Builder> loadConfig(
        final ObjectMapper mapper, final InputStream in
    ) throws JsonMappingException, IOException {
        final JsonParser parser = mapper.getFactory().createParser(in);

        if (parser.nextToken() == null) {
            return Optional.empty();
        }

        return Optional.of(parser.readValueAs(HeroicConfig.Builder.class));
    }

    static List<JettyServerConnector.Builder> defaultConnectors() {
        return ImmutableList.of(JettyServerConnector.builder());
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class Builder {
        private Optional<String> id = empty();
        private Optional<Duration> startTimeout = empty();
        private Optional<Duration> stopTimeout = empty();
        private Optional<String> host = empty();
        private Optional<Integer> port = empty();
        private Optional<List<JettyServerConnector.Builder>> connectors = empty();
        private Optional<Boolean> disableMetrics = empty();
        private Optional<Boolean> enableCors = empty();
        private Optional<String> corsAllowOrigin = empty();
        private Optional<FeatureSet> features = empty();
        private Optional<ClusterManagerModule.Builder> cluster = empty();
        private Optional<MetricManagerModule.Builder> metrics = empty();
        private Optional<MetadataManagerModule.Builder> metadata = empty();
        private Optional<SuggestManagerModule.Builder> suggest = empty();
        private Optional<CacheModule.Builder> cache = empty();
        private Optional<IngestionModule.Builder> ingestion = empty();
        private Optional<List<ConsumerModule.Builder>> consumers = empty();
        private Optional<ShellServerModule> shellServer = empty();
        private Optional<AnalyticsModule.Builder> analytics = empty();
        private Optional<CoreGeneratorModule.Builder> generator = empty();
        private Optional<StatisticsModule> statistics = empty();
        private Optional<QueryLoggingModule> queryLogging = empty();

        private Optional<String> version = empty();
        private Optional<String> service = empty();

        public Builder enableCors(boolean enableCors) {
            this.enableCors = of(enableCors);
            return this;
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

        public Builder features(FeatureSet features) {
            this.features = of(features);
            return this;
        }

        public Builder cluster(ClusterManagerModule.Builder cluster) {
            this.cluster = of(cluster);
            return this;
        }

        public Builder metrics(MetricManagerModule.Builder metrics) {
            this.metrics = of(metrics);
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

        public Builder cache(CacheModule.Builder cache) {
            this.cache = of(cache);
            return this;
        }

        public Builder ingestion(IngestionModule.Builder ingestion) {
            this.ingestion = of(ingestion);
            return this;
        }

        public Builder consumers(List<ConsumerModule.Builder> consumers) {
            requireNonNull(consumers, "consumers");
            this.consumers = of(consumers);
            return this;
        }

        public Builder analytics(AnalyticsModule.Builder analytics) {
            this.analytics = of(analytics);
            return this;
        }

        public Builder statistics(StatisticsModule statistics) {
            this.statistics = of(statistics);
            return this;
        }

        public Builder shellServer(ShellServerModule shellServer) {
            this.shellServer = of(shellServer);
            return this;
        }

        public Builder queryLogging(QueryLoggingModule queryLogging) {
            this.queryLogging = of(queryLogging);
            return this;
        }

        public Builder merge(Builder o) {
            // @formatter:off
            return new Builder(
                pickOptional(id, o.id),
                pickOptional(startTimeout, o.startTimeout),
                pickOptional(stopTimeout, o.stopTimeout),
                pickOptional(host, o.host),
                pickOptional(port, o.port),
                mergeOptionalList(connectors, o.connectors),
                pickOptional(disableMetrics, o.disableMetrics),
                pickOptional(enableCors, o.enableCors),
                pickOptional(corsAllowOrigin, o.corsAllowOrigin),
                mergeOptional(features, o.features, FeatureSet::combine),
                mergeOptional(cluster, o.cluster, ClusterManagerModule.Builder::merge),
                mergeOptional(metrics, o.metrics, MetricManagerModule.Builder::merge),
                mergeOptional(metadata, o.metadata, MetadataManagerModule.Builder::merge),
                mergeOptional(suggest, o.suggest, SuggestManagerModule.Builder::merge),
                pickOptional(cache, o.cache),
                mergeOptional(ingestion, o.ingestion, IngestionModule.Builder::merge),
                mergeOptionalList(consumers, o.consumers),
                pickOptional(shellServer, o.shellServer),
                pickOptional(analytics, o.analytics),
                mergeOptional(generator, o.generator, CoreGeneratorModule.Builder::merge),
                pickOptional(statistics, o.statistics),
                pickOptional(queryLogging, o.queryLogging),
                pickOptional(service, o.service),
                pickOptional(version, o.version)
            );
            // @formatter:on
        }

        public HeroicConfig build() {
            final List<JettyServerConnector> connectors = ImmutableList.copyOf(this.connectors
                .orElseGet(HeroicConfig::defaultConnectors)
                .stream()
                .map(JettyServerConnector.Builder::build)
                .iterator());

            final String defaultVersion = loadDefaultVersion().orElse(DEFAULT_VERSION);

            // @formatter:off
            return new HeroicConfig(
                id,
                startTimeout.orElse(DEFAULT_START_TIMEOUT),
                stopTimeout.orElse(DEFAULT_STOP_TIMEOUT),
                host,
                port,
                connectors,
                disableMetrics,
                enableCors.orElse(DEFAULT_ENABLE_CORS),
                corsAllowOrigin,
                features.orElseGet(FeatureSet::empty),
                cluster.orElseGet(ClusterManagerModule::builder).build(),
                metrics.orElseGet(MetricManagerModule::builder).build(),
                metadata.orElseGet(MetadataManagerModule::builder).build(),
                suggest.orElseGet(SuggestManagerModule::builder).build(),
                cache.orElseGet(NoopCacheModule::builder).build(),
                ingestion.orElseGet(IngestionModule::builder).build(),
                consumers.map(c -> c.stream().map(ConsumerModule.Builder::build).iterator()).map
                    (ImmutableList::copyOf).orElseGet(ImmutableList::of),
                shellServer,
                analytics.map(AnalyticsModule.Builder::build).orElseGet(NullAnalyticsModule::new),
                generator.orElseGet(CoreGeneratorModule::builder).build(),
                statistics.orElseGet(NoopStatisticsModule::new),
                queryLogging.orElseGet(NoopQueryLoggingModule::new),
                version.orElse(defaultVersion),
                service.orElse(DEFAULT_SERVICE)
            );
            // @formatter:on
        }

        private Optional<String> loadDefaultVersion() {
            try (final InputStream in = getClass()
                .getClassLoader()
                .getResourceAsStream("com.spotify.heroic/version")) {
                if (in == null) {
                    return Optional.empty();
                }

                final BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
                return Optional.of(reader.readLine());
            } catch (final Exception e) {
                log.warn("Failed to load version file", e);
                return Optional.empty();
            }
        }
    }
}
