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

package com.spotify.heroic.analytics.bigtable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.spotify.heroic.analytics.AnalyticsModule;
import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.metric.bigtable.BigtableConnection;
import com.spotify.heroic.metric.bigtable.BigtableConnectionBuilder;
import com.spotify.heroic.metric.bigtable.CredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.ComputeEngineCredentialsBuilder;
import com.spotify.heroic.statistics.AnalyticsReporter;
import com.spotify.heroic.statistics.HeroicReporter;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import javax.inject.Singleton;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@RequiredArgsConstructor
public class BigtableAnalyticsModule implements AnalyticsModule {
    public static final String DEFAULT_CLUSTER = "heroic";
    public static final CredentialsBuilder DEFAULT_CREDENTIALS =
            new ComputeEngineCredentialsBuilder();
    public static final String HITS_TABLE = "hits";
    public static final String HITS_COLUMN_FAMILY = "hits";
    public static final int DEFAULT_MAX_PENDING_REPORTS = 1000;

    private final String project;
    private final String zone;
    private final String cluster;
    private final CredentialsBuilder credentials;
    private final int maxPendingReports;

    @Override
    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            public AnalyticsReporter reporter(final HeroicReporter reporter) {
                return reporter.newAnalyticsReporter();
            }

            @Provides
            @Singleton
            public Managed<BigtableConnection> connection(final AsyncFramework async,
                    final ExecutorService executorService) {
                return async.managed(new ManagedSetup<BigtableConnection>() {
                    @Override
                    public AsyncFuture<BigtableConnection> construct() throws Exception {
                        return async.call(new BigtableConnectionBuilder(project, zone, cluster,
                                credentials, async, executorService));
                    }

                    @Override
                    public AsyncFuture<Void> destruct(final BigtableConnection value)
                            throws Exception {
                        return async.call(new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                value.close();
                                return null;
                            }
                        });
                    }
                });
            }

            @Override
            protected void configure() {
                bind(MetricAnalytics.class).toInstance(new BigtableMetricAnalytics(HITS_TABLE,
                        HITS_COLUMN_FAMILY, maxPendingReports));
                expose(MetricAnalytics.class);
            }
        };
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor
    public static class Builder implements AnalyticsModule.Builder {
        private Optional<String> project = Optional.empty();
        private Optional<String> zone = Optional.empty();
        private Optional<String> cluster = Optional.empty();
        private Optional<CredentialsBuilder> credentials = Optional.empty();
        private Optional<Integer> maxPendingReports = Optional.empty();

        @JsonCreator
        public Builder(@JsonProperty("project") Optional<String> project,
                @JsonProperty("zone") Optional<String> zone,
                @JsonProperty("cluster") Optional<String> cluster,
                @JsonProperty("credentials") Optional<CredentialsBuilder> credentials,
                @JsonProperty("maxPendingReports") Optional<Integer> maxPendingReports) {
            this.project = project;
            this.zone = zone;
            this.cluster = cluster;
            this.credentials = credentials;
            this.maxPendingReports = maxPendingReports;
        }

        public Builder project(String project) {
            this.project = Optional.of(project);
            return this;
        }

        public Builder zone(String zone) {
            this.zone = Optional.of(zone);
            return this;
        }

        public Builder cluster(String cluster) {
            this.cluster = Optional.of(cluster);
            return this;
        }

        public Builder credentials(CredentialsBuilder credentials) {
            this.credentials = Optional.of(credentials);
            return this;
        }

        public Builder maxPendingReports(int maxPendingReports) {
            this.maxPendingReports = Optional.of(maxPendingReports);
            return this;
        }

        public BigtableAnalyticsModule build() {
            final String project = this.project.orElseThrow(
                    () -> new IllegalStateException("'project' configuration is required"));

            final String zone = this.zone.orElseThrow(
                    () -> new IllegalStateException("'zone' configuration is required"));

            return new BigtableAnalyticsModule(project, zone, cluster.orElse(DEFAULT_CLUSTER),
                    credentials.orElse(DEFAULT_CREDENTIALS),
                    maxPendingReports.orElse(DEFAULT_MAX_PENDING_REPORTS));
        }
    }
}
