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
import com.spotify.heroic.analytics.AnalyticsComponent;
import com.spotify.heroic.analytics.AnalyticsModule;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metric.bigtable.BigtableConnection;
import com.spotify.heroic.metric.bigtable.BigtableConnectionBuilder;
import com.spotify.heroic.metric.bigtable.CredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.ComputeEngineCredentialsBuilder;
import com.spotify.heroic.statistics.AnalyticsReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.inject.Named;

@Module
public class BigtableAnalyticsModule implements AnalyticsModule {
    private static final String DEFAULT_INSTANCE = "heroic";
    private static final CredentialsBuilder DEFAULT_CREDENTIALS =
        new ComputeEngineCredentialsBuilder();
    private static final String HITS_TABLE = "hits";
    private static final String HITS_COLUMN_FAMILY = "hits";
    private static final int DEFAULT_MAX_PENDING_REPORTS = 1000;
    private static final boolean DEFAULT_DISABLE_BULK_MUTATIONS = false;
    private static final int DEFAULT_FLUSH_INTERVAL_SECONDS = 2;

    private final String project;
    private final String instance;
    private final String profile;
    private final CredentialsBuilder credentials;
    private final String emulatorEndpoint;
    private final int maxPendingReports;

    public BigtableAnalyticsModule(
        final String project,
        final String instance,
        final String profile,
        final CredentialsBuilder credentials,
        @Nullable final String emulatorEndpoint,
        final int maxPendingReports
    ) {
        this.project = project;
        this.instance = instance;
        this.profile = profile;
        this.credentials = credentials;
        this.emulatorEndpoint = emulatorEndpoint;
        this.maxPendingReports = maxPendingReports;
    }

    @Override
    public AnalyticsComponent module(final PrimaryComponent primary) {
        return DaggerBigtableAnalyticsComponent
            .builder()
            .primaryComponent(primary)
            .bigtableAnalyticsModule(this)
            .build();
    }

    @Provides
    @BigtableScope
    public AnalyticsReporter reporter(final HeroicReporter reporter) {
        return reporter.newAnalyticsReporter();
    }

    @Provides
    @BigtableScope
    public Managed<BigtableConnection> connection(final AsyncFramework async) {
        return async.managed(new ManagedSetup<>() {
            @Override
            public AsyncFuture<BigtableConnection> construct() {
                return async.call(
                    new BigtableConnectionBuilder(project, instance, profile, credentials,
                        emulatorEndpoint, async, DEFAULT_DISABLE_BULK_MUTATIONS,
                        DEFAULT_FLUSH_INTERVAL_SECONDS, Optional.empty()));
            }

            @Override
            public AsyncFuture<Void> destruct(final BigtableConnection value) {
                return async.call(() -> {
                    value.close();
                    return null;
                });
            }
        });
    }

    @Provides
    @BigtableScope
    @Named("hitsTableName")
    String hitsTableName() {
        return HITS_TABLE;
    }

    @Provides
    @BigtableScope
    @Named("hitsColumnFamily")
    String hitsColumnFamily() {
        return HITS_COLUMN_FAMILY;
    }

    @Provides
    @BigtableScope
    @Named("maxPendingReports")
    int maxPendingReports() {
        return maxPendingReports;
    }

    @Provides
    @BigtableScope
    @Named("analytics")
    LifeCycle analyticsLife(LifeCycleManager manager, BigtableMetricAnalytics backend) {
        return manager.build(backend);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String toString() {
      return "BigtableAnalyticsModule(project=" + this.project + ", cluster=" + this.instance
          + ", profile=" + this.profile + ", credentials=" + this.credentials
          + ", maxPendingReports=" + this.maxPendingReports + ")";
    }

  public static class Builder implements AnalyticsModule.Builder {
        private Optional<String> project = Optional.empty();
        private Optional<String> instance = Optional.empty();
        private Optional<String> profile = Optional.empty();
        private Optional<CredentialsBuilder> credentials = Optional.empty();
        private Optional<String> emulatorEndpoint = Optional.empty();
        private Optional<Integer> maxPendingReports = Optional.empty();

        @JsonCreator
        public Builder(
            @JsonProperty("project") Optional<String> project,
            @JsonProperty("instance") Optional<String> instance,
            @JsonProperty("profile") Optional<String> profile,
            @JsonProperty("credentials") Optional<CredentialsBuilder> credentials,
            @JsonProperty("emulatorEndpoint") Optional<String> emulatorEndpoint,
            @JsonProperty("maxPendingReports") Optional<Integer> maxPendingReports
        ) {
            this.project = project;
            this.instance = instance;
            this.profile = profile;
            this.credentials = credentials;
            this.emulatorEndpoint = emulatorEndpoint;
            this.maxPendingReports = maxPendingReports;
        }

        public Builder() {
        }

        public Builder project(String project) {
            this.project = Optional.of(project);
            return this;
        }

        public Builder instance(String instance) {
            this.instance = Optional.of(instance);
            return this;
        }

        public Builder profile(String profile) {
            this.profile = Optional.of(profile);
            return this;
        }

        public Builder credentials(CredentialsBuilder credentials) {
            this.credentials = Optional.of(credentials);
            return this;
        }

        public Builder emulatorEndpoint(String emulatorEndpoint) {
            this.emulatorEndpoint = Optional.of(emulatorEndpoint);
            return this;
        }

        public Builder maxPendingReports(int maxPendingReports) {
            this.maxPendingReports = Optional.of(maxPendingReports);
            return this;
        }

        public BigtableAnalyticsModule build() {
            final String project = this.project.orElseThrow(
                () -> new IllegalStateException("'project' configuration is required"));

            return new BigtableAnalyticsModule(
                project,
                instance.orElse(DEFAULT_INSTANCE),
                profile.orElse(null),
                credentials.orElse(DEFAULT_CREDENTIALS),
                emulatorEndpoint.orElse(null),
                maxPendingReports.orElse(DEFAULT_MAX_PENDING_REPORTS)
            );
        }
    }
}
