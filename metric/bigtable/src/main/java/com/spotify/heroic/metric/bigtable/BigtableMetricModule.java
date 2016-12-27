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

package com.spotify.heroic.metric.bigtable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.common.DynamicModuleId;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.ModuleId;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.bigtable.credentials.ComputeEngineCredentialsBuilder;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.serializer.Serializer;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.inject.Named;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import lombok.Data;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@Data
@ModuleId("bigtable")
public final class BigtableMetricModule implements MetricModule, DynamicModuleId {
    public static final String BIGTABLE_CONFIGURE_PARAM = "bigtable.configure";

    public static final String DEFAULT_GROUP = "bigtable";
    public static final String DEFAULT_INSTANCE = "heroic";
    public static final String DEFAULT_TABLE = "metrics";
    public static final CredentialsBuilder DEFAULT_CREDENTIALS =
        new ComputeEngineCredentialsBuilder();
    public static final boolean DEFAULT_CONFIGURE = false;
    public static final boolean DEFAULT_DISABLE_BULK_MUTATIONS = false;
    public static final int DEFAULT_FLUSH_INTERVAL_SECONDS = 2;

    private final Optional<String> id;
    private final Groups groups;
    private final String project;
    private final String instance;
    private final String table;
    private final CredentialsBuilder credentials;
    private final boolean configure;
    private final boolean disableBulkMutations;
    private final int flushIntervalSeconds;
    private final Optional<Integer> batchSize;
    private final Optional<Integer> defaultFetchSize;

    @JsonCreator
    public BigtableMetricModule(
        @JsonProperty("id") Optional<String> id, @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("project") Optional<String> project,
        @JsonProperty("instance") Optional<String> instance,
        @JsonProperty("table") Optional<String> table,
        @JsonProperty("credentials") Optional<CredentialsBuilder> credentials,
        @JsonProperty("configure") Optional<Boolean> configure,
        @JsonProperty("disableBulkMutations") Optional<Boolean> disableBulkMutations,
        @JsonProperty("flushIntervalSeconds") Optional<Integer> flushIntervalSeconds,
        @JsonProperty("batchSize") Optional<Integer> batchSize,
        @JsonProperty("defaultFetchSize") Optional<Integer> defaultFetchSize
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.project = project.orElseThrow(() -> new NullPointerException("project"));
        this.instance = instance.orElse(DEFAULT_INSTANCE);
        this.table = table.orElse(DEFAULT_TABLE);
        this.credentials = credentials.orElse(DEFAULT_CREDENTIALS);
        this.configure = configure.orElse(DEFAULT_CONFIGURE);
        this.disableBulkMutations = disableBulkMutations.orElse(DEFAULT_DISABLE_BULK_MUTATIONS);
        this.flushIntervalSeconds = flushIntervalSeconds.orElse(DEFAULT_FLUSH_INTERVAL_SECONDS);
        this.batchSize = batchSize;
        this.defaultFetchSize = defaultFetchSize;
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends depends, String id) {
        return DaggerBigtableMetricModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M())
            .build();
    }

    @BigtableScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class, Depends.class})
    interface C extends Exposed {
        @Override
        BigtableBackend backend();

        @Override
        LifeCycle life();
    }

    @Module
    class M {
        @Provides
        @BigtableScope
        public Managed<BigtableConnection> connection(
            final AsyncFramework async, final ExecutorService executorService
        ) {
            return async.managed(new ManagedSetup<BigtableConnection>() {
                @Override
                public AsyncFuture<BigtableConnection> construct() throws Exception {
                    return async.call(
                        new BigtableConnectionBuilder(project, instance, credentials, async,
                            executorService, disableBulkMutations, flushIntervalSeconds, batchSize,
                            defaultFetchSize));
                }

                @Override
                public AsyncFuture<Void> destruct(final BigtableConnection value) throws Exception {
                    return async.call(() -> {
                        value.close();
                        return null;
                    });
                }
            });
        }

        @Provides
        @BigtableScope
        @Named("table")
        public String table() {
            return table;
        }

        @Provides
        @BigtableScope
        @Named("configure")
        public boolean configure(final ExtraParameters params) {
            return params.contains(ExtraParameters.CONFIGURE) ||
                params.contains(BIGTABLE_CONFIGURE_PARAM) || configure;
        }

        @Provides
        @BigtableScope
        public Serializer<RowKey> rowKeySerializer() {
           return new MetricsRowKeySerializer();
        }

        @Provides
        @BigtableScope
        public Groups groups() {
            return groups;
        }

        @Provides
        @BigtableScope
        public LifeCycle life(LifeCycleManager manager, BigtableBackend backend) {
            return manager.build(backend);
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
        private Optional<String> project = empty();
        private Optional<String> instance = empty();
        private Optional<String> table = empty();
        private Optional<CredentialsBuilder> credentials = empty();
        private Optional<Boolean> configure = empty();
        private Optional<Boolean> disableBulkMutations = empty();
        private Optional<Integer> flushIntervalSeconds = empty();
        private Optional<Integer> batchSize = empty();
        private Optional<Integer> defaultFetchSize = empty();

        public Builder id(String id) {
            this.id = of(id);
            return this;
        }

        public Builder groups(Groups groups) {
            this.groups = of(groups);
            return this;
        }

        public Builder project(String project) {
            this.project = of(project);
            return this;
        }

        public Builder instance(String instance) {
            this.instance = of(instance);
            return this;
        }

        public Builder credentials(CredentialsBuilder credentials) {
            this.credentials = of(credentials);
            return this;
        }

        public Builder configure(boolean configure) {
            this.configure = of(configure);
            return this;
        }

        public Builder disableBulkMutations(boolean disableBulkMutations) {
            this.disableBulkMutations = of(disableBulkMutations);
            return this;
        }


        public Builder flushIntervalSeconds(int flushIntervalSeconds) {
            this.flushIntervalSeconds = of(flushIntervalSeconds);
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = of(batchSize);
            return this;
        }

        public Builder defaultFetchSize(int defaultFetchSize) {
            this.defaultFetchSize = of(defaultFetchSize);
            return this;
        }

        public Builder table(final String table) {
            this.table = of(table);
            return this;
        }

        public BigtableMetricModule build() {
            return new BigtableMetricModule(id, groups, project, instance, table, credentials,
                configure, disableBulkMutations, flushIntervalSeconds, batchSize, defaultFetchSize);
        }
    }
}
