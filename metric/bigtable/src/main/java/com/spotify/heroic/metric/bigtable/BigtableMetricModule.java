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

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.commons.lang3.builder.ToStringStyle.MULTI_LINE_STYLE;

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
import com.spotify.heroic.metric.bigtable.credentials.DefaultCredentialsBuilder;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import java.util.Optional;
import javax.inject.Named;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ModuleId("bigtable")
public final class BigtableMetricModule implements MetricModule, DynamicModuleId {

    private static final Logger log = LoggerFactory.getLogger(BigtableMetricModule.class);
    private static final String BIGTABLE_CONFIGURE_PARAM = "bigtable.configure";

    /* default number of Cells for each batch mutation */
    public static final int DEFAULT_MUTATION_BATCH_SIZE = 10_000;

    /* maximum possible number of Cells for each batch mutation */
    public static final int MAX_MUTATION_BATCH_SIZE = 1_000_000;

    /* minimum possible number of Cells supported for each batch mutation */
    public static final int MIN_MUTATION_BATCH_SIZE = 10;

    private static final String DEFAULT_GROUP = "bigtable";
    private static final String DEFAULT_INSTANCE = "heroic";
    private static final String DEFAULT_TABLE = "metrics";
    private static final CredentialsBuilder DEFAULT_CREDENTIALS = new DefaultCredentialsBuilder();
    private static final boolean DEFAULT_CONFIGURE = false;
    private static final boolean DEFAULT_DISABLE_BULK_MUTATIONS = false;
    private static final int DEFAULT_FLUSH_INTERVAL_SECONDS = 2;

    private final Optional<String> id;
    private final Groups groups;
    private final String project;
    private final String instance;
    private final String profile;
    private final String table;
    private final CredentialsBuilder credentials;
    private final boolean configure;
    private final boolean disableBulkMutations;

    /** This sets the max number of Rows in each batch that's written to the
     * BigTable Client. <p>Note that that does not mean that the client will send
     * that many in one request. That is seemingly controlled by:
     * @see BigtableMetricModule#batchSize
     */
    private final int maxWriteBatchSize;
    private final int flushIntervalSeconds;

    /**
     * batchSize is thought to set com.google.cloud.bigtable.config.BulkOptions
     * .BIGTABLE_BULK_MAX_ROW_KEY_COUNT_DEFAULT .
     * <p>
     * For controlling how many rows we send each time to the Bigtable client,
     * @see BigtableMetricModule#maxWriteBatchSize
     */
    @SuppressWarnings("LineLength")
    private final Optional<Integer> batchSize;
    private final String emulatorEndpoint;

    @JsonCreator
    public BigtableMetricModule(
        @JsonProperty("id") Optional<String> id,
        @JsonProperty("groups") Optional<Groups> groups,
        @JsonProperty("project") Optional<String> project,
        @JsonProperty("instance") Optional<String> instance,
        @JsonProperty("profile") Optional<String> profile,
        @JsonProperty("table") Optional<String> table,
        @JsonProperty("credentials") Optional<CredentialsBuilder> credentials,
        @JsonProperty("configure") Optional<Boolean> configure,
        @JsonProperty("disableBulkMutations") Optional<Boolean> disableBulkMutations,
        @JsonProperty("maxWriteBatchSize") Optional<Integer> maxWriteBatchSize,
        @JsonProperty("flushIntervalSeconds") Optional<Integer> flushIntervalSeconds,
        @JsonProperty("batchSize") Optional<Integer> batchSize,
        @JsonProperty("emulatorEndpoint") Optional<String> emulatorEndpoint
    ) {
        this.id = id;
        this.groups = groups.orElseGet(Groups::empty).or(DEFAULT_GROUP);
        this.project = project.orElseThrow(() -> new NullPointerException("project"));
        this.instance = instance.orElse(DEFAULT_INSTANCE);
        this.profile = profile.orElse(null);
        this.table = table.orElse(DEFAULT_TABLE);
        this.credentials = credentials.orElse(DEFAULT_CREDENTIALS);
        this.configure = configure.orElse(DEFAULT_CONFIGURE);
        this.disableBulkMutations = disableBulkMutations.orElse(DEFAULT_DISABLE_BULK_MUTATIONS);

        // Basically make sure that maxWriteBatchSize, if set, is sane
        int maxWriteBatch = maxWriteBatchSize.orElse(DEFAULT_MUTATION_BATCH_SIZE);
        maxWriteBatch = Math.max(MIN_MUTATION_BATCH_SIZE, maxWriteBatch);
        maxWriteBatch = Math.min(MAX_MUTATION_BATCH_SIZE, maxWriteBatch);

        this.maxWriteBatchSize = maxWriteBatch;
        this.flushIntervalSeconds = flushIntervalSeconds.orElse(DEFAULT_FLUSH_INTERVAL_SECONDS);
        this.batchSize = batchSize;
        this.emulatorEndpoint = emulatorEndpoint.orElse(null);

        log.info("BigTable Metric Module: \n{}", toString());
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

    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
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
        public Managed<BigtableConnection> connection(final AsyncFramework async) {
            return async.managed(new ManagedSetup<>() {
                @Override
                public AsyncFuture<BigtableConnection> construct() {
                    return async.call(
                        new BigtableConnectionBuilder(
                            project, instance, profile, credentials, emulatorEndpoint,
                            async, disableBulkMutations, flushIntervalSeconds, batchSize));
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
        @Named("table")
        public String table() {
            return table;
        }

        @Provides
        @BigtableScope
        @Named("maxWriteBatchSize")
        public Integer maxWriteBatchSize() {
            return maxWriteBatchSize;
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
        public RowKeySerializer rowKeySerializer() {
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
        private Optional<String> profile = empty();
        private Optional<String> table = empty();
        private Optional<CredentialsBuilder> credentials = empty();
        private Optional<Boolean> configure = empty();
        private Optional<Boolean> disableBulkMutations = empty();
        private Optional<Integer> maxWriteBatchSize = empty();
        private Optional<Integer> flushIntervalSeconds = empty();
        private Optional<Integer> batchSize = empty();
        private Optional<String> emulatorEndpoint = empty();

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

        public Builder profile(String profile) {
            this.profile = of(profile);
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

        public Builder maxWriteBatchSize(int maxWriteBatchSize) {
            this.maxWriteBatchSize = of(maxWriteBatchSize);
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = of(batchSize);
            return this;
        }

        public Builder table(final String table) {
            this.table = of(table);
            return this;
        }

        public Builder emulatorEndpoint(final String emulatorEndpoint) {
            this.emulatorEndpoint = of(emulatorEndpoint);
            return this;
        }

        public BigtableMetricModule build() {
            return new BigtableMetricModule(
                id,
                groups,
                project,
                instance,
                profile,
                table,
                credentials,
                configure,
                disableBulkMutations,
                maxWriteBatchSize,
                flushIntervalSeconds,
                batchSize,
                emulatorEndpoint);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, MULTI_LINE_STYLE)
            .append("id", id.orElse("N/A"))
            .append("groups", groups)
            .append("project", project)
            .append("instance", instance)
            .append("profile", profile)
            .append("table", table)
            .append("credentials", credentials)
            .append("configure", configure)
            .append("disableBulkMutations", disableBulkMutations)
            .append("maxWriteBatchSize", maxWriteBatchSize)
            .append("flushIntervalSeconds", flushIntervalSeconds)
            .append("batchSize", batchSize)
            .append("emulatorEndpoint", emulatorEndpoint)
            .toString();
    }
}
