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

import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;
import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES;
import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_MUTATE_RPC_TIMEOUT_MS;
import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_READ_ROWS_RPC_TIMEOUT_MS;
import static com.spotify.heroic.metric.consts.ApiQueryConsts.DEFAULT_SHORT_RPC_TIMEOUT_MS;
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
import com.spotify.heroic.metric.consts.ApiQueryConsts;
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

@SuppressWarnings({"LineLength"})
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
    /**
     * The amount of milliseconds to wait before issuing a client side timeout
     * for mutation remote procedure calls.
     * @see ApiQueryConsts#DEFAULT_MUTATE_RPC_TIMEOUT_MS
     */
    private final int mutateRpcTimeoutMs;
    /**
     * The amount of milliseconds to wait before issuing a client side
     * timeout for readRows streaming remote procedure calls.
     * @see ApiQueryConsts#DEFAULT_READ_ROWS_RPC_TIMEOUT_MS for description
     */
    private final int readRowsRpcTimeoutMs;
    /**
     * The amount of milliseconds to wait before issuing a client side timeout for
     * short remote procedure calls.
     * @see ApiQueryConsts#DEFAULT_SHORT_RPC_TIMEOUT_MS
     */
    private final int shortRpcTimeoutMs;

    /**
     * Maximum number of times to retry after a scan timeout (Google default value: 10 retries).
     * @See ApiQueryConsts#DEFAULT_MAX_SCAN_TIMEOUT_RETRIES
     */
    public final int maxScanTimeoutRetries;

    /**
     * Maximum amount of time to retry before failing the operation (Google default value: 600
     * seconds).
     * @See ApiQueryConsts#DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS
     */
    public final int maxElapsedBackoffMs;

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
        @JsonProperty("mutateRpcTimeoutMs") Optional<Integer> mutateRpcTimeoutMs,
        @JsonProperty("readRowsRpcTimeoutMs") Optional<Integer> readRowsRpcTimeoutMs,
        @JsonProperty("shortRpcTimeoutMs") Optional<Integer> shortRpcTimeoutMs,
        @JsonProperty("maxScanTimeoutRetries") Optional<Integer> maxScanTimeoutRetries,
        @JsonProperty("maxElapsedBackoffMs") Optional<Integer> maxElapsedBackoffMs,
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

        this.mutateRpcTimeoutMs = mutateRpcTimeoutMs.orElse(DEFAULT_MUTATE_RPC_TIMEOUT_MS);
        this.readRowsRpcTimeoutMs = readRowsRpcTimeoutMs.orElse(DEFAULT_READ_ROWS_RPC_TIMEOUT_MS);
        this.shortRpcTimeoutMs = shortRpcTimeoutMs.orElse(DEFAULT_SHORT_RPC_TIMEOUT_MS);
        this.maxScanTimeoutRetries = maxScanTimeoutRetries.orElse(DEFAULT_MAX_SCAN_TIMEOUT_RETRIES);
        this.maxElapsedBackoffMs =
         maxElapsedBackoffMs.orElse(DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS);

        this.flushIntervalSeconds = flushIntervalSeconds.orElse(DEFAULT_FLUSH_INTERVAL_SECONDS);
        this.batchSize = batchSize;
        this.emulatorEndpoint = emulatorEndpoint.orElse(null);

        log.info("BigTable Metric Module: \n{}", toString());
    }

    @Override
    public Exposed module(PrimaryComponent primary, Depends backend, String id) {
        return DaggerBigtableMetricModule_C
            .builder()
            .primaryComponent(primary)
            .depends(backend)
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
                            async, disableBulkMutations, flushIntervalSeconds, batchSize,
                                mutateRpcTimeoutMs, readRowsRpcTimeoutMs, shortRpcTimeoutMs,
                                maxScanTimeoutRetries, maxElapsedBackoffMs
                        ));
                }

                @Override
                public AsyncFuture<Void> destruct(final BigtableConnection t) {
                    return async.call(() -> {
                        t.close();
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
        @Named("mutateRpcTimeoutMs")
        public Integer mutateRpcTimeoutMs() {
            return mutateRpcTimeoutMs;
        }

        @Provides
        @BigtableScope
        @Named("readRowsRpcTimeoutMs")
        public Integer readRowsRpcTimeoutMs() {
            return readRowsRpcTimeoutMs;
        }

        @Provides
        @BigtableScope
        @Named("shortRpcTimeoutMs")
        public Integer shortRpcTimeoutMs() {
            return shortRpcTimeoutMs;
        }

        @Provides
        @BigtableScope
        @Named("maxScanTimeoutRetries")
        public Integer maxScanTimeoutRetries() {
            return maxScanTimeoutRetries;
        }

        @Provides
        @BigtableScope
        @Named("maxElapsedBackoffMs")
        public Integer maxElapsedBackoffMs() {
            return maxElapsedBackoffMs;
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
        private Optional<Integer> mutateRpcTimeoutMs = empty();
        private Optional<Integer> readRowsRpcTimeoutMs = empty();
        private Optional<Integer> shortRpcTimeoutMs = empty();
        private Optional<Integer> maxScanTimeoutRetries = empty();
        private Optional<Integer> maxElapsedBackoffMs = empty();
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

        public Builder mutateRpcTimeoutMs(int mutateRpcTimeoutMs) {
            this.mutateRpcTimeoutMs = of(mutateRpcTimeoutMs);
            return this;
        }

        public Builder readRowsRpcTimeoutMs(int readRowsRpcTimeoutMs) {
            this.readRowsRpcTimeoutMs = of(readRowsRpcTimeoutMs);
            return this;
        }

        public Builder shortRpcTimeoutMs(int shortRpcTimeoutMs) {
            this.shortRpcTimeoutMs = of(shortRpcTimeoutMs);
            return this;
        }

        public Builder maxScanTimeoutRetries(int maxScanTimeoutRetries) {
            this.maxScanTimeoutRetries = of(maxScanTimeoutRetries);
            return this;
        }

        public Builder maxElapsedBackoffMs(int maxElapsedBackoffMs) {
            this.maxElapsedBackoffMs = of(maxElapsedBackoffMs);
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
                mutateRpcTimeoutMs,
                readRowsRpcTimeoutMs,
                shortRpcTimeoutMs,
                maxScanTimeoutRetries,
                maxElapsedBackoffMs,
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
            .append("mutateRpcTimeoutMs", mutateRpcTimeoutMs)
            .append("readRowsRpcTimeoutMs", readRowsRpcTimeoutMs)
            .append("shortRpcTimeoutMs", shortRpcTimeoutMs)
            .append("maxScanTimeoutRetries", maxScanTimeoutRetries)
            .append("flushIntervalSeconds", flushIntervalSeconds)
            .append("maxElapsedBackoffMs", maxElapsedBackoffMs)
            .append("batchSize", batchSize)
            .append("emulatorEndpoint", emulatorEndpoint)
            .toString();
    }
}
