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

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClientImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableMutatorImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableTableTableAdminClientImpl;
import eu.toolchain.async.AsyncFramework;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableConnectionBuilder implements Callable<BigtableConnection> {
    private static final Logger log = LoggerFactory.getLogger(BigtableConnectionBuilder.class);

    private static final String USER_AGENT = "heroic";

    private final String project;
    private final String instance;
    private final String profile;

    private final CredentialsBuilder credentials;

    private final AsyncFramework async;

    private final boolean disableBulkMutations;
    private final int flushIntervalSeconds;
    private final Optional<Integer> batchSize;
    private final int mutateRpcTimeoutMs;
    private final int readRowsRpcTimeoutMs;
    private final int shortRpcTimeoutMs;
    private final int maxScanTimeoutRetries;
    private final int maxElapsedBackoffMs;

    private final String emulatorEndpoint;

    public BigtableConnectionBuilder(
        final String project,
        final String instance,
        final String profile,
        final CredentialsBuilder credentials,
        @Nullable final String emulatorEndpoint,
        final AsyncFramework async,
        final boolean disableBulkMutations,
        final int flushIntervalSeconds,
        final Optional<Integer> batchSize,
        final int mutateRpcTimeoutMs,
        final int readRowsRpcTimeoutMs,
        final int maxScanTimeoutRetries,
        final int shortRpcTimeoutMs,
        final int maxElapsedBackoffMs
    ) {
        this.project = project;
        this.instance = instance;
        this.profile = profile;
        this.credentials = credentials;
        this.emulatorEndpoint = emulatorEndpoint;
        this.async = async;
        this.disableBulkMutations = disableBulkMutations;
        this.flushIntervalSeconds = flushIntervalSeconds;
        this.batchSize = batchSize;
        this.mutateRpcTimeoutMs = mutateRpcTimeoutMs;
        this.readRowsRpcTimeoutMs = readRowsRpcTimeoutMs;
        this.shortRpcTimeoutMs = shortRpcTimeoutMs;
        this.maxScanTimeoutRetries = maxScanTimeoutRetries;
        this.maxElapsedBackoffMs = maxElapsedBackoffMs;
    }

    @Override
    public BigtableConnection call() throws Exception {
        final var credentials = this.credentials.build();

        final var retryOptions = RetryOptions.builder()
            .addStatusToRetryOn(Status.Code.UNKNOWN)
            .addStatusToRetryOn(Status.Code.UNAVAILABLE)
            .setAllowRetriesWithoutTimestamp(true)
            .setEnableRetries(true)
            .setMaxScanTimeoutRetries(maxScanTimeoutRetries)
            .setMaxElapsedBackoffMillis(maxElapsedBackoffMs)
            .build();

        final var bulkOptions = batchSize
            .map(integer ->  BulkOptions.builder().setBulkMaxRowKeyCount(integer).build())
            .orElseGet(() -> BulkOptions.builder().build());

        var callOptionsConfig = CallOptionsConfig.builder()
                .setReadRowsRpcTimeoutMs(readRowsRpcTimeoutMs)
                .setMutateRpcTimeoutMs(mutateRpcTimeoutMs)
                .setShortRpcTimeoutMs(shortRpcTimeoutMs)
                .setUseTimeout(true).build();

        var builder = BigtableOptions.builder()
            .setProjectId(project)
            .setInstanceId(instance)
            .setUserAgent(USER_AGENT)
            .setDataChannelCount(64)
            .setCredentialOptions(credentials)
            .setRetryOptions(retryOptions)
            .setBulkOptions(bulkOptions)
            .setCallOptionsConfig(callOptionsConfig);

        if (profile != null) {
            builder.setAppProfileId(profile);
        }

        if (emulatorEndpoint != null) {
            builder.enableEmulator(emulatorEndpoint);
        }

        final var bigtableOptions = builder.build();

        log.info("Retry Options: {}", retryOptions.toString());
        log.info("Bulk Options: {}", bulkOptions.toString());
        log.info("Bigtable Options: {}", bigtableOptions.toString());
        log.info("Call Options: {}", callOptionsConfig.toString());
        log.info("BigTable Connection Builder: \n{}", toString());

        final var session = new BigtableSession(bigtableOptions);

        final var adminClient =
            new BigtableTableTableAdminClientImpl(session.getTableAdminClient(), project, instance);

        final var mutator =
            new BigtableMutatorImpl(async, session, disableBulkMutations, flushIntervalSeconds);

        final var client =
            new BigtableDataClientImpl(async, session, mutator, project, instance);

        return new BigtableConnection(async, project, instance, session, mutator, adminClient,
            client);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
            .append("project", project)
            .append("instance", instance)
            .append("profile", profile)
            .append("credentials", credentials)
            .append("disableBulkMutations", disableBulkMutations)
            .append("flushIntervalSeconds", flushIntervalSeconds)
            .append("batchSize", batchSize.orElse(-1))
            .append("emulatorEndpoint", emulatorEndpoint)
            .append("mutateRpcTimeoutMs", mutateRpcTimeoutMs)
            .append("readRowsRpcTimeoutMs", readRowsRpcTimeoutMs)
            .append("shortRpcTimeoutMs", shortRpcTimeoutMs)
            .append("maxScanTimeoutRetries", maxScanTimeoutRetries)
            .append("maxElapsedBackoffMs", maxElapsedBackoffMs)
            .toString();
    }
}
