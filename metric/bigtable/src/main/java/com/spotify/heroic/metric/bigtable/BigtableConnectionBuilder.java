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
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClient;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClientImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableMutator;
import com.spotify.heroic.metric.bigtable.api.BigtableMutatorImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;
import com.spotify.heroic.metric.bigtable.api.BigtableTableTableAdminClientImpl;
import eu.toolchain.async.AsyncFramework;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

public class BigtableConnectionBuilder implements Callable<BigtableConnection> {
    private static final String USER_AGENT = "heroic";

    private final String project;
    private final String instance;

    private final CredentialsBuilder credentials;

    private final AsyncFramework async;

    private final boolean disableBulkMutations;
    private final int flushIntervalSeconds;
    private final Optional<Integer> batchSize;
    private final String emulatorEndpoint;

    public BigtableConnectionBuilder(
        final String project,
        final String instance,
        final CredentialsBuilder credentials,
        @Nullable final String emulatorEndpoint,
        final AsyncFramework async,
        final boolean disableBulkMutations,
        final int flushIntervalSeconds,
        final Optional<Integer> batchSize
    ) {
        this.project = project;
        this.instance = instance;
        this.credentials = credentials;
        this.emulatorEndpoint = emulatorEndpoint;
        this.async = async;
        this.disableBulkMutations = disableBulkMutations;
        this.flushIntervalSeconds = flushIntervalSeconds;
        this.batchSize = batchSize;
    }

    @Override
    public BigtableConnection call() throws Exception {
        final CredentialOptions credentials = this.credentials.build();

        final RetryOptions retryOptions = RetryOptions.builder()
            .addStatusToRetryOn(Status.Code.UNKNOWN)
            .addStatusToRetryOn(Status.Code.UNAVAILABLE)
            .setAllowRetriesWithoutTimestamp(true)
            .build();

        final BulkOptions bulkOptions = batchSize
            .map(integer ->  BulkOptions.builder().setBulkMaxRowKeyCount(integer).build())
            .orElseGet(() -> BulkOptions.builder().build());

        BigtableOptions.Builder builder = BigtableOptions.builder()
            .setProjectId(project)
            .setInstanceId(instance)
            .setUserAgent(USER_AGENT)
            .setDataChannelCount(64)
            .setCredentialOptions(credentials)
            .setRetryOptions(retryOptions)
            .setBulkOptions(bulkOptions);

        if (emulatorEndpoint != null) {
            builder.enableEmulator(emulatorEndpoint);
        }

        final BigtableSession session = new BigtableSession(builder.build());

        final BigtableTableAdminClient adminClient =
            new BigtableTableTableAdminClientImpl(session.getTableAdminClient(), project, instance);

        final BigtableMutator mutator =
            new BigtableMutatorImpl(async, session, disableBulkMutations, flushIntervalSeconds);

        final BigtableDataClient client =
            new BigtableDataClientImpl(async, session, mutator, project, instance);

        return new BigtableConnection(async, project, instance, session, mutator, adminClient,
            client);
    }

    public String toString() {
        return "BigtableConnectionBuilder(project=" + this.project + ", instance=" + this.instance
               + ", credentials=" + this.credentials + ")";
    }
}
