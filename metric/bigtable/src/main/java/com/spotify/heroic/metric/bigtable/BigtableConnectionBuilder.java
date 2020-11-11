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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
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
        final Optional<Integer> batchSize
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

        if (profile != null) {
            builder.setAppProfileId(profile);
        }

        if (emulatorEndpoint != null) {
            builder.enableEmulator(emulatorEndpoint);
        }

        final BigtableOptions bigtableOptions = builder.build();

        log.info("Retry Options: {}", retryOptions.toString());
        log.info("Bulk Options: {}", bulkOptions.toString());
        log.info("Bigtable Options: {}", bigtableOptions.toString());
        log.info("BigTable Connection Builder: \n{}", toString());

        final BigtableSession session = new BigtableSession(bigtableOptions);

        final BigtableTableAdminClient adminClient =
            new BigtableTableTableAdminClientImpl(session.getTableAdminClient(), project, instance);

        final BigtableMutator mutator =
            new BigtableMutatorImpl(async, session, disableBulkMutations, flushIntervalSeconds);

        final BigtableDataClient client =
            new BigtableDataClientImpl(async, session, mutator, project, instance);

        CredentialsProvider credentialsProvider = GoogleCredentialsProvider.newBuilder().build();
        BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
            .setInstanceId(instance)
            .setProjectId(project)
            .setCredentialsProvider(credentialsProvider)
            .setRefreshingChannel(true)
            .build();

        var clientV2 = com.google.cloud.bigtable.data.v2.BigtableDataClient.create(dataSettings);

        return new BigtableConnection(async, project, instance, session, mutator, adminClient,
            clientV2);
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
            .toString();
    }
}
