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
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClient;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClientImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableMutator;
import com.spotify.heroic.metric.bigtable.api.BigtableMutatorImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;
import com.spotify.heroic.metric.bigtable.api.BigtableTableTableAdminClientImpl;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.Callable;
import lombok.ToString;

@ToString(of = {"project", "instance", "credentials"})
public class BigtableConnectionBuilder implements Callable<BigtableConnection> {
    private static final String USER_AGENT = "heroic";

    private final String project;
    private final String instance;

    private final CredentialsBuilder credentials;

    private final AsyncFramework async;

    private final boolean disableBulkMutations;
    private final int flushIntervalSeconds;
    private final Optional<Integer> batchSize;

    @java.beans.ConstructorProperties({ "project", "instance", "credentials", "async",
                                        "disableBulkMutations", "flushIntervalSeconds",
                                        "batchSize" })
    public BigtableConnectionBuilder(final String project, final String instance,
                                     final CredentialsBuilder credentials,
                                     final AsyncFramework async, final boolean disableBulkMutations,
                                     final int flushIntervalSeconds,
                                     final Optional<Integer> batchSize) {
        this.project = project;
        this.instance = instance;
        this.credentials = credentials;
        this.async = async;
        this.disableBulkMutations = disableBulkMutations;
        this.flushIntervalSeconds = flushIntervalSeconds;
        this.batchSize = batchSize;
    }

    @Override
    public BigtableConnection call() throws Exception {
        final CredentialOptions credentials = this.credentials.build();

        final RetryOptions retryOptions = new RetryOptions.Builder()
            .addStatusToRetryOn(Status.Code.UNKNOWN)
            .addStatusToRetryOn(Status.Code.UNAVAILABLE)
            .setAllowRetriesWithoutTimestamp(true)
            .build();

        final BulkOptions bulkOptions = batchSize
            .map(integer -> new BulkOptions.Builder().setBulkMaxRowKeyCount(integer).build())
            .orElseGet(() -> new BulkOptions.Builder().build());

        final BigtableOptions options = new BigtableOptions.Builder()
            .setProjectId(project)
            .setInstanceId(instance)
            .setUserAgent(USER_AGENT)
            .setDataChannelCount(64)
            .setCredentialOptions(credentials)
            .setRetryOptions(retryOptions)
            .setBulkOptions(bulkOptions)
            .build();

        final BigtableSession session = new BigtableSession(options);

        final BigtableTableAdminClient adminClient =
            new BigtableTableTableAdminClientImpl(async, session.getTableAdminClient(), project,
                instance);

        final BigtableMutator mutator =
            new BigtableMutatorImpl(async, session, disableBulkMutations, flushIntervalSeconds);

        final BigtableDataClient client =
            new BigtableDataClientImpl(async, session, mutator, project, instance);

        return new GrpcBigtableConnection(async, project, instance, session, mutator, adminClient,
            client);
    }

    @ToString(of = {"project", "instance"})
    public static class GrpcBigtableConnection implements BigtableConnection {
        private final AsyncFramework async;
        private final String project;
        private final String instance;

        final BigtableSession session;
        final BigtableMutator mutator;
        final BigtableTableAdminClient tableAdminClient;
        final BigtableDataClient dataClient;

        @java.beans.ConstructorProperties({ "async", "project", "instance", "session", "mutator",
                                            "tableAdminClient", "dataClient" })
        public GrpcBigtableConnection(final AsyncFramework async, final String project,
                                      final String instance, final BigtableSession session,
                                      final BigtableMutator mutator,
                                      final BigtableTableAdminClient tableAdminClient,
                                      final BigtableDataClient dataClient) {
            this.async = async;
            this.project = project;
            this.instance = instance;
            this.session = session;
            this.mutator = mutator;
            this.tableAdminClient = tableAdminClient;
            this.dataClient = dataClient;
        }

        @Override
        public BigtableTableAdminClient tableAdminClient() {
            return tableAdminClient;
        }

        @Override
        public BigtableDataClient dataClient() {
            return dataClient;
        }

        @Override
        public AsyncFuture<Void> close() {
            final AsyncFuture<Void> closeSession = async.call(() -> {
                session.close();
                return null;
            });

            return async.collectAndDiscard(ImmutableList.of(mutator.close(), closeSession));
        }
    }
}
