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

import com.google.appengine.repackaged.com.google.common.collect.ImmutableList;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.BigtableGrpc.Bigtable;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.io.ChannelPool;

import com.spotify.heroic.bigtable.grpc.Status;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClient;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClientImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableMutator;
import com.spotify.heroic.metric.bigtable.api.BigtableMutatorImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;
import com.spotify.heroic.metric.bigtable.api.BigtableTableTableAdminClientImpl;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {"project", "instance", "credentials"})
@RequiredArgsConstructor
public class BigtableConnectionBuilder implements Callable<BigtableConnection> {
    private static final String USER_AGENT = "heroic";

    private final String project;
    private final String instance;

    private final CredentialsBuilder credentials;

    private final AsyncFramework async;
    private final ExecutorService executorService;

    private final boolean disableBulkMutations;
    private final int flushIntervalSeconds;
    private final Optional<Integer> batchSize;
    private final Optional<Integer> defaultFetchSize;

    @Override
    public BigtableConnection call() throws Exception {
        final CredentialOptions credentials = this.credentials.build();

        final RetryOptions retryOptions = new RetryOptions.Builder()
            .addStatusToRetryOn(Status.Code.UNKNOWN)
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

        final BigtableSession session = new BigtableSession(options, executorService);

        final BigtableTableAdminClient adminClient =
            new BigtableTableTableAdminClientImpl(async, session.getTableAdminClient(), project,
                instance);

        final BigtableMutator mutator =
            new BigtableMutatorImpl(async, session, disableBulkMutations, flushIntervalSeconds);

        final ChannelPool channelPool =
            BigtableSession.createChannelPool(options.getDataHost(), options,
                options.getChannelCount());

        final Bigtable bigtable = BigtableGrpc.newStub(channelPool);

        final BigtableDataClient client =
            new BigtableDataClientImpl(async, session, mutator, bigtable, project, instance,
                defaultFetchSize);

        return new GrpcBigtableConnection(async, project, instance, session, channelPool, mutator,
            adminClient, client);
    }

    @RequiredArgsConstructor
    @ToString(of = {"project", "instance"})
    public static class GrpcBigtableConnection implements BigtableConnection {
        private final AsyncFramework async;
        private final String project;
        private final String instance;

        final BigtableSession session;
        final ChannelPool channelPool;
        final BigtableMutator mutator;
        final BigtableTableAdminClient tableAdminClient;
        final BigtableDataClient dataClient;

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
            final AsyncFuture<Void> shutdownChannelPool = async.call(() -> {
                channelPool.shutdown();
                channelPool.awaitTermination(10, TimeUnit.SECONDS);
                return null;
            });
            return async.collectAndDiscard(
                ImmutableList.of(mutator.close(), closeSession, shutdownChannelPool));
        }
    }
}
