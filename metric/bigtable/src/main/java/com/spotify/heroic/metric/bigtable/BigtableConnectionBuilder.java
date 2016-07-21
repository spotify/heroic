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
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClient;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClientImpl;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;
import com.spotify.heroic.metric.bigtable.api.BigtableTableTableAdminClientImpl;
import eu.toolchain.async.AsyncFramework;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

@ToString(of = {"project", "zone", "cluster", "credentials"})
@RequiredArgsConstructor
public class BigtableConnectionBuilder implements Callable<BigtableConnection> {
    private static final String USER_AGENT = "heroic";

    private final String project;
    private final String zone;
    private final String cluster;

    private final CredentialsBuilder credentials;

    private final AsyncFramework async;
    private final ExecutorService executorService;

    @Override
    public BigtableConnection call() throws Exception {
        final CredentialOptions credentials = this.credentials.build();

        final BigtableOptions options = new BigtableOptions.Builder()
            .setProjectId(project)
            .setZoneId(zone)
            .setClusterId(cluster)
            .setUserAgent(USER_AGENT)
            .setDataChannelCount(64)
            .setCredentialOptions(credentials)
            .build();

        final BigtableSession session = new BigtableSession(options, executorService);

        final BigtableTableAdminClient adminClient =
            new BigtableTableTableAdminClientImpl(async, session.getTableAdminClient(), project,
                zone, cluster);

        final BigtableDataClient client =
            new BigtableDataClientImpl(async, session.getDataClient(), project, zone, cluster);

        return new GrpcBigtableConnection(project, zone, cluster, session, adminClient, client);
    }

    @RequiredArgsConstructor
    @ToString(of = {"project", "zone", "cluster"})
    public static class GrpcBigtableConnection implements BigtableConnection {
        private final String project;
        private final String zone;
        private final String cluster;

        final BigtableSession session;
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
        public void close() throws Exception {
            session.close();
        }
    }
}
