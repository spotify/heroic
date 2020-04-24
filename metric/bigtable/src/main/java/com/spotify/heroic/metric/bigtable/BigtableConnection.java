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

import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClient;
import com.spotify.heroic.metric.bigtable.api.BigtableMutator;
import com.spotify.heroic.metric.bigtable.api.BigtableTableAdminClient;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

public class BigtableConnection {
    private final AsyncFramework async;
    private final String project;
    private final String instance;

    private final BigtableSession session;
    private final BigtableMutator mutator;
    private final BigtableTableAdminClient tableAdminClient;
    private final BigtableDataClient dataClient;

    BigtableConnection(
        final AsyncFramework async,
        final String project,
        final String instance,
        final BigtableSession session,
        final BigtableMutator mutator,
        final BigtableTableAdminClient tableAdminClient,
        final BigtableDataClient dataClient
    ) {
        this.async = async;
        this.project = project;
        this.instance = instance;
        this.session = session;
        this.mutator = mutator;
        this.tableAdminClient = tableAdminClient;
        this.dataClient = dataClient;
    }

    public BigtableTableAdminClient getTableAdminClient() {
        return tableAdminClient;
    }

    public BigtableDataClient getDataClient() {
        return dataClient;
    }

    public AsyncFuture<Void> close() {
        final AsyncFuture<Void> closeSession = async.call(() -> {
            session.close();
            return null;
        });

        return async.collectAndDiscard(ImmutableList.of(mutator.close(), closeSession));
    }

    public String toString() {
        return "BigtableConnectionBuilder.GrpcBigtableConnection(project=" + this.project
               + ", instance=" + this.instance + ")";
    }
}
