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

package com.spotify.heroic.metric.datastax;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ManagedSetup;

@RequiredArgsConstructor
@ToString(of = { "seeds", "keyspace" })
public class ManagedSetupConnection implements ManagedSetup<Connection> {
    private static final String WRITE_METRICS_CQL = "INSERT INTO metrics (metric_key, data_timestamp_offset, data_value) VALUES (?, ?, ?)";

    private static final String FETCH_METRICS_CQL = ("SELECT data_timestamp_offset, data_value FROM metrics "
            + "WHERE metric_key = ? and data_timestamp_offset >= ? and data_timestamp_offset <= ? LIMIT ?");

    private static final String KEYS_UNBOUND_CQL = "SELECT DISTINCT metric_key FROM metrics";
    private static final String KEYS_LEFTBOUND_CQL = "SELECT DISTINCT metric_key FROM metrics WHERE token(metric_key) > token(?)";
    private static final String KEYS_RIGHTBOUND_CQL = "SELECT DISTINCT metric_key FROM metrics WHERE token(metric_key) <= token(?)";
    private static final String KEYS_BOUND_CQL = "SELECT DISTINCT metric_key FROM metrics WHERE token(metric_key) > token(?) AND token(metric_key) <= token(?)";

    private final AsyncFramework async;
    private final Collection<InetSocketAddress> seeds;
    private final String keyspace;

    public AsyncFuture<Connection> construct() {
        return async.call(new Callable<Connection>() {
            public Connection call() throws Exception {
                // @formatter:off
                    final HostDistance distance = HostDistance.LOCAL;
                    final PoolingOptions pooling = new PoolingOptions()
                        .setMaxConnectionsPerHost(distance, 20)
                        .setCoreConnectionsPerHost(distance, 4)
                        .setMaxSimultaneousRequestsPerHostThreshold(distance, Short.MAX_VALUE)
                        .setMaxSimultaneousRequestsPerConnectionThreshold(distance, 128);

                    final Cluster cluster = Cluster.builder()
                        .addContactPointsWithPorts(seeds)
                        .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                        .withPoolingOptions(pooling).build();
                    // @formatter:on

                final Session session = cluster.connect(keyspace);

                final PreparedStatement write = session.prepare(WRITE_METRICS_CQL);
                final PreparedStatement fetch = session.prepare(FETCH_METRICS_CQL);
                final PreparedStatement keysUnbound = session.prepare(KEYS_UNBOUND_CQL);
                final PreparedStatement keysLeftbound = session.prepare(KEYS_LEFTBOUND_CQL);
                final PreparedStatement keysRightbound = session.prepare(KEYS_RIGHTBOUND_CQL);
                final PreparedStatement keysBound = session.prepare(KEYS_BOUND_CQL);

                return new Connection(cluster, session, write, fetch, keysUnbound, keysLeftbound, keysRightbound,
                        keysBound);
            };
        });
    }

    @Override
    public AsyncFuture<Void> destruct(final Connection c) {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        futures.add(async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                c.session.close();
                return null;
            }
        }));

        futures.add(async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                c.cluster.close();
                return null;
            }
        }));

        return async.collectAndDiscard(futures);
    }
}